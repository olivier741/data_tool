/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.vas.data;

/**
 *
 * @author olivier.tatsinkou
 */
import com.viettel.cluster.agent.integration.Record;
import com.viettel.smsfw.process.ProcessRecordAbstract;
import com.viettel.smsfw.utils.MoRecord;
import com.viettel.vas.data.database.DbPost;
import com.viettel.vas.data.database.DbPre;
import com.viettel.vas.data.database.DbProcessor;
import com.viettel.vas.data.obj.DataPacket;
import com.viettel.vas.data.obj.DataSubscriber;
import com.viettel.vas.data.obj.SubscriberData;
import com.viettel.vas.data.utils.Commons;
import com.viettel.vas.util.ConnectionPoolManager;
import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.ResourceBundle;
import java.util.Set;
import org.apache.log4j.Logger;
import utils.Config;

public class OffExtend
  extends ProcessRecordAbstract
{
  private String loggerLabel = Register.class.getSimpleName() + ": ";
  private DbProcessor db;
  private DbPost dbPost;
  private DbPre dbPre;
  private StringBuilder br = new StringBuilder();
  private String countryCode;
  
  public boolean startProcessRecord()
  {
    return true;
  }
  
  public void initBeforeStart()
    throws Exception
  {
    ConnectionPoolManager.loadConfig(Config.configDir + File.separator + "database.xml");
    this.db = new DbProcessor("database", this.logger);
    this.dbPost = new DbPost("dbpos", this.logger);
    this.dbPre = new DbPre("dbpre", this.logger);
    Commons.createInstance("PROCESS", this.db, this.logger);
    this.countryCode = ResourceBundle.getBundle("vas").getString("country_code");
  }
  
  public List<Record> validateContraint(List<Record> list)
    throws Exception
  {
    for (Record record : list)
    {
      String language = Commons.defaultLang;
      MoRecord moRecord = (MoRecord)record;
      HashMap map = new HashMap();
      moRecord.setHashMap(map);
      
      String subInfo = this.dbPost.getSubInfoMobile(moRecord.getMsisdn().substring(this.countryCode.length()));
      if (subInfo == null)
      {
        this.br.setLength(0);
        this.br.append(this.loggerLabel).append("Fail to get subscriber info on CM_POS: MSISDN=").append(moRecord.getMsisdn());
        

        this.logger.error(this.br);
        moRecord.setErrCode("2");
        moRecord.setErrOcs("Fail to get subscriber info on CM_POS");
        moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("system_fail_" + language, this.logger));
      }
      else
      {
        if (subInfo.equals("NO_INFO_SUB"))
        {
          subInfo = this.dbPre.getSubInfoMobile(moRecord.getMsisdn().substring(this.countryCode.length()));
          if (subInfo == null)
          {
            this.br.setLength(0);
            this.br.append(this.loggerLabel).append("Fail to get subscriber info on CM_PRE: MSISDN=").append(moRecord.getMsisdn());
            

            this.logger.error(this.br);
            moRecord.setErrCode("2");
            moRecord.setErrOcs("Fail to get subscriber info on CM_PRE");
            moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("system_fail_" + language, this.logger));
            
            continue;
          }
          if (subInfo.equals("NO_INFO_SUB"))
          {
            this.br.setLength(0);
            this.br.append(this.loggerLabel).append("Subscriber is not mobile number: MSISDN=").append(moRecord.getMsisdn());
            

            this.logger.info(this.br);
            moRecord.setErrCode("6");
            moRecord.setErrOcs("Subscriber is not mobile number");
            moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("not_info_sub_" + language, this.logger));
            
            continue;
          }
          moRecord.setSubType(Integer.valueOf(1));
        }
        else
        {
          moRecord.setSubType(Integer.valueOf(0));
        }
        this.logger.info("SUB_INFO:\n" + subInfo);
        SubscriberData subscriber = new SubscriberData(moRecord.getMsisdn(), subInfo);
        language = subscriber.getLanguage();
        moRecord.getHashMap().put("LANGUAGE", language);
        String currentService = Commons.parseCurrentVas(subscriber.getVasList(), "-");
        moRecord.getHashMap().put("CURRENT_SERVICE", currentService);
        
        moRecord.setSubId(Long.valueOf(subscriber.getSubId()));
        moRecord.setProductCode(subscriber.getProductCode());
        moRecord.setErrCode("0");
      }
    }
    return list;
  }
  
  public List<Record> processListRecord(List<Record> list)
    throws Exception
  {
    List<DataSubscriber> listCancel = new ArrayList();
    for (Record record : list)
    {
      MoRecord moRecord = (MoRecord)record;
      if (moRecord.getErrCode().equals("0"))
      {
        String language = moRecord.getHashMap().get("LANGUAGE").toString();
        List<DataSubscriber> listSub = this.db.getDataSubscriberActive(moRecord.getMsisdn());
        if (listSub == null)
        {
          this.br.setLength(0);
          this.br.append("Error get subscriber on database").append(": MSISDN=").append(moRecord.getMsisdn());
          

          this.logger.error(this.br);
          moRecord.setErrCode("1");
          moRecord.setErrOcs("Error get subscriber on database");
          moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("system_fail_" + language, this.logger));
        }
        else
        {
          String currentService = (String)moRecord.getHashMap().get("CURRENT_SERVICE");
          if ((currentService != null) && (currentService.trim().length() > 0))
          {
            Set<String> listVasCode = Commons.getInstance("PROCESS").getAllVasCodeOnDataPacket();
            List<String> dataOnBccs = Commons.checkDuplicateVasCode(currentService.split("-"), listVasCode, moRecord.getSubType().intValue());
            for (String vas : dataOnBccs)
            {
              boolean exitDatabase = false;
              DataPacket currentPacket = Commons.getInstance("PROCESS").getDataPacketByVasCode(vas, moRecord.getSubType().intValue());
              for (DataSubscriber dataSubscriber : listSub) {
                if (dataSubscriber.getDataName().equals(currentPacket.getName()))
                {
                  exitDatabase = true;
                  break;
                }
              }
              if (!exitDatabase)
              {
                this.logger.info("Add virtual data_subcriber => " + currentPacket.getName());
                
                DataSubscriber dataSub = new DataSubscriber();
                dataSub.setDataName(currentPacket.getName());
                dataSub.setMsisdn(moRecord.getMsisdn());
                dataSub.setSubId(moRecord.getSubId().longValue());
                dataSub.setProductCode(moRecord.getProductCode());
                dataSub.setSubType(moRecord.getSubType().intValue());
                dataSub.setStatus(1);
                listSub.add(dataSub);
              }
            }
          }
          List<DataSubscriber> listSubCheck = new ArrayList();
          listSubCheck.addAll(listSub);
          listSub.clear();
          for (DataSubscriber dataSubscriber : listSubCheck) {
            if ((dataSubscriber.getExpireTime() != null) && (dataSubscriber.getExpireTime().getTime() < System.currentTimeMillis()) && (dataSubscriber.getAutoExtend() == 0))
            {
              this.logger.info("Packet " + dataSubscriber.getDataName() + " is expired");
              this.logger.warn("Please check auto extext thread for this packet: " + dataSubscriber.getMsisdn() + "=>" + dataSubscriber.getDataName());
            }
            else
            {
              listSub.add(dataSubscriber);
            }
          }
          listSubCheck.clear();
          if (listSub.isEmpty())
          {
            this.br.setLength(0);
            this.br.append(this.loggerLabel).append("Not using data: MSISDN=").append(moRecord.getMsisdn());
            

            this.logger.info(this.br);
            moRecord.setErrCode("12");
            moRecord.setErrOcs("Not using data");
            moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("msg_not_register_" + language, this.logger));
          }
          else
          {
            DataPacket dataPacketOffExtend = null;
            if (moRecord.getParam() != null)
            {
              String dataOffName = moRecord.getParam().split("\\s+")[0].trim();
              dataPacketOffExtend = Commons.getInstance("PROCESS").getDataPacketByName(dataOffName, moRecord.getSubType().intValue());
            }
            for (DataSubscriber dataSubscriber : listSub) {
              if ((dataPacketOffExtend == null) && (dataSubscriber.getAutoExtend() > 0)) {
                listCancel.add(dataSubscriber);
              } else if ((dataSubscriber.getAutoExtend() > 0) && (dataSubscriber.getDataName().equals(dataPacketOffExtend.getName()))) {
                listCancel.add(dataSubscriber);
              }
            }
            String message = "";
            if (listCancel.isEmpty())
            {
              message = Commons.getInstance("PROCESS").getConfig("msg_not_register_" + language, this.logger);
            }
            else
            {
              message = Commons.getInstance("PROCESS").getConfig("msg_off_extend_success_" + language, this.logger);
              
              message = message.replace("%package%", moRecord.getParam() == null ? "Mobile Internet" : moRecord.getParam());
            }
            moRecord.setMessage(message);
          }
        }
      }
    }
    if (!listCancel.isEmpty()) {
      this.db.cancelAutoExtendSubcriber(listCancel);
    }
    return list;
  }
  
  public void printListRecord(List<Record> list)
    throws Exception
  {
    this.br.setLength(0);
    this.br.append("Process list record\n").append(String.format("|%1$-11s|%2$-15s|%3$-30s|%4$-5s", new Object[] { "MO_ID", "MSISDN", "COMMAND", "ACTION_TYPE" })).append("\n");
    for (Record record : list)
    {
      MoRecord moRecord = (MoRecord)record;
      this.br.append(String.format("|%1$-11d|%2$-15s|%3$-30s|%4$-5d", new Object[] { Long.valueOf(moRecord.getID()), moRecord.getMsisdn(), moRecord.getCommand(), moRecord.getActionType() })).append("\n");
    }
    this.logger.info(this.br);
  }
  
  public List<Record> processException(List<Record> list)
  {
    for (Record record : list)
    {
      MoRecord moRecord = (MoRecord)record;
      if ((moRecord.getErrCode() == null) || (moRecord.getErrCode().equals("")))
      {
        moRecord.setErrCode("99");
        moRecord.setErrOcs("Exception");
        try
        {
          moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("system_fail", this.logger));
        }
        catch (Exception ex)
        {
          moRecord.setMessage("System fail");
        }
      }
    }
    return list;
  }
}