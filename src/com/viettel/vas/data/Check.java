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
import com.viettel.vas.data.service.Services;
import com.viettel.vas.data.utils.Commons;
import com.viettel.vas.util.ConnectionPoolManager;
import com.viettel.vas.util.ExchangeClientChannel;
import com.viettel.vas.util.obj.ViettelException;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.ResourceBundle;
import java.util.Set;
import org.apache.log4j.Logger;
import utils.Config;

public class Check
  extends ProcessRecordAbstract
{
  private String loggerLabel = Register.class.getSimpleName() + ": ";
  private DbProcessor db;
  private DbPost dbPost;
  private DbPre dbPre;
  private Services services;
  private StringBuilder br = new StringBuilder();
  private String countryCode;
  private String[] dataBalances;
  private List<String> listBalances;
  private SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
  
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
    this.services = new Services(ExchangeClientChannel.getInstance(Config.configDir + File.separator + "service_client.cfg").getInstanceChannel(), this.db, this.logger);
    

    this.countryCode = ResourceBundle.getBundle("vas").getString("country_code");
    
    String dataBalanceCheck = Commons.getInstance("PROCESS").getConfig("data_balance_check", this.logger);
    
    this.dataBalances = dataBalanceCheck.split(";");
    if ((this.dataBalances == null) || (this.dataBalances.length <= 0)) {
      throw new ViettelException("[!] No config data_balance_check");
    }
    this.listBalances = new ArrayList();
    for (String string : this.dataBalances) {
      this.listBalances.add(string.split("-")[1]);
    }
  }
  
  public List<Record> validateContraint(List<Record> list)
    throws Exception
  {
    for (Record record : list)
    {
      MoRecord moRecord = (MoRecord)record;
      HashMap map = new HashMap();
      moRecord.setHashMap(map);
      String language = Commons.defaultLang;
      
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
          
          boolean hasUnlimit = false;
          String message = "";
          for (DataSubscriber dataSubscriber : listSub)
          {
            DataPacket dataPacket = Commons.getInstance("PROCESS").getDataPacketByName(dataSubscriber.getDataName(), dataSubscriber.getSubType());
            

            String msg = Commons.getInstance("PROCESS").getConfig("msg_check_data_" + dataPacket.getName().toLowerCase() + "_" + language, "msg_check_data_" + language, this.logger);
            if (dataPacket.getRestrictData() > 0.0D)
            {
              this.logger.info("Data used is Unlimit packet");
              hasUnlimit = true;
            }
            if (dataPacket != null) {
              msg = msg.replaceAll("%packet%", dataPacket.getSyntax());
            } else {
              msg = msg.replaceAll("%packet%", dataSubscriber.getDataName());
            }
            msg = msg.replaceAll("%paidtime%", dataSubscriber.getPaidTime() != null ? this.sdf.format(dataSubscriber.getPaidTime()) : "");
            msg = msg.replaceAll("%expiretime%", dataSubscriber.getExpireTime() != null ? this.sdf.format(dataSubscriber.getExpireTime()) : ": no expire");
            
            message = message + msg + " ";
          }
          HashMap<String, String> map = this.services.getDataPromotionPre(moRecord.getMsisdn(), this.listBalances);
          if (map == null)
          {
            this.br.setLength(0);
            this.br.append("Error get data balance promotion on provisioning").append(": MSISDN=").append(moRecord.getMsisdn());
            

            this.logger.error(this.br);
            moRecord.setErrCode("5");
            moRecord.setErrOcs("Error get data balance promotion on provisioning");
            moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("system_fail_" + language, this.logger));
          }
          else
          {
            String dataInfo = "";
            for (String string : this.dataBalances)
            {
              String[] dataBalanceInfo = string.split("-");
              if (map.get(dataBalanceInfo[1]) != null)
              {
                dataInfo = dataInfo + dataBalanceInfo[0] + "=" + (map.get(dataBalanceInfo[1]) == null ? "0" : Double.valueOf(Double.valueOf((String)map.get(dataBalanceInfo[1])).doubleValue() / Double.valueOf(dataBalanceInfo[2]).doubleValue())) + " " + dataBalanceInfo[3] + " ";
                this.logger.info("MSISDN=" + moRecord.getMsisdn() + " => " + dataBalanceInfo[0] + "=" + (map.get(dataBalanceInfo[1]) == null ? "0" : Double.valueOf(Double.valueOf((String)map.get(dataBalanceInfo[1])).doubleValue() / Double.valueOf(dataBalanceInfo[2]).doubleValue())) + " " + dataBalanceInfo[3]);
              }
            }
            if (dataInfo.length() > 0) {
              dataInfo = dataInfo.substring(0, dataInfo.length() - 1);
            }
            message = message + dataInfo;
            moRecord.setMessage(message);
          }
        }
      }
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
