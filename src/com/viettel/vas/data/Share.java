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
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.ResourceBundle;
import org.apache.log4j.Logger;
import utils.Config;

public class Share
  extends ProcessRecordAbstract
{
  private String loggerLabel = Share.class.getSimpleName() + ": ";
  private DbProcessor db;
  private DbPost dbPost;
  private DbPre dbPre;
  private Services services;
  private StringBuilder br = new StringBuilder();
  private String countryCode;
  private List<String> listBalances;
  private SimpleDateFormat sdfPro = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  private SimpleDateFormat sdfPro2 = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss a");
  private String listDataName = "";
  private String dataBalanceA = "28";
  private String dataBalanceB = "25";
  private String regex = "\\d+";
  
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
    Commons.createInstance("PROVISIONING", this.db, this.logger);
    this.services = new Services(ExchangeClientChannel.getInstance(Config.configDir + File.separator + "service_client.cfg").getInstanceChannel(), this.db, this.logger);
    

    this.listDataName = Commons.getInstance("PROCESS").getConfig("share_data_list_fly_allow_share", this.logger);
    
    this.countryCode = ResourceBundle.getBundle("vas").getString("country_code");
    this.listBalances = new ArrayList();
    this.listBalances.add(this.dataBalanceA);
  }
  
  public List<Record> validateContraint(List<Record> list)
    throws Exception
  {
    for (Record record : list)
    {
      MoRecord moRecord = (MoRecord)record;
      try
      {
        HashMap map = new HashMap();
        moRecord.setHashMap(map);
        String language = Commons.defaultLang;
        if ((moRecord.getParam().split(" ").length != 2) || (!moRecord.getParam().split(" ")[0].matches(this.regex)) || (!moRecord.getParam().split(" ")[1].matches(this.regex)))
        {
          this.br.setLength(0);
          this.br.append(this.loggerLabel).append("Parameter is incorrect MSISDN=").append(moRecord.getMsisdn());
          

          this.logger.error(this.br);
          moRecord.setErrCode("99");
          moRecord.setErrOcs("Parameter incorrect");
          moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("msg_syntax_fail_" + language, this.logger));
          
          continue;
        }
        if (format(moRecord.getParam().split(" ")[0]).equalsIgnoreCase(moRecord.getMsisdn()))
        {
          this.br.setLength(0);
          this.br.append(this.loggerLabel).append("MsisdnA == MsisdnB: MSISDN=").append(moRecord.getMsisdn());
          

          this.logger.error(this.br);
          moRecord.setErrCode("2");
          moRecord.setErrOcs("Share to same msisdn");
          moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("share_data_same_msisdn_" + language, this.logger));
          
          continue;
        }
        String subInfoA = this.dbPre.getSubInfoMobile(moRecord.getMsisdn().substring(this.countryCode.length()));
        String subInfoB = this.dbPre.getSubInfoMobile(format(moRecord.getParam().split(" ")[0]).substring(this.countryCode.length()));
        if ((subInfoA == null) || (subInfoB == null))
        {
          this.br.setLength(0);
          this.br.append(this.loggerLabel).append("Fail to get subscriber info on CM_PRE: MSISDN=").append(moRecord.getMsisdn());
          

          this.logger.error(this.br);
          moRecord.setErrCode("2");
          moRecord.setErrOcs("Fail to get subscriber info on CM_PRE");
          moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("system_fail_" + language, this.logger));
          
          continue;
        }
        if (subInfoA.equals("NO_INFO_SUB"))
        {
          subInfoA = this.dbPost.getSubInfoMobile(moRecord.getMsisdn().substring(this.countryCode.length()));
          if (subInfoA == null)
          {
            this.br.setLength(0);
            this.br.append(this.loggerLabel).append("Fail to get subscriber info on CM_PRE: MSISDN=").append(moRecord.getMsisdn());
            

            this.logger.error(this.br);
            moRecord.setErrCode("2");
            moRecord.setErrOcs("Fail to get subscriber info on CM_PRE");
            moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("system_fail_" + language, this.logger));
            
            continue;
          }
          if (subInfoA.equals("NO_INFO_SUB"))
          {
            this.br.setLength(0);
            this.br.append(this.loggerLabel).append("Subscriber is not mobile number: MSISDN=").append(moRecord.getMsisdn());
            

            this.logger.info(this.br);
            moRecord.setErrCode("6");
            moRecord.setErrOcs("Subscriber is not mobile number");
            moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("not_info_sub_" + language, this.logger));
            
            continue;
          }
          moRecord.setSubType(Integer.valueOf(0));
        }
        else
        {
          moRecord.setSubType(Integer.valueOf(1));
        }
        if (subInfoB.equals("NO_INFO_SUB"))
        {
          this.br.setLength(0);
          this.br.append(this.loggerLabel).append("Subscriber is not mobile number: MSISDN=").append(moRecord.getMsisdn());
          

          this.logger.info(this.br);
          moRecord.setErrCode("6");
          moRecord.setErrOcs("Subscriber is not mobile number");
          moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("share_data_b_not_found_" + language, this.logger));
          
          continue;
        }
        this.logger.info("SUB_INFO:\n" + subInfoA);
        SubscriberData subscriberA = new SubscriberData(moRecord.getMsisdn(), subInfoA);
        SubscriberData subscriberB = new SubscriberData(moRecord.getMsisdn(), subInfoB);
        if ((!subscriberA.getActStatus().equals("00")) && (!subscriberA.getActStatus().equals("000")))
        {
          this.br.setLength(0);
          this.br.append(this.loggerLabel).append("SubscriberA is not active: MSISDN=").append(moRecord.getMsisdn());
          

          this.logger.info(this.br);
          moRecord.setErrCode("6");
          moRecord.setErrOcs("SubscriberA is not active");
          moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("share_data_a_not_enough_condition_" + language, this.logger));
          
          continue;
        }
        if ((!subscriberB.getActStatus().equals("00")) && (!subscriberB.getActStatus().equals("000")))
        {
          this.br.setLength(0);
          this.br.append(this.loggerLabel).append("SubscriberB is not active: MSISDN=").append(moRecord.getMsisdn());
          

          this.logger.info(this.br);
          moRecord.setErrCode("6");
          moRecord.setErrOcs("SubscriberB is not active");
          moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("share_data_b_not_enough_condition_" + language, this.logger));
          
          continue;
        }
        language = subscriberA.getLanguage();
        moRecord.getHashMap().put("LANGUAGE", language);
        String currentService = Commons.parseCurrentVas(subscriberA.getVasList(), "-");
        moRecord.getHashMap().put("CURRENT_SERVICE", currentService);
        
        moRecord.setSubId(Long.valueOf(subscriberA.getSubId()));
        moRecord.setProductCode(subscriberA.getProductCode());
        moRecord.setErrCode("0");
      }
      catch (Exception ex)
      {
        this.logger.error("Error", ex);
        moRecord.setErrCode("99");
        moRecord.setErrOcs("Exception");
        moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("system_fail", this.logger));
      }
    }
    return list;
  }
  
  public List<Record> processListRecord(List<Record> list)
    throws Exception
  {
    for (Record record : list)
    {
      MoRecord moRecord = (MoRecord)record;
      if (moRecord.getErrCode().equals("0"))
      {
        String msisdnA = moRecord.getMsisdn();
        String msisdnB = format(moRecord.getParam().split(" ")[0]);
        DataPacket dataPacketA = null;
        long amount = Long.valueOf(moRecord.getParam().split(" ")[1]).longValue();
        long dataOfA = 0L;
        long remainDataOfA = 0L;
        boolean allowShare = false;
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
        else if (listSub.isEmpty())
        {
          this.br.setLength(0);
          this.br.append(this.loggerLabel).append("Not using data: MSISDN=").append(moRecord.getMsisdn());
          

          this.logger.info(this.br);
          moRecord.setErrCode("12");
          moRecord.setErrOcs("Not using data");
          moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("share_data_no_fly_" + language, this.logger));
        }
        else
        {
          for (DataSubscriber dataSub : listSub) {
            if (this.listDataName.contains(dataSub.getDataName()))
            {
              allowShare = true;
              dataPacketA = Commons.getInstance("PROCESS").getDataPacketByName(dataSub.getDataName(), moRecord.getSubType().intValue());
            }
          }
          if (!allowShare)
          {
            this.br.setLength(0);
            this.br.append(this.loggerLabel).append("Not using data: MSISDN=").append(moRecord.getMsisdn());
            

            this.logger.info(this.br);
            moRecord.setErrCode("12");
            moRecord.setErrOcs("Not using data");
            moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("share_data_fly_not_allow_" + language, this.logger));
          }
          else
          {
            HashMap<String, String> listBalance = this.services.getDataPromotionPreFull(msisdnA, this.listBalances);
            
            dataOfA = Long.valueOf(listBalance.get(this.dataBalanceA) == null ? "0" : (String)listBalance.get(this.dataBalanceA)).longValue();
            remainDataOfA = dataOfA - 1024L * amount;
            if (remainDataOfA < 0L)
            {
              this.br.setLength(0);
              this.br.append(this.loggerLabel).append("Not using data: MSISDN=").append(moRecord.getMsisdn());
              

              this.logger.info(this.br);
              moRecord.setErrCode("12");
              moRecord.setErrOcs("Not using data");
              moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("share_data_not_enough_" + language, this.logger));
            }
            else
            {
              String expireA = (String)listBalance.get(this.dataBalanceA + "_EXP");
              Calendar cal = Calendar.getInstance();
              cal.setTimeInMillis(System.currentTimeMillis());
              cal.add(5, 1);
              if (this.sdfPro2.parse(expireA).before(cal.getTime()))
              {
                this.br.setLength(0);
                this.br.append(this.loggerLabel).append("Expire time less than 24 hour: MSISDN=").append(moRecord.getMsisdn());
                

                this.logger.info(this.br);
                moRecord.setErrCode("12");
                moRecord.setErrOcs("Expire time not enough");
                moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("share_data_not_time_" + language, this.logger));
              }
              else
              {
                this.services.changeBalance(msisdnA, this.dataBalanceA, false, Long.toString(remainDataOfA), null);
                
                this.services.changeBalance(msisdnB, this.dataBalanceB, false, Long.toString(1024L * amount), this.sdfPro.format(cal.getTime()));
                this.services.pcrfAddSub(msisdnB, "DSNext", 1024L * amount, language, this.sdfPro.format(cal.getTime()));
                if ((dataPacketA != null) && (dataPacketA.getPcrfName() != null) && (!dataPacketA.getPcrfName().isEmpty())) {
                  this.services.pcrfAddSub(moRecord.getMsisdn(), dataPacketA.getPcrfName(), remainDataOfA, language, this.sdfPro.format(this.sdfPro2.parse(expireA)));
                }
                String messageA = Commons.getInstance("PROCESS").getConfig("share_data_successful_a_" + language, this.logger);
                
                String messageB = Commons.getInstance("PROCESS").getConfig("share_data_successful_b_" + language, this.logger);
                
                messageA = messageA.replace("%msisdnB%", msisdnB);
                messageA = messageA.replace("%remainA%", Long.toString(remainDataOfA));
                messageA = messageA.replace("%amount%", String.valueOf(amount));
                messageB = messageB.replace("%msisdnA%", msisdnA);
                messageB = messageB.replace("%amount%", String.valueOf(amount));
                MoRecord moRecordB = new MoRecord();
                moRecordB.setMsisdn(msisdnB);
                moRecordB.setMessage(messageB);
                moRecordB.setChannel("865");
                this.db.insertMT(moRecordB);
                moRecord.setMessage(messageA);
              }
            }
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
  
  private String format(String msisdn)
  {
    if (msisdn.startsWith("237")) {
      return msisdn;
    }
    if (msisdn.startsWith("0")) {
      return "237" + msisdn.substring(1);
    }
    return "237" + msisdn;
  }
}

