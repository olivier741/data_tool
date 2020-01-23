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
import com.viettel.vas.data.database.DbProduct;
import com.viettel.vas.data.obj.ChargeLog;
import com.viettel.vas.data.obj.DataPacket;
import com.viettel.vas.data.obj.DataSubscriber;
import com.viettel.vas.data.obj.QuotaPacket;
import com.viettel.vas.data.obj.SubscriberData;
import com.viettel.vas.data.service.Services;
import com.viettel.vas.data.utils.Commons;
import com.viettel.vas.util.ConnectionPoolManager;
import com.viettel.vas.util.ExchangeClientChannel;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.ResourceBundle;
import org.apache.log4j.Logger;
import utils.Config;

public class BuyData
  extends ProcessRecordAbstract
{
  private String loggerLabel = BuyData.class.getSimpleName() + ": ";
  private DbProcessor db;
  private DbPost dbPost;
  private DbPre dbPre;
  private DbProduct dbProduct;
  private Services services;
  private static SimpleDateFormat formatMsg = new SimpleDateFormat("dd/MM/yyyy");
  private static SimpleDateFormat formatPro = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
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
    this.dbProduct = new DbProduct("dbproduct", this.logger);
    Commons.createInstance("PROCESS", this.db, this.logger);
    Commons.createInstance("PROVISIONING", this.db, this.logger);
    this.services = new Services(ExchangeClientChannel.getInstance(Config.configDir + File.separator + "service_client.cfg").getInstanceChannel(), this.db, this.logger);
    


    this.countryCode = ResourceBundle.getBundle("vas").getString("country_code");
  }
  
  public List<Record> validateContraint(List<Record> list)
    throws Exception
  {
    for (Record record : list)
    {
      MoRecord moRecord = (MoRecord)record;
      String language = Commons.defaultLang;
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
        moRecord.setSubId(Long.valueOf(subscriber.getSubId()));
        moRecord.setProductCode(subscriber.getProductCode());
        
        String currentService = Commons.parseCurrentVas(subscriber.getVasList(), "-");
        moRecord.getHashMap().put("CURRENT_SERVICE", currentService);
        moRecord.setErrCode("0");
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
        String pcrfName = "";
        String pcrfBalanceId = "";
        String pricePlanId = "";
        Date expireTime = new Date();
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
          List<DataSubscriber> listSubCheck = new ArrayList();
          listSubCheck.addAll(listSub);
          listSub.clear();
          for (DataSubscriber dataSubscriber : listSubCheck) {
            if ((dataSubscriber.getExpireTime() != null) && (dataSubscriber.getExpireTime().getTime() < System.currentTimeMillis()) && (dataSubscriber.getAutoExtend() == 0))
            {
              this.logger.info("Packet " + dataSubscriber.getDataName() + " is expired");
              this.logger.warn("Please check auto extend thread for this packet: " + dataSubscriber.getMsisdn() + "=>" + dataSubscriber.getDataName());
            }
            else
            {
              listSub.add(dataSubscriber);
              DataPacket dataPacket = Commons.getInstance("PROCESS").getDataPacketByName(dataSubscriber.getDataName(), moRecord.getSubType().intValue());
              if ((dataPacket != null) && (dataPacket.getPcrfName() != null) && (!dataPacket.getPcrfName().isEmpty()))
              {
                pcrfName = dataPacket.getPcrfName();
                pcrfBalanceId = dataPacket.getPcrfBalanceId();
                pricePlanId = dataPacket.getPriceOcs();
                expireTime = dataSubscriber.getExpireTime();
              }
            }
          }
          listSubCheck.clear();
          if (listSub.isEmpty())
          {
            this.br.setLength(0);
            this.br.append(this.loggerLabel).append("Not using Data3G: MSISDN=").append(moRecord.getMsisdn());
            

            this.logger.info(this.br);
            moRecord.setErrCode("12");
            moRecord.setErrOcs("Not using Data3G");
            moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("msg_not_register_" + language, this.logger));
          }
          else
          {
            String content = moRecord.getCommand().toUpperCase();
            content = content.replace("O", "0").replace("\\s+", "\\s");
            
            QuotaPacket quotaPacket = Commons.getInstance("PROCESS").getQuotaPacket(content);
            if (quotaPacket == null)
            {
              this.br.setLength(0);
              this.br.append(this.loggerLabel).append("Not found quota packet: MSISDN=").append(moRecord.getMsisdn());
              

              this.logger.info(this.br);
              moRecord.setErrCode("17");
              moRecord.setErrOcs("Quota packet not found");
              moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("msg_quota_packet_not_found_" + language, this.logger));
            }
            else
            {
              if ((moRecord.getSubType().intValue() == 1) || (moRecord.getSubType().intValue() == 0))
              {
                double balance = this.services.checkMoney(moRecord.getMsisdn(), "1");
                if (balance == -9999999.0D)
                {
                  this.br.setLength(0);
                  this.br.append("Error get balance").append(": MSISDN=").append(moRecord.getMsisdn());
                  

                  this.logger.error(this.br);
                  moRecord.setErrCode("5");
                  moRecord.setErrOcs("Error get balance");
                  moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("system_fail_" + language, this.logger));
                  
                  continue;
                }
                if (balance < quotaPacket.getFee())
                {
                  this.br.setLength(0);
                  this.br.append("Not enough balance").append(": MSISDN=").append(moRecord.getMsisdn());
                  

                  this.logger.error(this.br);
                  moRecord.setErrCode("5");
                  moRecord.setErrOcs("Balance not enough");
                  moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("msg_not_enough_money_" + language, this.logger));
                  
                  continue;
                }
                String addQuota = this.services.pcrfAddSubOCS(moRecord.getMsisdn(), pcrfBalanceId, String.valueOf(quotaPacket.getQuota()), pricePlanId, formatPro.format(expireTime));
                if (addQuota.equals("0"))
                {
                  String addOcs = this.services.changeBalance(moRecord.getMsisdn(), quotaPacket.getBalanceId(), true, String.valueOf(quotaPacket.getQuota()), null);
                  if (!addOcs.equals("0"))
                  {
                    this.br.setLength(0);
                    this.br.append("Add balance OCS fail").append(": MSISDN=").append(moRecord.getMsisdn());
                    

                    this.logger.error(this.br);
                  }
                  this.br.setLength(0);
                  this.br.append("Add quota PCRF successfull").append(": MSISDN=").append(moRecord.getMsisdn());
                  

                  this.logger.info(this.br);
                }
                else
                {
                  this.br.setLength(0);
                  this.br.append("Add quota PCRF fail").append(": MSISDN=").append(moRecord.getMsisdn());
                  

                  this.logger.error(this.br);
                  moRecord.setErrCode("5");
                  moRecord.setErrOcs("Add Quota PCRF fail");
                  moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("system_fail_" + language, this.logger));
                  
                  continue;
                }
                List<ChargeLog> listChargeLog = new ArrayList();
                
                String errCharge = this.services.chargeMoney(moRecord.getMsisdn(), quotaPacket.getFee());
                ChargeLog chargeLog = new ChargeLog(moRecord.getMsisdn(), moRecord.getSubId().longValue(), quotaPacket.getName(), 3, "1", "-" + quotaPacket.getFee(), errCharge);
                
                this.db.insertChargLog(chargeLog);
                if (!errCharge.equals("0"))
                {
                  moRecord.setErrCode("1");
                  moRecord.setErrOcs("charge fail");
                  moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("system_fail_" + language, this.logger));
                  
                  continue;
                }
              }
              this.br.setLength(0);
              this.br.append(this.loggerLabel).append("Buy data success: MSISDN=").append(moRecord.getMsisdn());
              

              this.logger.info(this.br);
              moRecord.setFee(Double.valueOf(Double.parseDouble(String.valueOf(quotaPacket.getFee()))));
              moRecord.setErrCode("0");
              String message = Commons.getInstance("PROCESS").getConfig("msg_buy_data_success_" + language, this.logger);
              
              message = message.replace("%data%", Services.doubleToString(quotaPacket.getQuota() / 1024L));
              message = message.replace("%total%", Services.doubleToString(quotaPacket.getQuota() / 1024L));
              message = message.replaceAll("%expire_date%", expireTime != null ? formatMsg.format(expireTime) : "unlimit");
              message = message.replaceAll("%expire_hour%", "" + (expireTime != null ? Integer.valueOf(expireTime.getHours()) : ""));
              moRecord.setMessage(message);
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
}

