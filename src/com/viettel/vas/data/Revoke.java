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
import com.viettel.vas.data.obj.CDR;
import com.viettel.vas.data.obj.ChargeLog;
import com.viettel.vas.data.obj.CmPricePlan;
import com.viettel.vas.data.obj.DataAction;
import com.viettel.vas.data.obj.DataPacket;
import com.viettel.vas.data.obj.DataServicePos;
import com.viettel.vas.data.obj.DataServicePre;
import com.viettel.vas.data.obj.DataSubscriber;
import com.viettel.vas.data.obj.PacketExtra;
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
import java.util.Set;
import org.apache.log4j.Logger;
import utils.Config;

public class Revoke
  extends ProcessRecordAbstract
{
  private String loggerLabel = Revoke.class.getSimpleName() + ": ";
  private DbProcessor db;
  private DbProduct dbProduct;
  private DbPost dbPost;
  private DbPre dbPre;
  private Services services;
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
        boolean isUnlimit = false;
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
            List<String> listChange = new ArrayList();
            boolean isFail = false;
            List listRollback = new ArrayList();
            List<String> listBalanceReset = new ArrayList();
            List<CDR> listCdr = new ArrayList();
            List<CDR> listPreCdr = new ArrayList();
            List<CDR> listPostCdr = new ArrayList();
            List<DataSubscriber> listSubRevoke = new ArrayList();
            
            List<DataSubscriber> listSubReg = new ArrayList();
            for (DataSubscriber dataSubscriber : listSub)
            {
              DataPacket dataOld = Commons.getInstance("PROCESS").getDataPacketByName(dataSubscriber.getDataName(), dataSubscriber.getSubType());
              if ((dataOld.getPcrfName() != null) && (!dataOld.getPcrfName().isEmpty())) {
                isUnlimit = true;
              }
              DataPacket dataNew = null;
              String newVasCode = "";
              String oldVasCode = dataOld.getVasCode();
              DataAction dataAction = Commons.getInstance("PROCESS").getDataAction(dataSubscriber.getDataName(), null, moRecord.getProductCode(), "0");
              if ((dataAction != null) && (dataAction.getFollowData() != null) && (dataAction.getFollowData().trim().length() > 0))
              {
                dataNew = Commons.getInstance("PROCESS").getDataPacketByName(dataAction.getFollowData(), dataSubscriber.getSubType());
                
                newVasCode = dataNew.getVasCode();
                listChange.add(dataAction.getFollowData());
              }
              Object dataObj = getPricePlan(moRecord, dataOld, dataNew, dataSubscriber, listChange);
              if (dataObj == null)
              {
                isFail = true;
                moRecord.setErrCode("5");
                moRecord.setErrOcs("Fail to get price plan product");
                moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("system_fail_" + language, this.logger));
                
                break;
              }
              String error = "";
              if (moRecord.getSubType().intValue() == 1)
              {
                DataServicePre dataServicePre = (DataServicePre)dataObj;
                
                error = this.services.changeDataPre(dataServicePre, true);
                if (!error.equals("0"))
                {
                  this.br.setLength(0);
                  this.br.append("Revoke fail: MSISDN=").append(moRecord.getMsisdn()).append(" - DATA=").append(dataOld.getName());
                  
                  this.logger.error(this.br);
                  isFail = true;
                  moRecord.setErrCode("5");
                  moRecord.setErrOcs("Revoke fail data=" + dataOld.getName() + " error: " + error);
                  moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("system_fail_" + language, this.logger));
                  
                  break;
                }
                this.br.setLength(0);
                this.br.append("Revoke success: MSISDN=").append(moRecord.getMsisdn()).append(" - DATA=").append(dataOld.getName());
                
                this.logger.info(this.br);
                listRollback.add(dataServicePre.cloneRollback());
                if ((dataAction != null) && (dataAction.isResetExtra()))
                {
                  this.logger.info("Reset balance extra: MSISDN=" + moRecord.getMsisdn());
                  List<PacketExtra> listExtra = dataOld.getListExtraForProduct(moRecord.getProductCode());
                  for (PacketExtra packetExtra : listExtra) {
                    listBalanceReset.add(packetExtra.getBalanceId());
                  }
                }
                CDR cdr = new CDR(oldVasCode, moRecord.getChannel(), moRecord.getMsisdn(), moRecord.getSubId() + "", "", "1", "0", "", "Cancel product " + dataOld.getName(), "0");
                

                cdr.setFileTypeID(moRecord.getSubType().intValue() == 1 ? CDR.PRE_3G : CDR.POS_3G);
                listCdr.add(cdr);
                if (cdr.getFileTypeID() == CDR.PRE_3G) {
                  listPreCdr.add(cdr);
                } else {
                  listPostCdr.add(cdr);
                }
                if ((("-" + currentService + "-").indexOf("-" + dataOld.getVasCode() + "-") >= 0) && (dataSubscriber.getID() <= 0L))
                {
                  this.br.setLength(0);
                  this.br.append("Only unregister on BCCS and OCS, no info on Database: MSISDN=").append(moRecord.getMsisdn()).append(" - DATA_PACKET=").append(dataOld.getName());
                  
                  this.logger.info(this.br);
                }
                else
                {
                  dataSubscriber.setActNote("1");
                  listSubRevoke.add(dataSubscriber);
                }
                if ((dataNew != null) && (!listChange.contains(dataNew.getName())))
                {
                  CDR cdrChange = new CDR(newVasCode, moRecord.getChannel(), moRecord.getMsisdn(), moRecord.getSubId() + "", "", "0", "0", "0", "Register product " + dataNew.getName(), "0");
                  


                  cdrChange.setFileTypeID(moRecord.getSubType().intValue() == 1 ? CDR.PRE_3G : CDR.POS_3G);
                  listCdr.add(cdrChange);
                  if (cdr.getFileTypeID() == CDR.PRE_3G) {
                    listPreCdr.add(cdr);
                  } else {
                    listPostCdr.add(cdr);
                  }
                  DataSubscriber newSub = new DataSubscriber();
                  newSub.setMsisdn(moRecord.getMsisdn());
                  newSub.setSubId(moRecord.getSubId().longValue());
                  newSub.setProductCode(moRecord.getProductCode());
                  newSub.setDataName(dataNew.getName());
                  newSub.setSubType(moRecord.getSubType().intValue());
                  newSub.setExpireTime(dataServicePre.getExpire());
                  listSubReg.add(newSub);
                }
              }
              else
              {
                DataServicePos dataServicePos = (DataServicePos)dataObj;
                error = this.services.changeDataPos(dataServicePos, true);
                if (!error.equals("0"))
                {
                  this.br.setLength(0);
                  this.br.append("Revoke fail: MSISDN=").append(moRecord.getMsisdn()).append(" - DATA=").append(dataOld.getName());
                  
                  this.logger.error(this.br);
                  isFail = true;
                  moRecord.setErrCode("5");
                  moRecord.setErrOcs("Revoke fail data=" + dataOld.getName() + " error: " + error);
                  moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("system_fail_" + language, this.logger));
                  
                  break;
                }
                this.br.setLength(0);
                this.br.append("Revoke success: MSISDN=").append(moRecord.getMsisdn()).append(" - DATA=").append(dataOld.getName());
                
                this.logger.info(this.br);
                listRollback.add(dataServicePos.cloneRollback());
                

                CDR cdr = new CDR(oldVasCode, moRecord.getChannel(), moRecord.getMsisdn(), moRecord.getSubId() + "", "", "1", "0", "", "Cancel product " + dataOld.getName(), "0");
                

                cdr.setFileTypeID(moRecord.getSubType().intValue() == 1 ? CDR.PRE_3G : CDR.POS_3G);
                listCdr.add(cdr);
                if (cdr.getFileTypeID() == CDR.PRE_3G) {
                  listPreCdr.add(cdr);
                } else {
                  listPostCdr.add(cdr);
                }
                if ((("-" + currentService + "-").indexOf("-" + dataOld.getVasCode() + "-") >= 0) && (dataSubscriber.getID() <= 0L))
                {
                  this.br.setLength(0);
                  this.br.append("Only unregister on BCCS and OCS, no info on Database: MSISDN=").append(moRecord.getMsisdn()).append(" - DATA_PACKET=").append(dataOld.getName());
                  
                  this.logger.info(this.br);
                }
                else
                {
                  dataSubscriber.setActNote("1");
                  listSubRevoke.add(dataSubscriber);
                }
                if ((dataNew != null) && (!listChange.contains(dataNew.getName())))
                {
                  CDR cdrChange = new CDR(newVasCode, moRecord.getChannel(), moRecord.getMsisdn(), moRecord.getSubId() + "", "", "0", "0", "0", "Register product " + dataNew.getName(), "0");
                  


                  cdrChange.setFileTypeID(moRecord.getSubType().intValue() == 1 ? CDR.PRE_3G : CDR.POS_3G);
                  listCdr.add(cdrChange);
                  if (cdr.getFileTypeID() == CDR.PRE_3G) {
                    listPreCdr.add(cdr);
                  } else {
                    listPostCdr.add(cdr);
                  }
                  DataSubscriber newSub = new DataSubscriber();
                  newSub.setMsisdn(moRecord.getMsisdn());
                  newSub.setSubId(moRecord.getSubId().longValue());
                  newSub.setProductCode(moRecord.getProductCode());
                  newSub.setDataName(dataNew.getName());
                  newSub.setSubType(moRecord.getSubType().intValue());
                  newSub.setExpireTime(dataServicePos.getExpire());
                  listSubReg.add(newSub);
                }
              }
            }
            if (isFail)
            {
              listCdr.clear();
              listPreCdr.clear();
              listPostCdr.clear();
              listSubReg.clear();
              listSubRevoke.clear();
              
              this.logger.info("Rollback data for MSISDN=" + moRecord.getMsisdn());
              String error;
              for (int i = listRollback.size() - 1; i >= 0; i--)
              {
                if (moRecord.getSubType().intValue() == 1)
                {
                  DataServicePre dataServicePre = (DataServicePre)listRollback.get(i);
                  error = this.services.changeDataPre(dataServicePre, true);
                }
                else
                {
                  DataServicePos dataServicePos = (DataServicePos)listRollback.get(i);
                  error = this.services.changeDataPos(dataServicePos, true);
                }
              }
            }
            else
            {
              if (moRecord.getSubType().intValue() == 1)
              {
                this.logger.info("Reset balance promotion: MSISDN=" + moRecord.getMsisdn());
                for (String balanceId : listBalanceReset)
                {
                  String err = this.services.changeBalance(moRecord.getMsisdn(), balanceId, false, "0", null);
                  
                  ChargeLog chargeLogExtra = new ChargeLog(moRecord.getMsisdn(), moRecord.getSubId().longValue(), "", 2, balanceId, "0", err);
                  
                  this.db.insertChargLog(chargeLogExtra);
                }
              }
              if ((!listCdr.isEmpty()) && (!Commons.insertVasRegister)) {
                this.db.insertCDR(listCdr);
              }
              if ((!listPreCdr.isEmpty()) && (Commons.insertVasRegister)) {
                this.dbPre.insertVasRegister(listPreCdr);
              }
              if ((!listPostCdr.isEmpty()) && (Commons.insertVasRegister)) {
                this.dbPost.insertVasRegister(listPostCdr);
              }
              if (!listSubRevoke.isEmpty()) {
                this.db.revokeDataSubcriber(listSubRevoke);
              }
              if (!listSubReg.isEmpty()) {
                this.db.insertDataSubcriber(listSubReg);
              }
              if (isUnlimit)
              {
                String removePcrf = "0";
                if (removePcrf.equals("0"))
                {
                  this.br.setLength(0);
                  this.br.append("Remove sub in PCRF success: MSISDN=").append(moRecord.getMsisdn());
                  this.logger.info(this.br);
                }
                else
                {
                  this.br.setLength(0);
                  this.br.append("Remove sub in PCRF error: MSISDN=").append(moRecord.getMsisdn());
                  this.logger.error(this.br);
                }
              }
              this.br.setLength(0);
              this.br.append("Revoke all data success: MSISDN=").append(moRecord.getMsisdn());
              this.logger.info(this.br);
              
              this.br.setLength(0);
              for (DataSubscriber dataSubscriber : listSubRevoke) {
                this.br.append(dataSubscriber.getDataName()).append(",");
              }
              moRecord.setErrOcs("Revoke all data success: " + this.br.toString());
              String message = Commons.getInstance("PROCESS").getConfig("msg_remove_success_" + language, this.logger);
              
              moRecord.setMessage(message);
            }
          }
        }
      }
    }
    return list;
  }
  
  private Object getPricePlan(MoRecord moRecord, DataPacket dataOld, DataPacket dataNew, DataSubscriber dataSubscriber, List<String> listChange)
  {
    try
    {
      String language = moRecord.getHashMap().get("LANGUAGE").toString();
      String pricePlanNew = "";
      String pricePlanOld = "";
      String hlrTplNew = "";
      String hlrTplOld = "";
      Date expire = new Date();
      String currentVas = "";
      if (moRecord.getHashMap().get("CURRENT_SERVICE") != null) {
        currentVas = (String)moRecord.getHashMap().get("CURRENT_SERVICE");
      }
      List<CmPricePlan> listPricePlanNew = new ArrayList();
      if ((dataNew != null) && (!listChange.contains(dataNew.getName())))
      {
        expire = dataNew.getExpireTime(this.logger);
        
        listPricePlanNew = this.dbProduct.getListPricePlanByVasId(moRecord.getProductCode(), dataNew.getVasCode(), moRecord.getSubType().intValue());
        if ((listPricePlanNew == null) || (listPricePlanNew.size() < 1))
        {
          this.br.setLength(0);
          this.br.append("Fail to get new product priceplan: MSISDN=").append(moRecord.getMsisdn());
          
          this.logger.info(this.br);
          moRecord.setErrCode("3");
          moRecord.setErrOcs("Fail to get new product priceplan");
          moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("system_fail_" + language, this.logger));
          
          return null;
        }
      }
      List<CmPricePlan> listPricePlanOld = new ArrayList();
      if (dataOld != null)
      {
        listPricePlanOld = this.dbProduct.getListPricePlanByVasId(moRecord.getProductCode(), dataOld.getVasCode(), moRecord.getSubType().intValue());
        if ((listPricePlanOld == null) || (listPricePlanOld.size() < 1))
        {
          this.br.setLength(0);
          this.br.append("Fail to get old product priceplan: MSISDN=").append(moRecord.getMsisdn());
          
          this.logger.info(this.br);
          moRecord.setErrCode("3");
          moRecord.setErrOcs("Fail to get old product priceplan");
          moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("system_fail_" + language, this.logger));
          
          return null;
        }
      }
      if (moRecord.getSubType().intValue() == 1)
      {
        for (CmPricePlan pricePlan : listPricePlanOld)
        {
          this.logger.info("list_price_old");
          this.logger.info("exchange_id=" + pricePlan.getExchangeId() + "=>" + pricePlan.getPricePlanCode());
          if (Commons.getInstance("PROVISIONING").getConfig("priceplan_pre_exchange_id", this.logger).equals(pricePlan.getExchangeId()))
          {
            pricePlanOld = String.valueOf(pricePlan.getPricePlanCode());
            this.logger.info("PricePlanOld= " + pricePlanOld);
          }
          else if (Commons.getInstance("PROVISIONING").getConfig("hlrbroadband_exchange_id", this.logger).equals(pricePlan.getExchangeId()))
          {
            hlrTplOld = String.valueOf(pricePlan.getPricePlanCode());
            this.logger.info("HlrTplOld= " + hlrTplOld);
          }
        }
        if (dataNew != null) {
          for (CmPricePlan pricePlan : listPricePlanNew)
          {
            this.logger.info("list_price_new");
            this.logger.info("exchange_id=" + pricePlan.getExchangeId() + "=>" + pricePlan.getPricePlanCode());
            if (Commons.getInstance("PROVISIONING").getConfig("priceplan_pre_exchange_id", this.logger).equals(pricePlan.getExchangeId()))
            {
              pricePlanNew = String.valueOf(pricePlan.getPricePlanCode());
              this.logger.info("PricePlanNew= " + pricePlanNew);
            }
            else if (Commons.getInstance("PROVISIONING").getConfig("hlrbroadband_exchange_id", this.logger).equals(pricePlan.getExchangeId()))
            {
              hlrTplNew = String.valueOf(pricePlan.getPricePlanCode());
              this.logger.info("HlrTplNew= " + hlrTplNew);
            }
          }
        }
        DataServicePre dataServicePre = new DataServicePre(moRecord.getMsisdn(), pricePlanNew, pricePlanOld, currentVas, hlrTplOld, hlrTplNew);
        
        dataServicePre.setExpire(expire);
        dataServicePre.setExpreOld(dataSubscriber.getExpireTime());
        if (dataServicePre.getRemovePpOcsList().trim().length() == 0)
        {
          this.logger.error("[!] No remove price plan on Product with exchange_type=priceplan_pos_exchange_id");
          return null;
        }
        if ((dataNew != null) && (!listChange.contains(dataNew.getName())) && 
          (dataServicePre.getAddPpOcsList().trim().length() == 0))
        {
          this.logger.error("[!] No add price plan on Product with exchange_type=priceplan_pos_exchange_id");
          return null;
        }
        return dataServicePre;
      }
      for (CmPricePlan pricePlan : listPricePlanOld)
      {
        this.logger.info("list_price_old");
        this.logger.info("exchange_id=" + pricePlan.getExchangeId() + "=>" + pricePlan.getPricePlanCode());
        if (Commons.getInstance("PROVISIONING").getConfig("priceplan_pos_exchange_id", this.logger).equals(pricePlan.getExchangeId()))
        {
          pricePlanOld = String.valueOf(pricePlan.getPricePlanCode());
          this.logger.info("PricePlanOld= " + pricePlanOld);
        }
        else if (Commons.getInstance("PROVISIONING").getConfig("hlrbroadband_exchange_id", this.logger).equals(pricePlan.getExchangeId()))
        {
          hlrTplOld = String.valueOf(pricePlan.getPricePlanCode());
          this.logger.info("HlrtplOld= " + hlrTplOld);
        }
      }
      for (CmPricePlan pricePlan : listPricePlanNew)
      {
        this.logger.info("list_price_new");
        this.logger.info("exchange_id=" + pricePlan.getExchangeId() + "=>" + pricePlan.getPricePlanCode());
        if (Commons.getInstance("PROVISIONING").getConfig("priceplan_pos_exchange_id", this.logger).equals(pricePlan.getExchangeId()))
        {
          pricePlanNew = String.valueOf(pricePlan.getPricePlanCode());
          this.logger.info("PricePlanNew= " + pricePlanNew);
        }
        else if (Commons.getInstance("PROVISIONING").getConfig("hlrbroadband_exchange_id", this.logger).equals(pricePlan.getExchangeId()))
        {
          hlrTplNew = String.valueOf(pricePlan.getPricePlanCode());
          this.logger.info("HlrTplNew= " + hlrTplNew);
        }
      }
      DataServicePos dataServicePos = new DataServicePos(moRecord.getMsisdn(), pricePlanNew, pricePlanOld, currentVas, formatPro.format(new Date()), hlrTplOld, hlrTplNew);
      

      dataServicePos.setExpire(expire);
      dataServicePos.setExpreOld(dataSubscriber.getExpireTime());
      if (dataServicePos.getRemovePpOcsPostList().trim().length() == 0)
      {
        this.logger.error("[!] No remove price plan on Product with exchange_type=priceplan_pos_exchange_id");
        return null;
      }
      if ((dataNew != null) && (!listChange.contains(dataNew.getName())) && 
        (dataServicePos.getAddPpOcsPostList().trim().length() == 0))
      {
        this.logger.error("[!] No add price plan on Product with exchange_type=priceplan_pos_exchange_id");
        return null;
      }
      return dataServicePos;
    }
    catch (Exception ex)
    {
      this.logger.error("Error getPricePlan", ex);
    }
    return null;
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
