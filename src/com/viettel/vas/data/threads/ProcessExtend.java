/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.vas.data.threads;

/**
 *
 * @author olivier.tatsinkou
 */
import com.viettel.cluster.agent.integration.Record;
import com.viettel.smsfw.process.ProcessRecordAbstract;
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
import com.viettel.vas.data.obj.PostPaid;
import com.viettel.vas.data.obj.Subscriber;
import com.viettel.vas.data.service.Services;
import com.viettel.vas.data.utils.Commons;
import com.viettel.vas.util.ConnectionPoolManager;
import com.viettel.vas.util.ExchangeClientChannel;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.ResourceBundle;
import java.util.regex.Pattern;
import org.apache.log4j.Logger;
import utils.Config;

public class ProcessExtend   extends ProcessRecordAbstract
{
  private String loggerLabel = ProcessExtend.class.getSimpleName() + ": ";
  private DbProcessor db;
  private DbProduct dbProduct;
  private DbPost dbPost;
  private DbPre dbPre;
  private Services services;
  private StringBuilder br = new StringBuilder();
  private String countryCode;
  private String channelExtend;
  private static SimpleDateFormat formatPro = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  
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
    Commons.createInstance("AUTO_EXTEND", this.db, this.logger);
    this.services = new Services(ExchangeClientChannel.getInstance(Config.configDir + File.separator + "service_client.cfg").getInstanceChannel(), this.db, this.logger);
    
    this.countryCode = ResourceBundle.getBundle("vas").getString("country_code");
    this.channelExtend = Commons.getInstance("AUTO_EXTEND").getConfig("channel_extend", this.logger);
    Commons.defaultLang = ResourceBundle.getBundle("vas").getString("default_lang");
  }
  
  public List<Record> validateContraint(List<Record> list)
    throws Exception
  {
    return list;
  }
  
  public List<Record> processListRecord(List<Record> list)
    throws Exception
  {
    List<DataSubscriber> listUnregister = new ArrayList();
    
    List<DataSubscriber> listRegister = new ArrayList();
    
    List<CDR> listCdr = new ArrayList();
    List<CDR> listPreCdr = new ArrayList();
    List<CDR> listPostCdr = new ArrayList();
    List<PostPaid> listPostPaid = new ArrayList();
    for (Record record : list)
    {
      DataSubscriber dataSub = (DataSubscriber)record;
      dataSub.setChannel(this.channelExtend);
      String language = Commons.defaultLang;
      
      DataPacket dataPacketCurrent = Commons.getInstance("PROCESS").getDataPacketByName(dataSub.getDataName(), dataSub.getSubType());
      if (dataPacketCurrent == null)
      {
        this.br.setLength(0);
        this.br.append(this.loggerLabel).append("Packet not found or packet not active: MSISDN=").append(dataSub.getMsisdn());
        

        this.logger.info(this.br);
        dataSub.setErr(-1);
      }
      else
      {
        this.logger.info("DATA_PACKET:\n" + dataPacketCurrent);
        if (dataSub.getAutoExtend() == 0)
        {
          if (dataPacketCurrent.getGroupPacket() != 1)
          {
            dataSub.setErr(1);
            dataSub.setActNote("2");
            listUnregister.add(dataSub);
            
            CDR cdr = new CDR(dataPacketCurrent.getVasCode(), this.channelExtend, dataSub.getMsisdn(), dataSub.getSubId() + "", "", "1", "0", "", "Cancel product " + dataPacketCurrent.getName(), "0");
            

            cdr.setFileTypeID(dataSub.getSubType() == 1 ? CDR.PRE_3G : CDR.POS_3G);
            listCdr.add(cdr);
            if (cdr.getFileTypeID() == CDR.PRE_3G) {
              listPreCdr.add(cdr);
            } else {
              listPostCdr.add(cdr);
            }
          }
          else
          {
            this.logger.info("No extend continous: MSISDN=" + dataSub.getMsisdn());
            dataSub.setErr(1);
            boolean isAdd = true;
            DataPacket dataNew = null;
            DataAction dataAction = Commons.getInstance("PROCESS").getDataAction(dataPacketCurrent.getName(), "", dataSub.getProductCode(), "3");
            if (dataAction != null) {
              isAdd = !dataAction.isResetExtra();
            }
            if ((dataAction != null) && (dataAction.getFollowData() != null) && (dataAction.getFollowData().trim().length() > 0)) {
              dataNew = Commons.getInstance("PROCESS").getDataPacketByName(dataAction.getFollowData(), dataPacketCurrent.getType());
            }
            Object dataService = getPricePlan(dataSub, dataPacketCurrent, dataNew, "");
            if (dataService == null)
            {
              dataSub.setErr(-1);
            }
            else
            {
              Date expire = null;
              if ((dataSub.getSubType() == 1) || (Commons.listHybridProductCode.contains(dataSub.getProductCode())))
              {
                DataServicePre dataServicePre = (DataServicePre)dataService;
                expire = dataServicePre.getExpire();
                String error = this.services.changeDataPre(dataServicePre, true);
                if (!error.equals("0"))
                {
                  this.br.setLength(0);
                  this.br.append("Change to pre packet ").append(dataNew.getName()).append(" fail: MSISDN=").append(dataSub.getMsisdn());
                  
                  this.logger.error(this.br);
                  dataSub.setErr(-1);
                  continue;
                }
              }
              else
              {
                DataServicePos dataServicePos = (DataServicePos)dataService;
                expire = dataServicePos.getExpire();
                String error = this.services.changeDataPos(dataServicePos, true);
                if (!error.equals("0"))
                {
                  this.br.setLength(0);
                  this.br.append("Change to pos packet ").append(dataNew.getName()).append(" fail: MSISDN=").append(dataSub.getMsisdn());
                  
                  this.logger.error(this.br);
                  dataSub.setErr(-1);
                  continue;
                }
              }
              if ((!isAdd) && ((dataSub.getSubType() == 1) || (Commons.listHybridProductCode.contains(dataSub.getProductCode()))))
              {
                this.logger.info("Reset balance extra: MSISDN=" + dataSub.getMsisdn());
                List<PacketExtra> listExtra = dataPacketCurrent.getListExtraForProduct(dataSub.getProductCode());
                for (PacketExtra packetExtra : listExtra)
                {
                  String err = this.services.changeBalance(dataSub.getMsisdn(), packetExtra.getBalanceId(), isAdd, "0", null);
                  
                  ChargeLog chargeLogExtra = new ChargeLog(dataSub.getMsisdn(), dataSub.getSubId(), dataPacketCurrent.getName(), 11, packetExtra.getBalanceId(), packetExtra.getBalance(), err);
                  
                  this.db.insertChargLog(chargeLogExtra);
                }
              }
              if (dataNew != null)
              {
                DataSubscriber newSub = new DataSubscriber();
                newSub.setMsisdn(dataSub.getMsisdn());
                newSub.setSubId(dataSub.getSubId());
                newSub.setProductCode(dataSub.getProductCode());
                newSub.setDataName(dataNew.getName());
                newSub.setSubType(dataSub.getSubType());
                newSub.setExpireTime(expire);
                listRegister.add(newSub);
                
                CDR cdr = new CDR(dataNew.getVasCode(), dataSub.getChannel(), dataSub.getMsisdn(), dataSub.getSubId() + "", "", "0", "0", String.valueOf(0), "Register product " + dataNew.getName(), "0");
                

                cdr.setFileTypeID(dataSub.getSubType() == 1 ? CDR.PRE_3G : CDR.POS_3G);
                listCdr.add(cdr);
                if (cdr.getFileTypeID() == CDR.PRE_3G) {
                  listPreCdr.add(cdr);
                } else {
                  listPostCdr.add(cdr);
                }
              }
              dataSub.setActNote("2");
              listUnregister.add(dataSub);
              if ((dataPacketCurrent != null) && (dataPacketCurrent.getPcrfName() != null) && (!dataPacketCurrent.getPcrfName().isEmpty()))
              {
                String removePcrf = "";
                if (!removePcrf.equals("0")) {
                  this.logger.error("Remove PCRF error: MSISDN=" + dataSub.getMsisdn());
                }
              }
              CDR cdr = new CDR(dataPacketCurrent.getVasCode(), this.channelExtend, dataSub.getMsisdn(), dataSub.getSubId() + "", "", "1", "0", "", "Cancel product " + dataPacketCurrent.getName(), "0");
              

              cdr.setFileTypeID(dataSub.getSubType() == 1 ? CDR.PRE_3G : CDR.POS_3G);
              listCdr.add(cdr);
              if (cdr.getFileTypeID() == CDR.PRE_3G) {
                listPreCdr.add(cdr);
              } else {
                listPostCdr.add(cdr);
              }
              dataSub.setErr(1);
            }
          }
        }
        else
        {
          String subInfo = this.dbPost.getSubInfoMobile(dataSub.getMsisdn().substring(this.countryCode.length()));
          if (subInfo == null)
          {
            this.br.setLength(0);
            this.br.append(this.loggerLabel).append("Fail to get subscriber info on CM_POS: MSISDN=").append(dataSub.getMsisdn());
            

            this.logger.error(this.br);
            dataSub.setErr(-1);
          }
          else
          {
            if (subInfo.equals("NO_INFO_SUB"))
            {
              subInfo = this.dbPre.getSubInfoMobile(dataSub.getMsisdn().substring(this.countryCode.length()));
              if (subInfo == null)
              {
                this.br.setLength(0);
                this.br.append(this.loggerLabel).append("Fail to get subscriber info on CM_PRE: MSISDN=").append(dataSub.getMsisdn());
                

                this.logger.error(this.br);
                dataSub.setErr(-1);
                continue;
              }
              if (subInfo.equals("NO_INFO_SUB"))
              {
                this.br.setLength(0);
                this.br.append(this.loggerLabel).append("Subscriber is not mobile number: MSISDN=").append(dataSub.getMsisdn());
                

                this.logger.info(this.br);
                
                dataSub.setActNote("3");
                listUnregister.add(dataSub);
                


                dataSub.setErr(1);
                continue;
              }
            }
            this.logger.info("SUB_INFO:\n" + subInfo);
            Subscriber subscriber = new Subscriber(dataSub.getMsisdn(), subInfo);
            language = subscriber.getLanguage();
            

            boolean isUse = this.dbProduct.checkVasInProduct(subscriber.getProductCode(), dataPacketCurrent.getVasCode(), subscriber.getServiceType());
            if (!isUse)
            {
              this.br.setLength(0);
              this.br.append("Not allow register on product ").append(dataPacketCurrent.getName()).append(": MSISDN=").append(dataSub.getMsisdn()).append(" => MAIN_PRODUCT=").append(subscriber.getProductCode()).append("; RELATION_PRODUCT=").append(dataPacketCurrent.getVasCode());
              


              this.logger.info(this.br);
              
              dataSub.setActNote("3");
              listUnregister.add(dataSub);
              



              List<String> listVas = Arrays.asList(subscriber.getVasList());
              if (listVas.contains(dataPacketCurrent.getVasCode()))
              {
                CDR cdr = new CDR(dataPacketCurrent.getVasCode(), this.channelExtend, dataSub.getMsisdn(), dataSub.getSubId() + "", "", "1", "0", "", "Cancel product " + dataPacketCurrent.getName(), "0");
                

                cdr.setFileTypeID(dataSub.getSubType() == 1 ? CDR.PRE_3G : CDR.POS_3G);
                listCdr.add(cdr);
                if (cdr.getFileTypeID() == CDR.PRE_3G) {
                  listPreCdr.add(cdr);
                } else {
                  listPostCdr.add(cdr);
                }
              }
              dataSub.setErr(1);
            }
            else
            {
              if ((dataPacketCurrent.getRefuseVas() != null) && (!dataPacketCurrent.getRefuseVas().isEmpty()))
              {
                String vasConflict = null;
                for (String vasCode : subscriber.getVasList()) {
                  if (dataPacketCurrent.getRefuseVas().contains(vasCode))
                  {
                    vasConflict = vasCode;
                    break;
                  }
                }
                if (vasConflict != null)
                {
                  this.br.setLength(0);
                  this.br.append("Has vascode conflict ").append(vasConflict).append(" DATA=").append(dataPacketCurrent.getName()).append(": MSISDN=").append(dataSub.getMsisdn());
                  


                  this.logger.info(this.br);
                  
                  dataSub.setErr(1);
                  dataSub.setActNote("3");
                  listUnregister.add(dataSub);
                  



                  CDR cdr = new CDR(dataPacketCurrent.getVasCode(), this.channelExtend, dataSub.getMsisdn(), dataSub.getSubId() + "", "", "1", "0", "", "Cancel product " + dataPacketCurrent.getName(), "0");
                  

                  cdr.setFileTypeID(dataSub.getSubType() == 1 ? CDR.PRE_3G : CDR.POS_3G);
                  listCdr.add(cdr);
                  if (cdr.getFileTypeID() == CDR.PRE_3G) {
                    listPreCdr.add(cdr);
                  } else {
                    listPostCdr.add(cdr);
                  }
                }
              }
              String currentService = Commons.parseCurrentVas(subscriber.getVasList(), "-");
              boolean isAdd = true;
              

              double fee = dataPacketCurrent.getFeeExtend();
              if ((dataPacketCurrent.getCheckBal() > 0.0D) && ((dataSub.getSubType() == 1) || (Commons.listHybridProductCode.contains(dataSub.getProductCode()))))
              {
                double balance = this.services.checkMoney(dataSub.getMsisdn(), "1");
                if (balance == -9999999.0D)
                {
                  this.br.setLength(0);
                  this.br.append("Error get balance on provisioning").append(": MSISDN=").append(dataSub.getMsisdn());
                  

                  this.logger.error(this.br);
                  dataSub.setErr(-1);
                  continue;
                }
                if (balance < dataPacketCurrent.getCheckBal())
                {
                  this.br.setLength(0);
                  this.br.append("Balance not enough: MSISDN=").append(dataSub.getMsisdn());
                  
                  this.logger.info(this.br);
                  

                  DataAction dataAction = Commons.getInstance("PROCESS").getDataAction(dataPacketCurrent.getName(), "", dataSub.getProductCode(), "3");
                  if (dataAction != null) {
                    isAdd = !dataAction.isResetExtra();
                  }
                  if ((dataAction != null) && (dataAction.getFollowData() != null) && (dataAction.getFollowData().trim().length() > 0))
                  {
                    DataPacket dataNew = Commons.getInstance("PROCESS").getDataPacketByName(dataAction.getFollowData(), dataPacketCurrent.getType());
                    

                    Object dataService = getPricePlan(dataSub, dataPacketCurrent, dataNew, currentService);
                    if ((dataSub.getSubType() == 1) || (Commons.listHybridProductCode.contains(dataSub.getProductCode())))
                    {
                      DataServicePre dataServicePre = (DataServicePre)dataService;
                      Date expire = dataServicePre.getExpire();
                      String error = this.services.changeDataPre(dataServicePre, true);
                      if (!error.equals("0"))
                      {
                        this.br.setLength(0);
                        this.br.append("Change to packet ").append(dataNew.getName()).append(" fail: MSISDN=").append(dataSub.getMsisdn());
                        
                        this.logger.info(this.br);
                        dataSub.setErr(-1);
                        continue;
                      }
                      DataSubscriber newSub = new DataSubscriber();
                      newSub.setMsisdn(dataSub.getMsisdn());
                      newSub.setSubId(dataSub.getSubId());
                      newSub.setProductCode(dataSub.getProductCode());
                      newSub.setDataName(dataNew.getName());
                      newSub.setSubType(dataSub.getSubType());
                      newSub.setExpireTime(expire);
                      listRegister.add(newSub);
                      
                      CDR cdr = new CDR(dataNew.getVasCode(), dataSub.getChannel(), dataSub.getMsisdn(), dataSub.getSubId() + "", "", "0", "0", "0", "Register product " + dataNew.getName(), "0");
                      


                      cdr.setFileTypeID(dataSub.getSubType() == 1 ? CDR.PRE_3G : CDR.POS_3G);
                      listCdr.add(cdr);
                      if (cdr.getFileTypeID() == CDR.PRE_3G) {
                        listPreCdr.add(cdr);
                      } else {
                        listPostCdr.add(cdr);
                      }
                    }
                  }
                  if ((!isAdd) && ((dataSub.getSubType() == 1) || (Commons.listHybridProductCode.contains(dataSub.getProductCode()))))
                  {
                    this.logger.info("Reset balance extra: MSISDN=" + dataSub.getMsisdn());
                    List<PacketExtra> listExtra = dataPacketCurrent.getListExtraForProduct(dataSub.getProductCode());
                    for (PacketExtra packetExtra : listExtra)
                    {
                        int enable_reset = 0;
                        try {
                            enable_reset = Integer.parseInt(Commons.getInstance("PROCESS").getConfig("enable_reset_" + dataSub.getDataName().toLowerCase().trim(), this.logger).trim());
                        } catch (Exception e) {

                        } 
                        
                       String err = "";
                       if ( (enable_reset == 1) ){
                            err = this.services.changeBalance(dataSub.getMsisdn(), packetExtra.getBalanceId(), true, packetExtra.getBalance(), null);
                         }else{
                            err = this.services.changeBalance(dataSub.getMsisdn(), packetExtra.getBalanceId(), false, packetExtra.getBalance(), null);
                         }  
                     
                      ChargeLog chargeLogExtra = new ChargeLog(dataSub.getMsisdn(), dataSub.getSubId(), dataPacketCurrent.getName(), 11, packetExtra.getBalanceId(), packetExtra.getBalance(), err);
                      
                      this.db.insertChargLog(chargeLogExtra);
                    }
                  }
                  dataSub.setActNote("6");
                  listUnregister.add(dataSub);
                  



                  CDR cdr = new CDR(dataPacketCurrent.getVasCode(), this.channelExtend, dataSub.getMsisdn(), dataSub.getSubId() + "", "", "1", "0", "", "Cancel product " + dataPacketCurrent.getName(), "0");
                  

                  cdr.setFileTypeID(dataSub.getSubType() == 1 ? CDR.PRE_3G : CDR.POS_3G);
                  listCdr.add(cdr);
                  if (cdr.getFileTypeID() == CDR.PRE_3G) {
                    listPreCdr.add(cdr);
                  } else {
                    listPostCdr.add(cdr);
                  }
                  dataSub.setMessage(Commons.getInstance("AUTO_EXTEND").getConfig("msg_extend_not_enough_" + dataPacketCurrent.getName().toLowerCase() + "_" + language, this.logger));
                  
                  dataSub.setErr(1);
                  continue;
                }
              }
              Object dataService = getPricePlan(dataSub, dataPacketCurrent, dataPacketCurrent, currentService);
              if (dataService == null)
              {
                this.br.setLength(0);
                this.br.append("Error get priceplane on product: MSISDN=").append(dataSub.getMsisdn());
                
                this.logger.info(this.br);
                dataSub.setErr(-1);
              }
              else if ((dataSub.getSubType() == 1) || (Commons.listHybridProductCode.contains(dataSub.getProductCode())))
              {
                if (fee > 0.0D)
                {
                  String errCharge = this.services.chargeMoney(dataSub.getMsisdn(), fee);
                  ChargeLog chargeLog = new ChargeLog(dataSub.getMsisdn(), dataSub.getSubId(), dataPacketCurrent.getName(), 11, "1", "-" + fee, errCharge);
                  
                  this.db.insertChargLog(chargeLog);
                  if (errCharge.equals(Commons.getInstance("PROVISIONING").getConfig("error_not_enought", this.logger)))
                  {
                    this.br.setLength(0);
                    this.br.append("Balance not enough: MSISDN=").append(dataSub.getMsisdn());
                    
                    this.logger.info(this.br);
                    
                    DataAction dataAction = Commons.getInstance("PROCESS").getDataAction(dataPacketCurrent.getName(), "", dataSub.getProductCode(), "3");
                    if (dataAction != null) {
                      isAdd = !dataAction.isResetExtra();
                    }
                    if ((dataAction != null) && (dataAction.getFollowData() != null) && (dataAction.getFollowData().trim().length() > 0))
                    {
                      DataPacket dataPacketNew = Commons.getInstance("PROCESS").getDataPacketByName(dataAction.getFollowData(), dataPacketCurrent.getType());
                      

                      Object dataServiceOff = getPricePlan(dataSub, dataPacketCurrent, dataPacketNew, currentService);
                      if ((dataSub.getSubType() == 1) || (Commons.listHybridProductCode.contains(dataSub.getProductCode())))
                      {
                        DataServicePre dataServicePre = (DataServicePre)dataServiceOff;
                        Date expire = dataServicePre.getExpire();
                        String error = this.services.changeDataPre(dataServicePre, true);
                        if (!error.equals("0"))
                        {
                          this.br.setLength(0);
                          this.br.append("Change to packet ").append(dataPacketNew.getName()).append(" fail: MSISDN=").append(dataSub.getMsisdn());
                          
                          this.logger.info(this.br);
                          dataSub.setErr(-1);
                          continue;
                        }
                        DataSubscriber newSub = new DataSubscriber();
                        newSub.setMsisdn(dataSub.getMsisdn());
                        newSub.setSubId(dataSub.getSubId());
                        newSub.setProductCode(dataSub.getProductCode());
                        newSub.setDataName(dataPacketNew.getName());
                        newSub.setSubType(dataSub.getSubType());
                        newSub.setExpireTime(expire);
                        listRegister.add(newSub);
                        
                        CDR cdr = new CDR(dataPacketNew.getVasCode(), dataSub.getChannel(), dataSub.getMsisdn(), dataSub.getSubId() + "", "", "0", "0", "0", "Register product " + dataPacketNew.getName(), "0");
                        


                        cdr.setFileTypeID(dataSub.getSubType() == 1 ? CDR.PRE_3G : CDR.POS_3G);
                        listCdr.add(cdr);
                        if (cdr.getFileTypeID() == CDR.PRE_3G) {
                          listPreCdr.add(cdr);
                        } else {
                          listPostCdr.add(cdr);
                        }
                      }
                    }
                    if (!isAdd)
                    {
                      this.logger.info("Reset balance extra: MSISDN=" + dataSub.getMsisdn());
                      List<PacketExtra> listExtra = dataPacketCurrent.getListExtraForProduct(dataSub.getProductCode());
                      for (PacketExtra packetExtra : listExtra)
                      {
                            int enable_reset = 0;
                            try {
                                enable_reset = Integer.parseInt(Commons.getInstance("PROCESS").getConfig("enable_reset_" + dataSub.getDataName().toLowerCase().trim(), this.logger).trim());
                            } catch (Exception e) {

                            }
                            String err = "";
                            
                            if ( (enable_reset == 1)  ){
                                 err = this.services.changeBalance(dataSub.getMsisdn(), packetExtra.getBalanceId(), true, packetExtra.getBalance(), null);
                              }else{
                                 err = this.services.changeBalance(dataSub.getMsisdn(), packetExtra.getBalanceId(), false, packetExtra.getBalance(), null);
                              }   

                        
                        ChargeLog chargeLogExtra = new ChargeLog(dataSub.getMsisdn(), dataSub.getSubId(), dataPacketCurrent.getName(), 11, packetExtra.getBalanceId(), packetExtra.getBalance(), err);
                        
                        this.db.insertChargLog(chargeLogExtra);
                      }
                    }
                    dataSub.setActNote("6");
                    listUnregister.add(dataSub);
                    



                    List<String> listVas = Arrays.asList(subscriber.getVasList());
                    if (listVas.contains(dataPacketCurrent.getVasCode()))
                    {
                      CDR cdr = new CDR(dataPacketCurrent.getVasCode(), this.channelExtend, dataSub.getMsisdn(), dataSub.getSubId() + "", "", "1", "0", "", "Cancel product " + dataPacketCurrent.getName(), "0");
                      

                      cdr.setFileTypeID(dataSub.getSubType() == 1 ? CDR.PRE_3G : CDR.POS_3G);
                      listCdr.add(cdr);
                      if (cdr.getFileTypeID() == CDR.PRE_3G) {
                        listPreCdr.add(cdr);
                      } else {
                        listPostCdr.add(cdr);
                      }
                    }
                    dataSub.setMessage(Commons.getInstance("AUTO_EXTEND").getConfig("msg_extend_not_enough_" + dataPacketCurrent.getName().toLowerCase() + "_" + language, this.logger));
                    
                    dataSub.setErr(1);
                    continue;
                  }
                  if (!errCharge.equals("0"))
                  {
                    this.br.setLength(0);
                    this.br.append("Charge extend error: MSISDN=").append(dataSub.getMsisdn());
                    
                    this.logger.info(this.br);
                    dataSub.setErr(-1);
                    continue;
                  }
                }
                DataServicePre dataServicePre = (DataServicePre)dataService;
                Date expire = dataServicePre.getExpire();
                
                String error = this.services.changeDataPre(dataServicePre, true);
                if (!error.equals("0"))
                {
                  this.br.setLength(0);
                  this.br.append("Extend MI fail: MSISDN=").append(dataSub.getMsisdn());
                  
                  this.logger.error(this.br);
                  if (fee > 0.0D)
                  {
                    this.br.setLength(0);
                    this.br.append("Rollback add money: MSISDN=").append(dataSub.getMsisdn()).append("MONEY=").append(fee);
                    
                    this.logger.info(this.br);
                    String errorAdd = this.services.changeBalance(dataSub.getMsisdn(), "1", true, Services.doubleToString(fee), null);
                    
                    ChargeLog chargeLog = new ChargeLog(dataSub.getMsisdn(), dataSub.getSubId(), dataPacketCurrent.getName(), 11, "1", "" + fee, errorAdd);
                    

                    this.db.insertChargLog(chargeLog);
                    if (!errorAdd.equals("0"))
                    {
                      this.br.setLength(0);
                      this.br.append("Rollback add money fail: MSISDN=").append(dataSub.getMsisdn());
                      
                      this.logger.error(this.br);
                    }
                  }
                  dataSub.setErr(-1);
                }
                else
                {
                  this.logger.info("Extend Data success: MSISDN=" + dataSub.getMsisdn());
                  






                  DataAction dataAction = Commons.getInstance("PROCESS").getDataAction(dataPacketCurrent.getName(), dataPacketCurrent.getName(), dataSub.getProductCode(), "4");
                  if (dataAction != null) {
                    isAdd = !dataAction.isResetExtra();
                  }
                  this.logger.info("Add balance extra: MSISDN=" + dataSub.getMsisdn());
                  List<PacketExtra> listExtra = dataPacketCurrent.getListExtraForProduct(dataSub.getProductCode());
                  for (PacketExtra packetExtra : listExtra)
                  {
                        int enable_reset = 0;
                        try {
                            enable_reset = Integer.parseInt(Commons.getInstance("PROCESS").getConfig("enable_reset_" + dataSub.getDataName().toLowerCase().trim(), this.logger).trim());
                        } catch (Exception e) {

                        }
                      
                      
                      String err = "";
                       if ( (enable_reset == 1) ){
                            err = this.services.changeBalance(dataSub.getMsisdn(), packetExtra.getBalanceId(), true, packetExtra.getBalance(), Services.sdf.format(packetExtra.getExpireTime(this.logger)));
                         }else{
                            err = this.services.changeBalance(dataSub.getMsisdn(), packetExtra.getBalanceId(), false, packetExtra.getBalance(), Services.sdf.format(packetExtra.getExpireTime(this.logger)));
                         }  
                  
                    ChargeLog chargeLogExtra = new ChargeLog(dataSub.getMsisdn(), dataSub.getSubId(), dataPacketCurrent.getName(), 11, packetExtra.getBalanceId(), packetExtra.getBalance(), err);
                    
                    this.db.insertChargLog(chargeLogExtra);
                  }
                  dataSub.setErr(0);
                  dataSub.setExpireTime(expire);
                  dataSub.setRestrictData(dataPacketCurrent.getRestrictData());
                  dataSub.setPaidTime(new Date());
                  




                  dataSub.setStatus(1);
                  if ((dataPacketCurrent.getPcrfName() != null) && (!dataPacketCurrent.getPcrfName().isEmpty()) && (dataPacketCurrent.getRestrictData() > 0.0D))
                  {
                    String addPcrf = "";
                    

                    addPcrf = this.services.pcrfAddSubOCS(dataSub.getMsisdn(), dataPacketCurrent.getPcrfBalanceId(), Long.toString(Math.round(dataPacketCurrent.getRestrictData())), dataServicePre.getAddPpOcsList(), dataServicePre.getExpirePro());
                    if (!addPcrf.equals("0")) {
                      this.logger.error("Add PCRF error: MSISDN=" + dataSub.getMsisdn());
                    }
                  }
                  dataSub.setMessage(Commons.getInstance("AUTO_EXTEND").getConfig("msg_extend_success_" + dataPacketCurrent.getName().toLowerCase() + "_" + language, this.logger));
                }
              }
              else
              {
                DataServicePos dataServicePos = (DataServicePos)dataService;
                String error = this.services.changeDataPos(dataServicePos, true);
                Date expire = dataServicePos.getExpire();
                if (error.equals("0"))
                {
                  if (fee > 0.0D)
                  {
                    String errCharge = this.services.chargeMoneyPostpaid(dataSub.getMsisdn(), fee, dataServicePos.getAddPpOcsPostList(), "Charge money for " + dataPacketCurrent.getVasCode());
                    

                    ChargeLog chargeLog = null;
                    if (!errCharge.equals("0"))
                    {
                      this.br.setLength(0);
                      this.br.append("Fail to charge money postpaid: MSISDN=").append(dataSub.getMsisdn());
                      
                      this.logger.error(this.br);
                      
                      this.services.changeDataPos(dataServicePos.cloneRollback(), true);
                    }
                    chargeLog = new ChargeLog(dataSub.getMsisdn(), dataSub.getSubId(), dataPacketCurrent.getName(), 11, "1", "-" + fee, errCharge);
                    
                    this.db.insertChargLog(chargeLog);
                  }
                  this.logger.info("Extend Data pos success: MSISDN=" + dataSub.getMsisdn());
                  
                  dataSub.setErr(0);
                  dataSub.setExpireTime(expire);
                  dataSub.setRestrictData(dataPacketCurrent.getRestrictData());
                  dataSub.setPaidTime(new Date());
                  




                  dataSub.setStatus(1);
                  

                  dataSub.setMessage(Commons.getInstance("AUTO_EXTEND").getConfig("msg_extend_success_" + dataPacketCurrent.getName().toLowerCase() + "_" + language, this.logger));
                  if (dataSub.getSubType() == 0)
                  {
                    PostPaid postPaid = new PostPaid();
                    postPaid.setActionType(1);
                    postPaid.setFee(fee);
                    postPaid.setIsdn(dataSub.getMsisdn());
                    postPaid.setVasCode(dataSub.getDataName());
                    postPaid.setContractId(subscriber.getContractId());
                    postPaid.setSubId(String.valueOf(dataSub.getSubId()));
                    postPaid.setSubType(0);
                    listPostPaid.clear();
                    listPostPaid.add(postPaid);
                    this.db.insertPostPaid(listPostPaid);
                  }
                }
                else
                {
                  this.br.setLength(0);
                  this.br.append("Extend fail: MSISDN=").append(dataSub.getMsisdn());
                  
                  this.logger.error(this.br);
                  dataSub.setErr(-1);
                }
              }
            }
          }
        }
      }
    }
    if (!listUnregister.isEmpty()) {
      this.db.revokeDataSubcriber(listUnregister);
    }
    if (!listRegister.isEmpty()) {
      this.db.insertDataSubcriber(listRegister);
    }
    if ((!listCdr.isEmpty()) && (!Commons.insertVasRegister)) {
      this.db.insertCDR(listCdr);
    }
    if ((!listPreCdr.isEmpty()) && (Commons.insertVasRegister)) {
      this.dbPre.insertVasRegister(listCdr);
    }
    if ((!listPostCdr.isEmpty()) && (Commons.insertVasRegister)) {
      this.dbPost.insertVasRegister(listCdr);
    }
    return list;
  }
  
  private Object getPricePlan(DataSubscriber dataSubscriber, DataPacket dataOld, DataPacket dataNew, String currentVas)
  {
    try
    {
      String pricePlanNew = "";
      String pricePlanOld = "";
      String hlrTplNew = "";
      String hlrTplOld = "";
      Date expire = dataNew != null ? dataNew.getExpireTime(this.logger) : null;
      this.logger.info("Expire=" + expire);
      

      List<CmPricePlan> listPricePlanNew = new ArrayList();
      if (dataNew != null)
      {
        listPricePlanNew = this.dbProduct.getListPricePlanByVasId(dataSubscriber.getProductCode(), dataNew.getVasCode(), dataSubscriber.getSubType());
        if ((listPricePlanNew == null) || (listPricePlanNew.size() < 1))
        {
          this.br.setLength(0);
          this.br.append("Fail to get new product priceplan: MSISDN=").append(dataSubscriber.getMsisdn());
          
          this.logger.info(this.br);
          return null;
        }
      }
      List<CmPricePlan> listPricePlanOld = new ArrayList();
      if (dataOld != null)
      {
        listPricePlanOld = this.dbProduct.getListPricePlanByVasId(dataSubscriber.getProductCode(), dataOld.getVasCode(), dataSubscriber.getSubType());
        if ((listPricePlanOld == null) || (listPricePlanOld.size() < 1))
        {
          this.br.setLength(0);
          this.br.append("Fail to get old product priceplan: MSISDN=").append(dataSubscriber.getMsisdn());
          
          this.logger.info(this.br);
          return null;
        }
      }
      if ((dataSubscriber.getSubType() == 1) || (Commons.listHybridProductCode.contains(dataSubscriber.getProductCode())))
      {
        for (CmPricePlan pricePlan : listPricePlanOld) {
          if (Commons.getInstance("PROVISIONING").getConfig("priceplan_pre_exchange_id", this.logger).equals(pricePlan.getExchangeId()))
          {
            pricePlanOld = String.valueOf(pricePlan.getPricePlanCode());
            this.logger.info("PricePlanOld= " + pricePlanOld);
          }
          else if (Commons.getInstance("PROVISIONING").getConfig("hlrbroadband_exchange_id", this.logger).equals(pricePlan.getExchangeId()))
          {
            hlrTplOld = String.valueOf(pricePlan.getPricePlanCode());
            if (dataSubscriber.getStatus() == 2) {
              hlrTplOld = dataOld.getTemplateRestrict();
            }
            this.logger.info("HlrTplOld= " + hlrTplOld);
          }
        }
        for (CmPricePlan pricePlan : listPricePlanNew) {
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
        DataServicePre dataServicePre = new DataServicePre(dataSubscriber.getMsisdn(), pricePlanNew, pricePlanOld, currentVas, hlrTplOld, hlrTplNew);
        
        dataServicePre.setExpire(expire);
        if (dataSubscriber.getExpireTime() != null) {
          dataServicePre.setExpreOld(dataSubscriber.getExpireTime());
        }
        return dataServicePre;
      }
      for (CmPricePlan pricePlan : listPricePlanOld) {
        if (Commons.getInstance("PROVISIONING").getConfig("priceplan_pos_exchange_id", this.logger).equals(pricePlan.getExchangeId()))
        {
          pricePlanOld = String.valueOf(pricePlan.getPricePlanCode());
          this.logger.info("PricePlanOld= " + pricePlanOld);
        }
        else if (Commons.getInstance("PROVISIONING").getConfig("hlrbroadband_exchange_id", this.logger).equals(pricePlan.getExchangeId()))
        {
          hlrTplOld = String.valueOf(pricePlan.getPricePlanCode());
          if (dataSubscriber.getStatus() == 2) {
            hlrTplOld = dataOld.getTemplateRestrict();
          }
          this.logger.info("HlrtplOld= " + hlrTplOld);
        }
      }
      for (CmPricePlan pricePlan : listPricePlanNew) {
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
      DataServicePos dataServicePos = new DataServicePos(dataSubscriber.getMsisdn(), pricePlanNew, pricePlanOld, currentVas, formatPro.format(new Date()), hlrTplOld, hlrTplNew);
      

      dataServicePos.setExpire(expire);
      if (dataSubscriber.getExpireTime() != null)
      {
        dataServicePos.setExpreOld(dataSubscriber.getExpireTime());
        

        dataServicePos.setEffect(dataSubscriber.getExpireTime());
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
    this.br.append("Process list record").append(String.format("|%1$-11s|%2$-15s|%3$-10s|%4$-5s|%5$-15s", new Object[] { "ID", "MSISDN", "DATA_NAME", "AUTO_EXTEND", "EXPIRE" })).append("\n");
    for (Record record : list)
    {
      DataSubscriber dataSub = (DataSubscriber)record;
      this.br.append(String.format("|%1$-11s|%2$-15s|%3$-10s|%4$-5s|%5$-15s", new Object[] { Long.valueOf(dataSub.getID()), dataSub.getMsisdn(), dataSub.getDataName(), Integer.valueOf(dataSub.getAutoExtend()), dataSub.getExpireTime() }));
    }
    this.logger.info(this.br);
  }
  
  public List<Record> processException(List<Record> list)
  {
    return list;
  }
}

