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
import com.viettel.vas.data.obj.Discount;
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
import java.util.HashMap;
import java.util.List;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.log4j.Logger;
import utils.Config;

public class Register
  extends ProcessRecordAbstract
{
  private String loggerLabel = Register.class.getSimpleName() + ": ";
  private DbProcessor db;
  private DbProduct dbProduct;
  private DbPost dbPost;
  private DbPre dbPre;
  private Services services;
  private static SimpleDateFormat formatMsg = new SimpleDateFormat("dd/MM/yyyy");
  private static SimpleDateFormat formatPro = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  private StringBuilder br = new StringBuilder();
  private String countryCode;
  
  public Register()
  {
    this.logger = Logger.getLogger(Register.class);
  }
  
  public Register(Logger logger, String dbAppName, String dbPosName, String dbPreName, String dbProductName)
  {
    try
    {
      ConnectionPoolManager.loadConfig("../etc/database.xml");
      this.db = new DbProcessor(dbAppName, logger);
      this.dbPost = new DbPost(dbPosName, logger);
      this.dbPre = new DbPre(dbPreName, logger);
      this.dbProduct = new DbProduct(dbProductName, logger);
      this.services = new Services(ExchangeClientChannel.getInstance(Config.configDir + File.separator + "service_client.cfg").getInstanceChannel(), this.db, logger);
      

      Commons.createInstance("PROCESS", this.db, logger);
      Commons.createInstance("PROVISIONING", this.db, logger);
      
      this.countryCode = ResourceBundle.getBundle("vas").getString("country_code");
      Commons.defaultLang = ResourceBundle.getBundle("vas").getString("default_lang");
    }
    catch (Exception ex)
    {
      logger.error("Init Register ERROR", ex);
    }
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
    Commons.defaultLang = ResourceBundle.getBundle("vas").getString("default_lang");
    
   
  }
  
  public boolean startProcessRecord()
  {
    return true;
  }
  
  public List<Record> validateContraint(List<Record> list)
    throws Exception
  {
    List<DataSubscriber> listDataSubscribers = new ArrayList();
    List<CDR> listPostCdr = new ArrayList();
    List<CDR> listPreCdr = new ArrayList();
    List<CDR> listCdr = new ArrayList();
    for (Record record : list)
    {
      String language = Commons.defaultLang;
      MoRecord moRecord = (MoRecord)record;
      String content = moRecord.getCommand().toUpperCase() + (moRecord.getParam() == null ? "" : new StringBuilder().append(" ").append(moRecord.getParam().toUpperCase().trim()).toString());
      
      content = content.replace("O", "0");
      HashMap map = new HashMap();
      moRecord.setHashMap(map);
      moRecord.getHashMap().put("CONTENT", content.trim());
      

      String subInfo = this.dbPre.getSubInfoMobile(moRecord.getMsisdn().substring(this.countryCode.length()));
      if (subInfo == null)
      {
        this.br.setLength(0);
        this.br.append(this.loggerLabel).append("Fail to get subscriber info on CM_PRE: MSISDN=").append(moRecord.getMsisdn());
        

        this.logger.error(this.br);
        moRecord.setErrCode("2");
        moRecord.setErrOcs("Fail to get subscriber info on CM_PRE");
        moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("system_fail_" + language, this.logger));
      }
      else
      {
        if (subInfo.equals("NO_INFO_SUB"))
        {
          subInfo = this.dbPost.getSubInfoMobile(moRecord.getMsisdn().substring(this.countryCode.length()));
          if (subInfo == null)
          {
            this.br.setLength(0);
            this.br.append(this.loggerLabel).append("Fail to get subscriber info on CM_POS: MSISDN=").append(moRecord.getMsisdn());
            

            this.logger.error(this.br);
            moRecord.setErrCode("2");
            moRecord.setErrOcs("Fail to get subscriber info on CM_POS");
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
          moRecord.setSubType(Integer.valueOf(0));
        }
        else
        {
          moRecord.setSubType(Integer.valueOf(1));
        }
        this.logger.info("SUB_INFO:\n" + subInfo);
        Subscriber subscriber = new Subscriber(moRecord.getMsisdn(), subInfo);
        language = subscriber.getLanguage();
        moRecord.getHashMap().put("LANGUAGE", language);
        moRecord.setSubId(Long.valueOf(Long.parseLong(subscriber.getSubId())));
        moRecord.setProductCode(subscriber.getProductCode());
        moRecord.getHashMap().put("SUB_INFO", subscriber);
        
        moRecord.getHashMap().put("HYBRID", "0");
        if (Commons.listHybridProductCode.contains("," + subscriber.getProductCode().toUpperCase() + ",")) {
          moRecord.getHashMap().put("HYBRID", "1");
        }
        String currentService = Commons.parseCurrentVas(subscriber.getVasList(), "-");
        moRecord.getHashMap().put("CURRENT_SERVICE", currentService);
        if (Commons.getInstance("PROCESS").statusBlocked.contains(subscriber.getActStatus()))
        {
          this.br.setLength(0);
          this.br.append("Subscriber is blocked: MSISDN=").append(moRecord.getMsisdn());
          
          this.logger.info(this.br);
          moRecord.setErrCode("10");
          moRecord.setErrOcs("Subscriber is blocked");
          moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("msg_mobile_block_" + language, this.logger));
        }
        else if (Commons.getInstance("PROCESS").statusNotActive.contains(subscriber.getActStatus()))
        {
          this.br.setLength(0);
          this.br.append("Subscriber not active: MSISDN=").append(moRecord.getMsisdn());
          
          this.logger.info(this.br);
          moRecord.setErrCode("10");
          moRecord.setErrOcs("Subscriber not active");
          moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("msg_mobile_not_active_" + language, this.logger));
        }
        else
        {
          DataPacket dataPacket = Commons.getInstance("PROCESS").getDataPacket(content, moRecord.getSubType().intValue());
          if (dataPacket == null)
          {
            this.br.setLength(0);
            this.br.append(this.loggerLabel).append("Packet not found or packet not active: MSISDN=").append(moRecord.getMsisdn());
            

            this.logger.info(this.br);
            moRecord.setErrCode("15");
            moRecord.setErrOcs("Packet not found or packet not active");
            moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("msg_packet_not_active_" + language, this.logger));
          }
          else
          {
            this.logger.info("DATA_PACKET:\n" + dataPacket);
            if ((dataPacket.getListAllow() != null) && (!dataPacket.getListAllow().isEmpty()))
            {
              boolean inListAllow = this.db.checkInListAllow(dataPacket.getListAllow(), moRecord.getMsisdn());
              if (!inListAllow)
              {
                this.br.setLength(0);
                this.br.append(this.loggerLabel).append("Not in list allow: MSISDN=").append(moRecord.getMsisdn());
                

                this.logger.info(this.br);
                moRecord.setErrCode("18");
                moRecord.setErrOcs("Not in list allow");
                moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("msg_not_in_list_allow_" + language, this.logger));
                
                continue;
              }
            }
            boolean isUse = this.dbProduct.checkVasInProduct(subscriber.getProductCode(), dataPacket.getVasCode(), moRecord.getSubType().intValue());
            if (!isUse)
            {
              this.br.setLength(0);
              this.br.append("Not allow register on product ").append(dataPacket.getName()).append(": MSISDN=").append(moRecord.getMsisdn()).append(" => MAIN_PRODUCT=").append(subscriber.getProductCode()).append("; RELATION_PRODUCT=").append(dataPacket.getVasCode());
              


              this.logger.info(this.br);
              moRecord.setErrCode("10");
              moRecord.setErrOcs("Not allow register in product " + dataPacket.getVasCode());
              String message = Commons.getInstance("PROCESS").getConfig("msg_product_not_support_" + language, this.logger).replace("%packet%", dataPacket.getSyntax());
              

              moRecord.setMessage(message);
            }
            else
            {
              if ((dataPacket.getRefuseVas() != null) && (!dataPacket.getRefuseVas().isEmpty()))
              {
                String vasConflict = null;
                for (String vasCode : subscriber.getVasList()) {
                  if (dataPacket.getRefuseVas().contains(vasCode))
                  {
                    vasConflict = vasCode;
                    break;
                  }
                }
                if (vasConflict != null)
                {
                  this.br.setLength(0);
                  this.br.append("Has vascode conflict ").append(vasConflict).append(" DATA=").append(dataPacket.getName()).append(": MSISDN=").append(moRecord.getMsisdn());
                  


                  this.logger.info(this.br);
                  

                  moRecord.setErrCode("10");
                  moRecord.setErrOcs("Has vascode conflict " + vasConflict);
                  String message = Commons.getInstance("PROCESS").getConfig("msg_product_conflict_" + language, this.logger).replace("%packet%", dataPacket.getSyntax());
                  

                  DataPacket conflictData = Commons.getInstance("PROCESS").getDataPacketByVasCode(vasConflict, moRecord.getSubType().intValue());
                  if (conflictData != null) {
                    message = message.replaceAll("%conflict_vas%", conflictData.getName());
                  } else {
                    message = message.replaceAll("%conflict_vas%", vasConflict);
                  }
                  moRecord.setMessage(message);
                  continue;
                }
              }
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
                boolean existBccs = false;
                if (subscriber.getVasList().length > 0)
                {
                  Set<String> listVasCode = Commons.getInstance("PROCESS").getAllVasCodeOnDataPacket();
                  List<String> dataOnBccs = Commons.checkDuplicateVasCode(subscriber.getVasList(), listVasCode, moRecord.getSubType().intValue());
                  for (String vas : dataOnBccs)
                  {
                    boolean exit = false;
                    DataPacket currentPacket = Commons.getInstance("PROCESS").getDataPacketByVasCode(vas, moRecord.getSubType().intValue());
                    if (currentPacket.getName().equals(dataPacket.getName())) {
                      existBccs = true;
                    }
                    for (DataSubscriber dataSubscriber : listSub) {
                      if (dataSubscriber.getDataName().equals(currentPacket.getName()))
                      {
                        exit = true;
                        break;
                      }
                    }
                    if (!exit)
                    {
                      this.logger.info("Add virtual data_subcriber => " + currentPacket.getName());
                      
                      DataSubscriber dataSub = new DataSubscriber();
                      dataSub.setDataName(currentPacket.getName());
                      dataSub.setMsisdn(moRecord.getMsisdn());
                      dataSub.setSubId(Long.parseLong(subscriber.getSubId()));
                      dataSub.setProductCode(subscriber.getProductCode());
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
                    this.logger.warn("Please check auto extent thread for this packet: " + dataSubscriber.getMsisdn() + "=>" + dataSubscriber.getDataName());
                  }
                  else
                  {
                    listSub.add(dataSubscriber);
                  }
                }
                listSubCheck.clear();
                




                boolean notHasBase = false;
                List<String> packetBase = new ArrayList();
                if ((dataPacket.getPacketBase() != null) && (!dataPacket.getPacketBase().isEmpty())) {
                  packetBase.addAll(dataPacket.getPacketBase());
                }
                if (dataPacket.getGroupBase() > 0) {
                  notHasBase = true;
                }
                List<String> listAutoAddPacket = null;
                if ((dataPacket.getAutoAddPacket() != null) && (!dataPacket.getAutoAddPacket().isEmpty()))
                {
                  listAutoAddPacket = new ArrayList();
                  listAutoAddPacket.addAll(dataPacket.getAutoAddPacket());
                  if (notHasBase) {
                    for (String dataName : listAutoAddPacket)
                    {
                      DataPacket dataAuto = Commons.getInstance("PROCESS").getDataPacketByName(dataName, moRecord.getSubType().intValue());
                      if (dataAuto.getGroupPacket() == dataPacket.getGroupBase())
                      {
                        notHasBase = false;
                        break;
                      }
                    }
                  }
                }
                if (!listSub.isEmpty())
                {
                  int action = 1;
                  boolean notAllowExtend = false;
                  boolean isExtend = false;
                  for (DataSubscriber dataSubscriber : listSub)
                  {
                    if ((dataSubscriber.getSubId() != moRecord.getSubId().longValue()) || (dataSubscriber.getSubType() != moRecord.getSubType().intValue()))
                    {
                      dataSubscriber.setActNote("3");
                      listDataSubscribers.add(dataSubscriber);
                    }
                    DataPacket dataOld = Commons.getInstance("PROCESS").getDataPacketByName(dataSubscriber.getDataName(), dataSubscriber.getSubType());
                    if (dataPacket.getGroupBase() == dataOld.getGroupPacket())
                    {
                      notHasBase = false;
                      if (listAutoAddPacket != null) {
                        listAutoAddPacket.clear();
                      }
                    }
                    if ((listAutoAddPacket != null) && (listAutoAddPacket.contains(dataOld.getName()))) {
                      listAutoAddPacket.remove(dataOld.getName());
                    }
                    if (dataSubscriber.getDataName().equals(dataPacket.getName()))
                    {
                      action = 3;
                      isExtend = true;
                      if (!existBccs)
                      {
                        this.br.setLength(0);
                        this.br.append("Not synchronize database, bccs => insert CDR syn").append(": MSISDN=").append(moRecord.getMsisdn()).append(" - PACKET:").append(dataPacket.getName());
                        

                        this.logger.info(this.br);
                        

                        CDR cdr = new CDR(dataPacket.getVasCode(), moRecord.getChannel(), moRecord.getMsisdn(), moRecord.getSubId() + "", "", "0", "0", String.valueOf(dataPacket.getFee()), "Register product " + dataPacket.getVasCode(), "0");
                        



                        cdr.setFileTypeID(subscriber.getServiceType() == 1 ? CDR.PRE_3G : CDR.POS_3G);
                        

                        listCdr.add(cdr);
                        if (cdr.getFileTypeID() == CDR.PRE_3G) {
                          listPreCdr.add(cdr);
                        } else {
                          listPostCdr.add(cdr);
                        }
                      }
                      if (!dataPacket.isAllowExtend())
                      {
                        notAllowExtend = true;
                      }
                      else
                      {
                        moRecord.getHashMap().put("IS_EXTEND", "1");
                        moRecord.getHashMap().put("DATA_SUBCRIBER", dataSubscriber);
                        if (listAutoAddPacket != null) {
                          listAutoAddPacket.clear();
                        }
                      }
                    }
                    else if (!isExtend)
                    {
                      if (dataOld.getGroupPacket() == dataPacket.getGroupPacket())
                      {
                        action = 2;
                        moRecord.getHashMap().put("IS_EXTEND", "0");
                        moRecord.getHashMap().put("DATA_SUBCRIBER", dataSubscriber);
                      }
                    }
                  }
                  if ((action == 3) && (notAllowExtend))
                  {
                    this.br.setLength(0);
                    this.br.append("Already in use ").append(dataPacket.getName()).append(": MSISDN=").append(moRecord.getMsisdn());
                    
                    this.logger.info(this.br);
                    moRecord.setErrCode("11");
                    moRecord.setErrOcs("Already in use " + dataPacket.getName());
                    String message = Commons.getInstance("PROCESS").getConfig("msg_already_in_use_" + language, this.logger).replace("%packet%", dataPacket.getSyntax());
                    

                    moRecord.setMessage(message);
                    continue;
                  }
                  if ((isExtend) && (!notAllowExtend))
                  {
                    DataSubscriber dataSub = (DataSubscriber)moRecord.getHashMap().get("DATA_SUBCRIBER");
                    if (dataSub.getID() <= 0L)
                    {
                      Object dataObj = getPricePlan(moRecord, dataPacket, dataPacket, dataSub);
                      if (dataObj == null)
                      {
                        moRecord.setErrCode("1");
                        moRecord.setErrOcs("Error get priceplane on product");
                        moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("system_fail_" + language, this.logger));
                        
                        continue;
                      }
                      dataSub.setAutoExtend(dataPacket.isAutoExtend() ? 1 : 0);
                      dataSub.setPaidTime(new Date());
                      dataSub.setRestrictData(dataPacket.getRestrictData());
                      if ((moRecord.getSubType().intValue() == 1) || ("1".equals(moRecord.getHashMap().get("HYBRID"))))
                      {
                        DataServicePre dataService = (DataServicePre)dataObj;
                        dataSub.setTemplateHlr(dataService.getNewTplHlr());
                        dataSub.setExpireTime(dataService.getExpire());
                      }
                      else
                      {
                        DataServicePos dataService = (DataServicePos)dataObj;
                        dataSub.setTemplateHlr(dataService.getNewTplHlr());
                        dataSub.setExpireTime(dataService.getExpire());
                      }
                    }
                  }
                }
                if ((notHasBase) || (!packetBase.isEmpty()))
                {
                  this.br.setLength(0);
                  this.br.append("Require base group [").append(dataPacket.getGroupBase()).append("] or packetBase ").append(Arrays.toString(packetBase.toArray(new String[packetBase.size()]))).append(": MSISDN=").append(moRecord.getMsisdn()).append(" - PACKET:").append(dataPacket.getName());
                  


                  this.logger.info(this.br);
                  moRecord.setErrCode("13");
                  moRecord.setErrOcs("Require base group " + dataPacket.getGroupBase() + " or base packet");
                  moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("msg_not_register_base_" + dataPacket.getName() + "_" + language, this.logger));
                }
                else
                {
                  if ((listAutoAddPacket != null) && (!listAutoAddPacket.isEmpty())) {
                    moRecord.getHashMap().put("DATA_AUTO_ADD", listAutoAddPacket);
                  }
                  moRecord.getHashMap().put("DATA_PACKET", dataPacket);
                  moRecord.setErrCode("0");
                }
              }
            }
          }
        }
      }
    }
    if (!listDataSubscribers.isEmpty()) {
      this.db.revokeDataSubcriber(listDataSubscribers);
    }
    if ((!listPreCdr.isEmpty()) && (Commons.insertVasRegister)) {
      this.dbPre.insertVasRegister(listPreCdr);
    }
    if ((!listPostCdr.isEmpty()) && (Commons.insertVasRegister)) {
      this.dbPost.insertVasRegister(listPostCdr);
    }
    if ((!listCdr.isEmpty()) && (!Commons.insertVasRegister)) {
      this.db.insertCDR(listCdr);
    }
    return list;
  }
  
  public List<Record> processListRecord(List<Record> list)
    throws Exception
  {
    List<DataSubscriber> listRevoke = new ArrayList();
    
    List<DataSubscriber> listInsert = new ArrayList();
    List<CDR> listPostCdr = new ArrayList();
    List<CDR> listPreCdr = new ArrayList();
    List<CDR> listCdr = new ArrayList();
    List<PostPaid> listPostPaid = new ArrayList();
    for (Record record : list)
    {
      MoRecord moRecord = (MoRecord)record;
      if (moRecord.getErrCode().equals("0"))
      {
        String language = moRecord.getHashMap().get("LANGUAGE").toString();
        DataPacket dataPacket = (DataPacket)moRecord.getHashMap().get("DATA_PACKET");
        DataSubscriber dataSubscriber = null;
        if (moRecord.getHashMap().get("DATA_SUBCRIBER") != null) {
          dataSubscriber = (DataSubscriber)moRecord.getHashMap().get("DATA_SUBCRIBER");
          this.logger.info("dataSubscriber =" + dataSubscriber);
          
        }
        double discount = 0.0D;
        if ((Commons.listDiscount != null) && (Commons.listDiscount.get(dataPacket.getName()) != null) && (dataSubscriber != null) && (Commons.listDiscount.get(dataSubscriber.getDataName()) != null) && (((Discount)Commons.listDiscount.get(dataPacket.getName())).getGroupDiscount() == ((Discount)Commons.listDiscount.get(dataSubscriber.getDataName())).getGroupDiscount()))
        {
          Discount discountObj = (Discount)Commons.listDiscount.get(dataPacket.getName());
          discount = discountObj.getDiscount();
        }
        double fee = dataPacket.getFee() * (1.0D - discount / 100.0D);
        boolean isExtend = false;
        if (dataSubscriber != null)
        {
          String isExtendStr = null;
          if (moRecord.getHashMap().get("IS_EXTEND") != null) {
            isExtendStr = (String)moRecord.getHashMap().get("IS_EXTEND");
          }
          if ((isExtendStr != null) && (isExtendStr.equals("1")))
          {
            fee = dataPacket.getFeeExtend() * (1.0D - discount / 100.0D);
            isExtend = true;
          }
        }
        if ((dataPacket.getCheckBal() > 0.0D) && ((moRecord.getSubType().intValue() == 1) || ("1".equals(moRecord.getHashMap().get("HYBRID")))))
        {
          double balance = this.services.checkMoney(moRecord.getMsisdn(), "1");
          if (balance == -9999999.0D)
          {
            this.br.setLength(0);
            this.br.append("Error get balance on provisioning").append(": MSISDN=").append(moRecord.getMsisdn());
            

            this.logger.error(this.br);
            moRecord.setErrCode("1");
            moRecord.setErrOcs("Error get balance on provisioning");
            moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("system_fail_" + language, this.logger));
            
            continue;
          }
          if (balance < dataPacket.getCheckBal())
          {
            this.br.setLength(0);
            this.br.append("Balance not enough: MSISDN=").append(moRecord.getMsisdn());
            
            this.logger.info(this.br);
            moRecord.setErrCode("9");
            moRecord.setErrOcs("Balance not enough");
            moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("msg_not_enough_money_" + language, this.logger));
            
            continue;
          }
        }
        List listDataServiceRollback = null;
        List<String> listDataAuto = null;
        HashMap<String, Date> listDataAutoExpire = null;
        List<DataSubscriber> listSubAuto = null;
        if ((moRecord.getHashMap().get("DATA_AUTO_ADD") != null) && (dataSubscriber == null))
        {
          boolean addSuccess = true;
          listDataAuto = (List)moRecord.getHashMap().get("DATA_AUTO_ADD");
          listDataServiceRollback = new ArrayList();
          listSubAuto = new ArrayList();
          listDataAutoExpire = new HashMap();
          for (String dataName : listDataAuto)
          {
            DataPacket dataAuto = Commons.getInstance("PROCESS").getDataPacketByName(dataName, moRecord.getSubType().intValue());
            
            DataSubscriber autoSub = new DataSubscriber();
            autoSub.setMsisdn(moRecord.getMsisdn());
            autoSub.setSubId(moRecord.getSubId().longValue());
            autoSub.setProductCode(moRecord.getProductCode());
            autoSub.setDataName(dataAuto.getName());
            autoSub.setSubType(moRecord.getSubType().intValue());
            autoSub.setAutoExtend(dataAuto.isAutoExtend() ? 1 : 0);
            

            Object dataService = getPricePlan(moRecord, null, dataAuto, dataSubscriber);
            if (dataService == null)
            {
              addSuccess = false;
              moRecord.setErrCode("1");
              moRecord.setErrOcs("Error get priceplane on product");
              moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("system_fail_" + language, this.logger));
            }
            else if ((moRecord.getSubType().intValue() == 1) || ("1".equals(moRecord.getHashMap().get("HYBRID"))))
            {
              DataServicePre dataServicePre = (DataServicePre)dataService;
              listDataAutoExpire.put(dataAuto.getName(), dataServicePre.getExpire());
              
              String error = this.services.changeDataPre(dataServicePre, true);
              if (!error.equals("0"))
              {
                addSuccess = false;
                listSubAuto.clear();
                this.br.setLength(0);
                this.br.append("Error register auto packet: MSISDN=").append(moRecord.getMsisdn()).append(" - DATA_PACKET=").append(dataAuto.getName());
                
                this.logger.error(this.br);
                for (int i = listDataServiceRollback.size() - 1; i >= 0; i--)
                {
                  DataServicePre dataServiceRb = (DataServicePre)listDataServiceRollback.get(i);
                  String rb = this.services.changeDataPre(dataServiceRb, true);
                  if (rb.equals("0"))
                  {
                    this.br.setLength(0);
                    this.br.append("Rollback revoke auto packet fail: MSISDN=").append(moRecord.getMsisdn()).append(" - REVOKE_PRICEPLAN=").append(dataServiceRb.getRemovePpOcsList()).append(" - ERROR=").append(rb);
                    


                    this.logger.error(this.br);
                  }
                }
              }
              else
              {
                DataServicePre dataServiceRb = dataServicePre.cloneRollback();
                listDataServiceRollback.add(dataServiceRb);
                autoSub.setExpireTime(dataServicePre.getExpire());
                listSubAuto.add(autoSub);
              }
            }
            else
            {
              DataServicePos dataServicePos = (DataServicePos)dataService;
              listDataAutoExpire.put(dataAuto.getName(), dataServicePos.getExpire());
              
              String error = this.services.changeDataPos(dataServicePos, true);
              if (!error.equals("0"))
              {
                addSuccess = false;
                listSubAuto.clear();
                this.br.setLength(0);
                this.br.append("Error register auto packet: MSISDN=").append(moRecord.getMsisdn()).append(" - DATA_PACKET=").append(dataAuto.getName()).append(" - ERROR=").append(error);
                

                this.logger.error(this.br);
                for (int i = listDataServiceRollback.size() - 1; i >= 0; i--)
                {
                  DataServicePos dataServiceRb = (DataServicePos)listDataServiceRollback.get(i);
                  String rb = this.services.changeDataPos(dataServiceRb, true);
                  if (rb.equals("0"))
                  {
                    this.br.setLength(0);
                    this.br.append("Rollback revoke auto packet fail: MSISDN=").append(moRecord.getMsisdn()).append(" - REVOKE_PRICEPLAN=").append(dataServiceRb.getRemovePpOcsPostList()).append(" - ERROR=").append(rb);
                   
                    this.logger.error(this.br);
                  }
                }
              }
              else
              {
                DataServicePos dataServiceRb = dataServicePos.cloneRollback();
                listDataServiceRollback.add(dataServiceRb);
                autoSub.setExpireTime(dataServicePos.getExpire());
                listSubAuto.add(autoSub);
              }
            }
          }
          if (!addSuccess) {}
        }
        else
        {
          DataPacket dataPacketOld = null;
          if (dataSubscriber != null) {
            dataPacketOld = Commons.getInstance("PROCESS").getDataPacketByName(dataSubscriber.getDataName(), dataSubscriber.getSubType());
             this.logger.info("dataPacketOld =" + dataPacketOld);
          }
          Object dataService = getPricePlan(moRecord, dataPacketOld, dataPacket, dataSubscriber);
          if (dataService == null)
          {
            moRecord.setErrCode("5");
            moRecord.setErrOcs("Fail to get price plan on product");
            moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("system_fail_" + language, this.logger));
          }
          else
          {
            Date expire = null;
            String error = "";
            String templateHlr = "";
            if ((moRecord.getSubType().intValue() == 1) || ("1".equals(moRecord.getHashMap().get("HYBRID"))))
            {
              if (fee > 0.0D)
              {
                String errCharge = this.services.chargeMoney(moRecord.getMsisdn(), fee);
                ChargeLog chargeLog = new ChargeLog(moRecord.getMsisdn(), moRecord.getSubId().longValue(), dataPacket.getName(), 1, "1", "-" + fee, errCharge);
                
                this.db.insertChargLog(chargeLog);
                if (errCharge.equals(Commons.getInstance("PROVISIONING").getConfig("error_not_enought", this.logger)))
                {
                  this.br.setLength(0);
                  this.br.append("Balance not enough: MSISDN=").append(moRecord.getMsisdn());
                  
                  this.logger.info(this.br);
                  moRecord.setErrCode("9");
                  moRecord.setErrOcs("Balance not enough");
                  moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("msg_not_enough_money_" + language, this.logger));
                  if (listDataServiceRollback == null) {
                    continue;
                  }
                  for (int i = listDataServiceRollback.size() - 1; i >= 0; i--)
                  {
                    DataServicePre dataServiceRb = (DataServicePre)listDataServiceRollback.get(i);
                    String rb = this.services.changeDataPre(dataServiceRb, true);
                    if (rb.equals("0"))
                    {
                      this.br.setLength(0);
                      this.br.append("Rollback revoke auto packet fail: MSISDN=").append(moRecord.getMsisdn()).append(" - REVOKE_PRICEPLAN=").append(dataServiceRb.getRemovePpOcsList()).append(" - ERROR=").append(rb);
                      


                      this.logger.error(this.br);
                    }
                  }
                  continue;
                }
                if (!errCharge.equals("0"))
                {
                  moRecord.setErrCode("5");
                  moRecord.setErrOcs(errCharge);
                  moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("system_fail_" + language, this.logger));
                  if (listDataServiceRollback == null) {
                    continue;
                  }
                  for (int i = listDataServiceRollback.size() - 1; i >= 0; i--)
                  {
                    DataServicePre dataServiceRb = (DataServicePre)listDataServiceRollback.get(i);
                    String rb = this.services.changeDataPre(dataServiceRb, true);
                    if (rb.equals("0"))
                    {
                      this.br.setLength(0);
                      this.br.append("Rollback revoke auto packet fail: MSISDN=").append(moRecord.getMsisdn()).append(" - REVOKE_PRICEPLAN=").append(dataServiceRb.getRemovePpOcsList()).append(" - ERROR=").append(rb);
                      
                      this.logger.error(this.br);
                    }
                  }
                  continue;
                }
              }
              DataServicePre dataServicePre = (DataServicePre)dataService;
              if (discount > 0.0D) {
                dataServicePre.setExpire(dataServicePre.getExpreOld());
              }
              expire = dataServicePre.getExpire();
              templateHlr = dataServicePre.getNewTplHlr();
              
              error = this.services.changeDataPre(dataServicePre, true);
              if (!error.equals("0"))
              {
                this.br.setLength(0);
                this.br.append("Register fail: MSISDN=").append(moRecord.getMsisdn());
                
                this.logger.error(this.br);
                moRecord.setErrCode("5");
                moRecord.setErrOcs("Register fail: " + error);
                moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("system_fail_" + language, this.logger));
                if (fee > 0.0D)
                {
                  this.br.setLength(0);
                  this.br.append("Rollback add money: MSISDN=").append(moRecord.getMsisdn()).append("MONEY=").append(fee);
                  
                  this.logger.info(this.br);
                  String errorAdd = this.services.changeBalance(moRecord.getMsisdn(), "1", true, Services.doubleToString(fee), null);
                  
                  ChargeLog chargeLog = new ChargeLog(moRecord.getMsisdn(), moRecord.getSubId().longValue(), dataPacket.getName(), 1, "1", "" + fee, errorAdd);
                  

                  this.db.insertChargLog(chargeLog);
                  if (!errorAdd.equals("0"))
                  {
                    this.br.setLength(0);
                    this.br.append("Rollback add money fail: MSISDN=").append(moRecord.getMsisdn());
                    
                    this.logger.error(this.br);
                  }
                }
                if (listDataServiceRollback == null) {
                  continue;
                }
                for (int i = listDataServiceRollback.size() - 1; i >= 0; i--)
                {
                  DataServicePre dataServiceRb = (DataServicePre)listDataServiceRollback.get(i);
                  String rb = this.services.changeDataPre(dataServiceRb, true);
                  if (rb.equals("0"))
                  {
                    this.br.setLength(0);
                    this.br.append("Rollback revoke auto packet fail: MSISDN=").append(moRecord.getMsisdn()).append(" - REVOKE_PRICEPLAN=").append(dataServiceRb.getRemovePpOcsList()).append(" - ERROR=").append(rb);
                    


                    this.logger.error(this.br);
                  }
                }
                continue;
              }
              if (dataPacketOld != null) {
                this.logger.info("Change Data success: MSISDN=" + moRecord.getMsisdn());
              } else {
                this.logger.info("Register Data success: MSISDN=" + moRecord.getMsisdn());
              }
              if ((dataPacket.getPcrfName() != null) && (!dataPacket.getPcrfBalanceId().isEmpty()) && (dataPacket.getRestrictData() > 0.0D))
              {
                String addPcrf = "";
                

                addPcrf = this.services.pcrfAddSubOCS(moRecord.getMsisdn(), dataPacket.getPcrfBalanceId(), Long.toString(Math.round(dataPacket.getRestrictData())), dataServicePre.getAddPpOcsList(), dataServicePre.getExpirePro());
                if (!addPcrf.equals("0")) {
                  this.logger.error("Add PCRF error: MSISDN=" + moRecord.getMsisdn());
                }
              }
              else if ((dataPacketOld != null) && (dataPacketOld.getPcrfName() != null) && (!dataPacketOld.getPcrfName().isEmpty()))
              {
                String removePcrf = this.services.pcrfRemoveSub(moRecord.getMsisdn());
                if (!removePcrf.equals("0")) {
                  this.logger.error("Remove PCRF error: MSISDN=" + moRecord.getMsisdn());
                }
              }
              String currentDataName = "";
              String action = "1";
              if (dataPacketOld != null)
              {
                action = "2";
                currentDataName = dataPacketOld.getName();
              }
              boolean isAdd = true;
              DataAction dataAction = Commons.getInstance("PROCESS").getDataAction(currentDataName, dataPacket.getName(), moRecord.getProductCode(), action);
              if (dataAction != null) {
                isAdd = !dataAction.isResetExtra();
              }
              List<PacketExtra> listExtra = dataPacket.getListExtraForProduct(moRecord.getProductCode());
              if (dataPacketOld != null)
              {
                List<PacketExtra> listExtraOld = dataPacketOld.getListExtraForProduct(moRecord.getProductCode());
                for (PacketExtra packetExtra : listExtraOld)
                {
                  boolean isReset = true;
                  for (PacketExtra packetExtra1 : listExtra) {
                    if (packetExtra1.getBalanceId().equals(packetExtra.getBalanceId()))
                    {
                      isReset = false;
                      break;
                    }
                  }
                  if (isReset)
                  {
                    this.logger.info("Reset balance " + packetExtra.getBalanceId() + ": MSISDN=" + moRecord.getMsisdn());
                    
                    
                    String err = this.services.changeBalance(moRecord.getMsisdn(), packetExtra.getBalanceId(), !isAdd, "0", null);
                    
                    ChargeLog chargeLogExtra = new ChargeLog(moRecord.getMsisdn(), moRecord.getSubId().longValue(), dataPacket.getName(), 1, packetExtra.getBalanceId(), "0", "Reset for change Data: " + err);
                    
                    this.db.insertChargLog(chargeLogExtra);
                  }
                }
              }
              for (PacketExtra packetExtra : listExtra)
              {
                this.logger.info("Add balance " + packetExtra.getBalanceId() + ": MSISDN=" + moRecord.getMsisdn());
                String err = "";
                
                int enable_reset = 0;
                try {
                    enable_reset = Integer.parseInt(Commons.getInstance("PROCESS").getConfig("enable_reset_" + moRecord.getCommand().toLowerCase().trim(), this.logger).trim());
                } catch (Exception e) {

                }
                
                if (discount > 0.0D) {
                  if ( (enable_reset == 1)  ){
                     err = this.services.changeBalance(moRecord.getMsisdn(), packetExtra.getBalanceId(), true, packetExtra.getBalance(), dataServicePre.getExpirePro());
                  }else{
                     err = this.services.changeBalance(moRecord.getMsisdn(), packetExtra.getBalanceId(), false, packetExtra.getBalance(), dataServicePre.getExpirePro());  
                  }
                 
                } else {
                    if ( (enable_reset == 1) ){
                        err = this.services.changeBalance(moRecord.getMsisdn(), packetExtra.getBalanceId(), true, packetExtra.getBalance(), Services.sdf.format(packetExtra.getExpireTime(this.logger)));
                    }else{
                        err = this.services.changeBalance(moRecord.getMsisdn(), packetExtra.getBalanceId(), false, packetExtra.getBalance(), Services.sdf.format(packetExtra.getExpireTime(this.logger)));
                    }
                  
                }
                ChargeLog chargeLogExtra = new ChargeLog(moRecord.getMsisdn(), moRecord.getSubId().longValue(), dataPacket.getName(), 1, packetExtra.getBalanceId(), packetExtra.getBalance(), err);
                
                this.db.insertChargLog(chargeLogExtra);
              }
            }
            else
            {
              DataServicePos dataServicePos = (DataServicePos)dataService;
              this.logger.info("DataServicePos: " + dataServicePos);
              expire = dataServicePos.getExpire();
              templateHlr = dataServicePos.getNewTplHlr();
              error = this.services.changeDataPos(dataServicePos, true);
              if (error.equals("0"))
              {
                if (fee > 0.0D)
                {
                  String errCharge = this.services.chargeMoneyPostpaid(moRecord.getMsisdn(), fee, dataServicePos.getAddPpOcsPostList(), "Charge money for " + dataPacket.getVasCode());
                  
                  ChargeLog chargeLog = null;
                  if (!errCharge.equals("0"))
                  {
                    this.br.setLength(0);
                    this.br.append("Fail to charge money postpaid: MSISDN=").append(moRecord.getMsisdn());
                    
                    this.logger.error(this.br);
                    chargeLog = new ChargeLog(moRecord.getMsisdn(), moRecord.getSubId().longValue(), dataPacket.getName(), 1, "1", "-" + fee, errCharge);
                    

                    String rbChange = this.services.changeDataPos(dataServicePos.cloneRollback(), true);
                    if (rbChange.equals("0"))
                    {
                      this.br.setLength(0);
                      this.br.append("Rollback revoke data packet fail: MSISDN=").append(moRecord.getMsisdn()).append(" - DATA_PACKET=").append(dataPacket.getName()).append(" - ERROR=").append(rbChange);
                      


                      this.logger.error(this.br);
                    }
                    if (listDataServiceRollback != null) {
                      for (int i = listDataServiceRollback.size() - 1; i >= 0; i--)
                      {
                        DataServicePos dataServiceRb = (DataServicePos)listDataServiceRollback.get(i);
                        String rb = this.services.changeDataPos(dataServiceRb, true);
                        if (rb.equals("0"))
                        {
                          this.br.setLength(0);
                          this.br.append("Rollback revoke auto packet fail: MSISDN=").append(moRecord.getMsisdn()).append(" - REVOKE_PRICEPLAN=").append(dataServiceRb.getRemovePpOcsPostList()).append(" - ERROR=").append(rb);
                          


                          this.logger.error(this.br);
                        }
                      }
                    }
                    moRecord.setErrCode("5");
                    moRecord.setErrOcs("Charge money fail: " + errCharge);
                    moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("system_fail_" + language, this.logger));
                  }
                  else
                  {
                    chargeLog = new ChargeLog(moRecord.getMsisdn(), moRecord.getSubId().longValue(), dataPacket.getName(), 1, "1", "-" + fee, errCharge);
                  }
                  this.db.insertChargLog(chargeLog);
                }
                if (dataPacketOld != null) {
                  this.logger.info("Change Data success: MSISDN=" + moRecord.getMsisdn());
                } else {
                  this.logger.info("Register Data success: MSISDN=" + moRecord.getMsisdn());
                }
              }
              else
              {
                this.br.setLength(0);
                this.br.append("Register fail: MSISDN=").append(moRecord.getMsisdn());
                
                this.logger.error(this.br);
                moRecord.setErrCode("5");
                moRecord.setErrOcs("Register fail: " + error);
                moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("system_fail_" + language, this.logger));
                if (listDataServiceRollback == null) {
                  continue;
                }
                for (int i = listDataServiceRollback.size() - 1; i >= 0; i--)
                {
                  DataServicePos dataServiceRb = (DataServicePos)listDataServiceRollback.get(i);
                  String rb = this.services.changeDataPos(dataServiceRb, true);
                  this.br.setLength(0);
                  this.br.append("Rollback revoke auto packet fail: MSISDN=").append(moRecord.getMsisdn()).append(" - REVOKE_PRICEPLAN=").append(dataServiceRb.getRemovePpOcsPostList()).append(" - ERROR=").append(rb);
                  


                  this.logger.error(this.br);
                }
                continue;
              }
            }
            if ((dataSubscriber != null) && (!isExtend))
            {
              dataSubscriber.setActNote("2");
              listRevoke.add(dataSubscriber);
              




              DataSubscriber newSub = new DataSubscriber();
              newSub.setMsisdn(moRecord.getMsisdn());
              newSub.setSubId(moRecord.getSubId().longValue());
              newSub.setProductCode(moRecord.getProductCode());
              newSub.setDataName(dataPacket.getName());
              newSub.setSubType(moRecord.getSubType().intValue());
              newSub.setExpireTime(expire);
              newSub.setAutoExtend(dataPacket.isAutoExtend() ? 1 : 0);
              newSub.setRestrictData(dataPacket.getRestrictData());
              newSub.setTemplateHlr(templateHlr);
              if (expire != null) {
                listInsert.add(newSub);
              } else {
                this.db.insertDataSubcriber(newSub);
              }
            }
            else if (dataSubscriber == null)
            {
              DataSubscriber newSub = new DataSubscriber();
              newSub.setMsisdn(moRecord.getMsisdn());
              newSub.setSubId(moRecord.getSubId().longValue());
              newSub.setProductCode(moRecord.getProductCode());
              newSub.setDataName(dataPacket.getName());
              newSub.setSubType(moRecord.getSubType().intValue());
              newSub.setExpireTime(expire);
              newSub.setAutoExtend(dataPacket.isAutoExtend() ? 1 : 0);
              newSub.setRestrictData(dataPacket.getRestrictData());
              newSub.setTemplateHlr(templateHlr);
              if (expire != null) {
                listInsert.add(newSub);
              } else {
                this.db.insertDataSubcriber(newSub);
              }
              if (listDataAuto != null) {
                for (String dataName : listDataAuto)
                {
                  DataPacket dataAuto = Commons.getInstance("PROCESS").getDataPacket(dataName, moRecord.getSubType().intValue());
                  
                  DataSubscriber autoSub = new DataSubscriber();
                  autoSub.setMsisdn(moRecord.getMsisdn());
                  autoSub.setSubId(moRecord.getSubId().longValue());
                  autoSub.setProductCode(moRecord.getProductCode());
                  autoSub.setDataName(dataAuto.getName());
                  autoSub.setSubType(moRecord.getSubType().intValue());
                  Date expireAuto = (Date)listDataAutoExpire.get(dataAuto.getName());
                  this.logger.info("Expire auto " + dataAuto.getName() + ": " + expireAuto);
                  autoSub.setExpireTime(expireAuto);
                  autoSub.setAutoExtend(dataAuto.isAutoExtend() ? 1 : 0);
                  autoSub.setRestrictData(dataAuto.getRestrictData());
                  if (expireAuto != null) {
                    listInsert.add(autoSub);
                  } else {
                    this.db.insertDataSubcriber(autoSub);
                  }
                }
              }
            }
            else if (isExtend)
            {
              dataSubscriber.setExpireTime(expire);
              dataSubscriber.setPaidTime(new Date());
              dataSubscriber.setStatus(1);
              if (dataSubscriber.getID() > 0L) {
                this.db.updateExpireDataSubcriber(dataSubscriber);
              } else if (expire != null) {
                listInsert.add(dataSubscriber);
              } else {
                this.db.insertDataSubcriber(dataSubscriber);
              }
            }
            if ((dataSubscriber != null) && (!isExtend))
            {
              CDR cdr = new CDR(dataPacketOld.getVasCode(), moRecord.getChannel(), moRecord.getMsisdn(), moRecord.getSubId() + "", "", "1", "0", "", "Cancel product " + dataPacketOld.getName(), "0");
              

              cdr.setFileTypeID(moRecord.getSubType().intValue() == 1 ? CDR.PRE_3G : CDR.POS_3G);
              
              listCdr.add(cdr);
              if (cdr.getFileTypeID() == CDR.PRE_3G) {
                listPreCdr.add(cdr);
              } else {
                listPostCdr.add(cdr);
              }
            }
            CDR cdr;
            if (!isExtend)
            {
              cdr = new CDR(dataPacket.getVasCode(), moRecord.getChannel(), moRecord.getMsisdn(), moRecord.getSubId() + "", "", "0", "0", String.valueOf(fee), "Register product " + dataPacket.getName(), "0");
              


              cdr.setFileTypeID(moRecord.getSubType().intValue() == 1 ? CDR.PRE_3G : CDR.POS_3G);
              
              listCdr.add(cdr);
              if (cdr.getFileTypeID() == CDR.PRE_3G) {
                listPreCdr.add(cdr);
              } else {
                listPostCdr.add(cdr);
              }
              if (listDataAuto != null) {
                for (String dataName : listDataAuto)
                {
                  DataPacket dataAuto = Commons.getInstance("PROCESS").getDataPacket(dataName, moRecord.getSubType().intValue());
                  
                  CDR cdr1 = new CDR(dataAuto.getVasCode(), moRecord.getChannel(), moRecord.getMsisdn(), moRecord.getSubId() + "", "", "0", "0", String.valueOf(fee), "Register product " + dataAuto.getName(), "0");
                  


                  cdr.setFileTypeID(moRecord.getSubType().intValue() == 1 ? CDR.PRE_3G : CDR.POS_3G);
                  
                  listCdr.add(cdr1);
                  if (cdr.getFileTypeID() == CDR.PRE_3G) {
                    listPreCdr.add(cdr1);
                  } else {
                    listPostCdr.add(cdr1);
                  }
                }
              }
            }
            else
            {
             cdr = new CDR(dataPacket.getVasCode(), moRecord.getChannel(), moRecord.getMsisdn(), moRecord.getSubId() + "", "", "2", "0", String.valueOf(fee), "Extend product " + dataPacket.getName(), "0");
              


              cdr.setFileTypeID(moRecord.getSubType().intValue() == 1 ? CDR.PRE_3G : CDR.POS_3G);
              
              listCdr.add(cdr);
              if (cdr.getFileTypeID() == CDR.PRE_3G) {
                listPreCdr.add(cdr);
              } else {
                listPostCdr.add(cdr);
              }
            }
            this.br.setLength(0);
            this.br.append("Return register success: MSISDN=").append(moRecord.getMsisdn());
            
            this.logger.info(this.br);
            moRecord.setErrCode("0");
            moRecord.setErrOcs("Register success to " + dataPacket.getName());
            moRecord.setFee(Double.valueOf(fee));
            String message = "";
            if ((dataSubscriber != null) && (!isExtend))
            {
              if (dataPacketOld.getRestrictData() > 0.0D)
              {
                message = Commons.getInstance("PROCESS").getConfig("msg_change_from_unlimit_" + dataPacket.getName().toLowerCase() + "_" + language, this.logger);
                if (discount > 0.0D) {
                  message = Commons.getInstance("PROCESS").getConfig("msg_change_from_unlimit_discount_" + dataPacket.getName().toLowerCase() + "_" + language, this.logger);
                }
              }
              else
              {
                message = Commons.getInstance("PROCESS").getConfig("msg_change_success_" + dataPacket.getName().toLowerCase() + "_" + language, this.logger);
                if (discount > 0.0D) {
                  message = Commons.getInstance("PROCESS").getConfig("msg_change_success_discount_" + dataPacket.getName().toLowerCase() + "_" + language, this.logger);
                }
              }
            }
            else
            {
              message = Commons.getInstance("PROCESS").getConfig("msg_register_success_" + dataPacket.getName().toLowerCase() + "_" + language, this.logger);
              if (discount > 0.0D) {
                message = Commons.getInstance("PROCESS").getConfig("msg_register_success_discount_" + dataPacket.getName().toLowerCase() + "_" + language, this.logger);
              }
            }
            if ((dataPacket.getExpire() != null) && (dataPacket.getExpire().trim().length() > 0))
            {
              message = message.replaceAll("%expire_date%", formatMsg.format(expire));
              message = message.replaceAll("%expire_hour%", "" + expire.getHours());
            }
            message = message.replaceAll("%total%", "X");
            moRecord.setMessage(message);
            if ("1".equals(moRecord.getHashMap().get("HYBRID")))
            {
              PostPaid postPaid = new PostPaid();
              postPaid.setActionType(1);
              postPaid.setFee(fee);
              postPaid.setIsdn(moRecord.getMsisdn());
              postPaid.setVasCode(dataPacket.getName());
              postPaid.setSubId(moRecord.getSubId().toString());
              postPaid.setContractId(((Subscriber)moRecord.getHashMap().get("SUB_INFO")).getContractId());
              postPaid.setSubType(0);
              listPostPaid.clear();
              listPostPaid.add(postPaid);
              this.db.insertPostPaid(listPostPaid);
            }
          }
        }
      }
    }
    if (!listRevoke.isEmpty()) {
      this.db.revokeDataSubcriber(listRevoke);
    }
    if (!listInsert.isEmpty())
    {
      this.logger.info("Num insert register: " + listInsert.size());
      this.db.insertDataSubcriber(listInsert);
    }
    if ((!listPreCdr.isEmpty()) && (Commons.insertVasRegister)) {
      this.dbPre.insertVasRegister(listPreCdr);
    }
    if ((!listPostCdr.isEmpty()) && (Commons.insertVasRegister)) {
      this.dbPost.insertVasRegister(listPostCdr);
    }
    if ((!listCdr.isEmpty()) && (!Commons.insertVasRegister)) {
      this.db.insertCDR(listCdr);
    }
    return list;
  }
  
  private Object getPricePlan(MoRecord moRecord, DataPacket dataOld, DataPacket dataNew, DataSubscriber dataSubscriber)
  {
    try
    {
      this.logger.info("Get price plan: PRODUCT_CODE=" + moRecord.getProductCode() + " => OLD=" + dataOld + " => NEW=" + dataNew);
      String language = moRecord.getHashMap().get("LANGUAGE").toString();
      String pricePlanNew = "";
      String pricePlanOld = "";
      String hlrTplNew = "";
      String hlrTplOld = "";
      Date expire = dataNew.getExpireTime(this.logger);
      String currentVas = "";
      if (moRecord.getHashMap().get("CURRENT_SERVICE") != null) {
        currentVas = (String)moRecord.getHashMap().get("CURRENT_SERVICE");
      }
      List<CmPricePlan> listPricePlanNew = this.dbProduct.getListPricePlanByVasId(moRecord.getProductCode(), dataNew.getVasCode(), moRecord.getSubType().intValue());
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
      if ((moRecord.getSubType().intValue() == 1) || ("1".equals(moRecord.getHashMap().get("HYBRID"))))
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
            if (dataSubscriber.getStatus() == 2) {
              hlrTplOld = dataOld.getTemplateRestrict();
            }
            this.logger.info("HlrTplOld= " + hlrTplOld);
          }
        }
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
        DataServicePre dataServicePre = new DataServicePre(moRecord.getMsisdn(), pricePlanNew, pricePlanOld, currentVas, hlrTplOld, hlrTplNew);
        
        dataServicePre.setExpire(expire);
        if ((dataSubscriber != null) && (dataSubscriber.getExpireTime() != null)) {
          dataServicePre.setExpreOld(dataSubscriber.getExpireTime());
        }
        if (dataServicePre.getAddPpOcsList().trim().length() == 0)
        {
          this.logger.error("[!] No add price plan on Product with exchange_type=priceplan_pos_exchange_id");
          return null;
        }
        if ((dataSubscriber != null) && 
          (dataServicePre.getRemovePpOcsList().trim().length() == 0))
        {
          this.logger.error("[!] No remove price plan on Product with exchange_type=priceplan_pos_exchange_id");
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
          if (dataSubscriber.getStatus() == 2) {
            hlrTplOld = dataOld.getTemplateRestrict();
          }
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
      if ((dataSubscriber != null) && (dataSubscriber.getExpireTime() != null))
      {
        dataServicePos.setExpreOld(dataSubscriber.getExpireTime());
        if (dataOld.getName().equals(dataNew.getName())) {
          dataServicePos.setRemovePpOcsPostList("");
        }
      }
      if (dataServicePos.getAddPpOcsPostList().trim().length() == 0)
      {
        this.logger.error("[!] No add price plan on Product with exchange_type=priceplan_pos_exchange_id");
        return null;
      }
      if ((dataSubscriber != null) && 
        (dataServicePos.getRemovePpOcsPostList().trim().length() == 0) && (!dataOld.getName().equals(dataNew.getName())))
      {
        this.logger.error("[!] No remove price plan on Product with exchange_type=priceplan_pos_exchange_id");
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
