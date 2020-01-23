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
import com.viettel.vas.data.obj.AddOn;
import com.viettel.vas.data.obj.AddOnSub;
import com.viettel.vas.data.obj.CDR;
import com.viettel.vas.data.obj.ChargeLog;
import com.viettel.vas.data.obj.CmPricePlan;
import com.viettel.vas.data.obj.DataPacket;
import com.viettel.vas.data.obj.DataServicePos;
import com.viettel.vas.data.obj.DataServicePre;
import com.viettel.vas.data.obj.DataSubscriber;
import com.viettel.vas.data.obj.PostPaid;
import com.viettel.vas.data.obj.Subscriber;
import com.viettel.vas.data.obj.SubscriberData;
import com.viettel.vas.data.service.Services;
import com.viettel.vas.data.utils.Commons;
import com.viettel.vas.util.ConnectionPoolManager;
import com.viettel.vas.util.ExchangeClientChannel;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.ResourceBundle;
import java.util.regex.Pattern;
import org.apache.log4j.Logger;
import utils.Config;

public class RegAddOn
  extends ProcessRecordAbstract
{
  private String loggerLabel = RegAddOn.class.getSimpleName() + ": ";
  private DbProcessor db;
  private DbPost dbPost;
  private DbPre dbPre;
  private DbProduct dbProduct;
  private Services services;
  private static SimpleDateFormat formatMsg = new SimpleDateFormat("dd/MM/yyyy");
  private static SimpleDateFormat proSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
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
          this.br.setLength(0);
          this.br.append(this.loggerLabel).append("Subscriber is post paid number: MSISDN=").append(moRecord.getMsisdn());
          

          this.logger.debug(this.br);
        }
        else
        {
          moRecord.setSubType(Integer.valueOf(1));
        }
        this.logger.info("SUB_INFO:\n" + subInfo);
        SubscriberData subscriber = new SubscriberData(moRecord.getMsisdn(), subInfo);
        language = subscriber.getLanguage();
        moRecord.getHashMap().put("LANGUAGE", language);
        moRecord.setSubId(Long.valueOf(subscriber.getSubId()));
        moRecord.setProductCode(subscriber.getProductCode());
        moRecord.getHashMap().put("SUB_INFO", subscriber);
        
        moRecord.getHashMap().put("HYBRID", "0");
        if (Commons.listHybridProductCode.contains("," + subscriber.getProductCode().toUpperCase() + ",")) {
          moRecord.getHashMap().put("HYBRID", "1");
        }
        if ((moRecord.getSubType().equals(Integer.valueOf(0))) && ("0".equals(moRecord.getHashMap().get("HYBRID"))))
        {
          moRecord.setErrCode("14");
          moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("msg_not_support_post_paid_" + language, this.logger));
        }
        else
        {
          String currentService = Commons.parseCurrentVas(subscriber.getVasList(), "-");
          moRecord.getHashMap().put("CURRENT_SERVICE", currentService);
          moRecord.setErrCode("0");
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
        }
      }
    }
    return list;
  }
  
  public List<Record> processListRecord(List<Record> list)
    throws Exception
  {
    List<DataSubscriber> listInsertDataSubcriber = new ArrayList();
    List<AddOnSub> listUpdateAddOn = new ArrayList();
    List<AddOnSub> listInsertAddOn = new ArrayList();
    List<CDR> listPreCdr = new ArrayList();
    List<CDR> listPostCdr = new ArrayList();
    List<CDR> listCdr = new ArrayList();
    List<PostPaid> listPostPaid = new ArrayList();
    for (Record record : list)
    {
      MoRecord moRecord = (MoRecord)record;
      if (moRecord.getErrCode().equals("0"))
      {
        String language = moRecord.getHashMap().get("LANGUAGE").toString();
        AddOn addOn = Commons.getInstance("PROCESS").getAddOn(moRecord.getCommand().toUpperCase(), moRecord.getSubType().intValue());
        
        AddOn oldAddOn = null;
        if (addOn == null)
        {
          this.br.setLength(0);
          this.br.append("Not found AddOn").append(": MSISDN=").append(moRecord.getMsisdn());
          

          this.logger.error(this.br);
          moRecord.setErrCode("10");
          moRecord.setErrOcs("Not found AddOn");
          moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("msg_product_not_support_" + language, this.logger));
        }
        else
        {
          if ((addOn.getRegisterDay() != null) && (!addOn.getRegisterDay().isEmpty()))
          {
            Calendar cal = Calendar.getInstance();
            cal.setTime(moRecord.getReceiveTime());
            String dayOfWeek = String.valueOf(cal.get(7));
            if (!addOn.getRegisterDay().contains(dayOfWeek))
            {
              this.br.setLength(0);
              this.br.append("Not in day register").append(": MSISDN=").append(moRecord.getMsisdn());
              this.logger.error(this.br);
              moRecord.setErrCode("10");
              moRecord.setErrOcs("Not in time");
              moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("msg_product_not_support_" + language, this.logger));
              continue;
            }
          }
          if ((addOn.getListProductCodeAllow() != null) && (!("," + addOn.getListProductCodeAllow() + ",").contains(moRecord.getProductCode())))
          {
            this.br.setLength(0);
            this.br.append("Product code was not be  allow to register").append(": MSISDN=").append(moRecord.getMsisdn());
            this.logger.error(this.br);
            moRecord.setErrCode("10");
            moRecord.setErrOcs("Product code was not be allow");
            moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("msg_product_not_support_" + language, this.logger));
          }
          else if ((addOn.getListAllow() != null) && (!this.db.checkInListAllow(addOn.getListAllow(), moRecord.getMsisdn())))
          {
            this.br.setLength(0);
            this.br.append("Not in list allow").append(": MSISDN=").append(moRecord.getMsisdn());
            this.logger.error(this.br);
            moRecord.setErrCode("10");
            moRecord.setErrOcs("Not in list allow");
            moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("notInListAllow_" + language, this.logger));
          }
          else
          {
            List<AddOnSub> listOldAddOnSub = this.db.getAddOnSub(moRecord.getMsisdn());
            AddOnSub oldAddOnSub = null;
            if (!listOldAddOnSub.isEmpty())
            {
              for (AddOnSub addOnSub : listOldAddOnSub)
              {
                oldAddOn = Commons.getInstance("PROCESS").getAddOn(addOnSub.getName(), moRecord.getSubType().intValue());
                if ((oldAddOn != null) && (oldAddOn.getGroupPacket() != null) && (addOn.getGroupPacket() != null) && (oldAddOn.getGroupPacket().equals(addOn.getGroupPacket()))) {
                  oldAddOnSub = addOnSub;
                }
              }
              if (oldAddOnSub != null) {
                oldAddOn = Commons.getInstance("PROCESS").getAddOn(oldAddOnSub.getName(), moRecord.getSubType().intValue());
              }
            }
            boolean isRegistedInCycle = false;
            if (oldAddOnSub == null) {
              isRegistedInCycle = false;
            } else if (addOn.getName().equals(oldAddOnSub.getName())) {
              isRegistedInCycle = true;
            }
            if ((isRegistedInCycle) && (oldAddOnSub.getNumberBuyCycle() + 1 > addOn.getNumBuyCycle()))
            {
              this.br.setLength(0);
              this.br.append("Over number register in cycle").append(": MSISDN=").append(moRecord.getMsisdn());
              

              this.logger.error(this.br);
              moRecord.setErrCode("16");
              moRecord.setErrOcs("Over number register in cycle");
              moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("msg_over_num_reg_" + language, this.logger));
            }
            else
            {
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
                    this.logger.warn("Please check auto extext thread for this packet: " + dataSubscriber.getMsisdn() + "=>" + dataSubscriber.getDataName());
                  }
                  else
                  {
                    listSub.add(dataSubscriber);
                  }
                }
                listSubCheck.clear();
                if ((!listSub.isEmpty()) && 
                  (("-" + addOn.getRefuseVas() + "-").contains(((DataSubscriber)listSub.get(0)).getDataName())))
                {
                  this.br.setLength(0);
                  this.br.append("Has vascode conflict ").append(((DataSubscriber)listSub.get(0)).getDataName()).append(" DATA=").append(addOn.getName()).append(": MSISDN=").append(moRecord.getMsisdn());
                  


                  this.logger.info(this.br);
                  moRecord.setErrCode("10");
                  moRecord.setErrOcs("Has vascode conflict " + ((DataSubscriber)listSub.get(0)).getDataName() + "-" + addOn.getName());
                  String message = Commons.getInstance("PROCESS").getConfig("msg_not_support_reg_add_on_" + language, this.logger).replace("%packet%", addOn.getName());
                  

                  moRecord.setMessage(message);
                }
                else
                {
                  if ((listSub.isEmpty()) && (!((String)moRecord.getHashMap().get("CURRENT_SERVICE")).contains("F0")))
                  {
                    this.br.setLength(0);
                    this.br.append(this.loggerLabel).append("Not using Data3G, ADD DEFAULT F0: MSISDN=").append(moRecord.getMsisdn());
                    

                    this.logger.info(this.br);
                    
                    boolean isUse = this.dbProduct.checkVasInProduct(moRecord.getProductCode(), "F0", moRecord.getSubType().intValue());
                    if (!isUse)
                    {
                      this.br.setLength(0);
                      this.br.append("Not allow register on product ").append("F0").append(": MSISDN=").append(moRecord.getMsisdn()).append(" => MAIN_PRODUCT=").append(moRecord.getProductCode()).append("; RELATION_PRODUCT=").append("F0");
                      


                      this.logger.info(this.br);
                      moRecord.setErrCode("10");
                      moRecord.setErrOcs("Not allow register in product F0");
                      String message = Commons.getInstance("PROCESS").getConfig("msg_product_not_support_" + language, this.logger).replace("%packet%", "F0");
                      

                      moRecord.setMessage(message);
                      continue;
                    }
                    DataPacket dataAuto = Commons.getInstance("PROCESS").getDataPacketByName("F0", moRecord.getSubType().intValue());
                    
                    DataSubscriber autoSub = new DataSubscriber();
                    autoSub.setMsisdn(moRecord.getMsisdn());
                    autoSub.setSubId(moRecord.getSubId().longValue());
                    autoSub.setProductCode(moRecord.getProductCode());
                    autoSub.setDataName(dataAuto.getName());
                    autoSub.setSubType(moRecord.getSubType().intValue());
                    autoSub.setAutoExtend(dataAuto.isAutoExtend() ? 1 : 0);
                    

                    Object dataService = getPricePlan(moRecord, null, dataAuto, null);
                    if (dataService == null)
                    {
                      moRecord.setErrCode("1");
                      moRecord.setErrOcs("Error get priceplane on product");
                      moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("system_fail_" + language, this.logger));
                      
                      continue;
                    }
                    DataServicePre dataServicePre = (DataServicePre)dataService;
                    
                    String error = this.services.changeDataPre(dataServicePre, true);
                    if (!error.equals("0"))
                    {
                      moRecord.setErrCode("5");
                      moRecord.setErrOcs("Error register default package F0");
                      moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("system_fail_" + language, this.logger));
                      
                      continue;
                    }
                    autoSub.setExpireTime(dataServicePre.getExpire());
                    listInsertDataSubcriber.add(autoSub);
                    CDR cdr = new CDR("F0", moRecord.getChannel(), moRecord.getMsisdn(), moRecord.getSubId() + "", "", "0", "0", "", "Register product F0 ", "0");
                    

                    cdr.setFileTypeID(moRecord.getSubType().intValue() == 1 ? CDR.PRE_3G : CDR.POS_3G);
                    
                    listCdr.add(cdr);
                    if (cdr.getFileTypeID() == CDR.PRE_3G) {
                      listPreCdr.add(cdr);
                    } else {
                      listPostCdr.add(cdr);
                    }
                  }
                  String errCharge = this.services.chargeMoney(moRecord.getMsisdn(), addOn.getFee());
                  ChargeLog chargeLog = new ChargeLog(moRecord.getMsisdn(), moRecord.getSubId().longValue(), addOn.getName(), 1, "1", "-" + addOn.getFee(), errCharge);
                  
                  this.db.insertChargLog(chargeLog);
                  if (errCharge.equals(Commons.getInstance("PROVISIONING").getConfig("error_not_enought", this.logger)))
                  {
                    this.br.setLength(0);
                    this.br.append("Balance not enough: MSISDN=").append(moRecord.getMsisdn());
                    
                    this.logger.info(this.br);
                    moRecord.setErrCode("9");
                    moRecord.setErrOcs("Balance not enough");
                    moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("msg_not_enough_money_" + language, this.logger));
                  }
                  else if (!errCharge.equals("0"))
                  {
                    moRecord.setErrCode("5");
                    moRecord.setErrOcs(errCharge);
                    moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("system_fail_" + language, this.logger));
                  }
                  else
                  {
                    HashMap<String, Date> expireMap = addOn.getExpireTime(this.logger);
                    

                    Calendar cal = Calendar.getInstance();
                    cal.setTimeInMillis(((Date)expireMap.get("END_TIME")).getTime());
                    cal.add(5, addOn.getExtendCycle());
                    
                    String oldPricePlan = "";
                    if ((!isRegistedInCycle) && (oldAddOnSub != null))
                    {
                      oldPricePlan = Commons.getInstance("PROCESS").getAddOnByName(oldAddOnSub.getName()).getPricePlan();
                      
                      String removePricePlan = this.services.removePricePlan(moRecord.getMsisdn(), oldPricePlan, new Date());
                      if (!removePricePlan.equals("0"))
                      {
                        this.br.setLength(0);
                        this.br.append(this.loggerLabel).append("Error to remove old price plan: MSISDN=").append(moRecord.getMsisdn()).append("PRICE_PLAN=").append(oldPricePlan);
                        



                        this.logger.info(this.br);
                        this.br.setLength(0);
                        this.br.append("Rollback add money: MSISDN=").append(moRecord.getMsisdn()).append("MONEY=").append(addOn.getFee());
                        
                        this.logger.info(this.br);
                        String errorAdd = this.services.changeBalance(moRecord.getMsisdn(), "1", true, Services.doubleToString(addOn.getFee()), null);
                        
                        chargeLog = new ChargeLog(moRecord.getMsisdn(), moRecord.getSubId().longValue(), addOn.getName(), 1, "1", "" + addOn.getFee(), errorAdd);
                        

                        this.db.insertChargLog(chargeLog);
                        if (!errorAdd.equals("0"))
                        {
                          this.br.setLength(0);
                          this.br.append("Rollback add money fail: MSISDN=").append(moRecord.getMsisdn());
                          
                          this.logger.error(this.br);
                        }
                        moRecord.setErrCode("5");
                        moRecord.setErrOcs("Remove priceplan error");
                        String message = Commons.getInstance("PROCESS").getConfig("system_fail_" + language, this.logger);
                        
                        moRecord.setMessage(message);
                        continue;
                      }
                    }
                    if (!isRegistedInCycle)
                    {
                      String addPricePlan = this.services.addPricePlan(moRecord.getMsisdn(), addOn.getPricePlan(), (Date)expireMap.get("END_TIME"), (Date)expireMap.get("START_TIME"));
                      if (!addPricePlan.equals("0"))
                      {
                        this.br.setLength(0);
                        this.br.append(this.loggerLabel).append("Error to add price plan: MSISDN=").append(moRecord.getMsisdn()).append("PRICE_PLAN=").append(addOn.getPricePlan());
                        



                        this.logger.info(this.br);
                        this.br.setLength(0);
                        this.br.append("Rollback add money: MSISDN=").append(moRecord.getMsisdn()).append("MONEY=").append(addOn.getFee());
                        
                        this.logger.info(this.br);
                        String errorAdd = this.services.changeBalance(moRecord.getMsisdn(), "1", true, Services.doubleToString(addOn.getFee()), null);
                        
                        chargeLog = new ChargeLog(moRecord.getMsisdn(), moRecord.getSubId().longValue(), addOn.getName(), 1, "1", "" + addOn.getFee(), errorAdd);
                        

                        this.db.insertChargLog(chargeLog);
                        if (!errorAdd.equals("0"))
                        {
                          this.br.setLength(0);
                          this.br.append("Rollback add money fail: MSISDN=").append(moRecord.getMsisdn());
                          
                          this.logger.error(this.br);
                        }
                        if (oldAddOnSub != null)
                        {
                          errorAdd = this.services.addPricePlan(moRecord.getMsisdn(), oldPricePlan, oldAddOnSub.getEndTime(), new Date());
                          if (!errorAdd.equals("0"))
                          {
                            this.br.setLength(0);
                            this.br.append("Rollback add old Price Plan fail: MSISDN=").append(moRecord.getMsisdn());
                            
                            this.logger.error(this.br);
                          }
                        }
                        moRecord.setErrCode("5");
                        moRecord.setErrOcs("Add price plan OCS error");
                        String message = Commons.getInstance("PROCESS").getConfig("system_fail_" + language, this.logger);
                        
                        moRecord.setMessage(message);
                        continue;
                      }
                    }
                    String addData ="";
                    int add_on_enable_reset = 0;
                    int addon_promo1_enable =0;
                    int addon_promo2_enable = 0;
                    try {
                        add_on_enable_reset = Integer.parseInt(Commons.getInstance("PROCESS").getConfig("addon_enable_reset_"+moRecord.getCommand().toLowerCase().trim(), this.logger).trim());
                        addon_promo1_enable = Integer.parseInt(Commons.getInstance("PROCESS").getConfig("addon_promo1_enable_reset_"+moRecord.getCommand().toLowerCase().trim(), this.logger).trim());
                        addon_promo2_enable = Integer.parseInt(Commons.getInstance("PROCESS").getConfig("addon_promo2_enable_reset_"+moRecord.getCommand().toLowerCase().trim(), this.logger).trim());
                    } catch (Exception e) {
                      
                    }
                     
                    if ( (add_on_enable_reset == 1)  ){
                        addData = this.services.changeBalance(moRecord.getMsisdn(), addOn.getBalanceDataId(), true, addOn.getAddData(), proSdf.format(cal.getTime()));
                     }else{
                        addData = this.services.changeBalance(moRecord.getMsisdn(), addOn.getBalanceDataId(), false, addOn.getAddData(), proSdf.format(cal.getTime()));
                     }
                    
                    
                    if ((addData.equals("0")) && (addOn.getPromotion1Id() != null))
                    {
                        String addPromotion1  = "";
                     if ( (addon_promo1_enable == 1) ){
                        addPromotion1 = this.services.changeBalance(moRecord.getMsisdn(), addOn.getPromotion1Id(), true, addOn.getPromotion1Amount(), proSdf.format(cal.getTime()));
                     }else{
                        addPromotion1 = this.services.changeBalance(moRecord.getMsisdn(), addOn.getPromotion1Id(), false, addOn.getPromotion1Amount(), proSdf.format(cal.getTime()));
                     }

                      if (!addPromotion1.equals("0")) {
                        this.logger.error("Add Promotion 1 fail: MSISDN = " + moRecord.getMsisdn() + ", Balance id = " + addOn.getPromotion1Id() + ", Promotion amount = " + addOn.getPromotion1Amount() + "\n");
                      }
                    }
                    if ((addData.equals("0")) && (addOn.getPromotion2Id() != null))
                    {
                        
                       String addPromotion2  = "";
                     if ( (addon_promo2_enable == 1) ){
                        addPromotion2 = this.services.changeBalance(moRecord.getMsisdn(), addOn.getPromotion2Id(), true, addOn.getPromotion2Amount(), proSdf.format(cal.getTime()));
                     }else{
                        addPromotion2 = this.services.changeBalance(moRecord.getMsisdn(), addOn.getPromotion2Id(), false, addOn.getPromotion2Amount(), proSdf.format(cal.getTime()));
                     }

                      if (!addPromotion2.equals("0")) {
                        this.logger.error("Add Promotion 2 fail: MSISDN = " + moRecord.getMsisdn() + ", Balance id = " + addOn.getPromotion2Id() + ", Promotion amount = " + addOn.getPromotion2Amount() + "\n");
                      }
                    }
                    if (!addData.equals("0"))
                    {
                      this.br.setLength(0);
                      this.br.append(this.loggerLabel).append("Error to add data: MSISDN=").append(moRecord.getMsisdn()).append("DATA=").append(addOn.getAddData());
                      



                      this.logger.info(this.br);
                      this.br.setLength(0);
                      this.br.append("Rollback add money: MSISDN=").append(moRecord.getMsisdn()).append("MONEY=").append(addOn.getFee());
                      
                      this.logger.info(this.br);
                      String errorAdd = this.services.changeBalance(moRecord.getMsisdn(), "1", true, Services.doubleToString(addOn.getFee()), null);
                      
                      chargeLog = new ChargeLog(moRecord.getMsisdn(), moRecord.getSubId().longValue(), addOn.getName(), 1, "1", "" + addOn.getFee(), errorAdd);
                      

                      this.db.insertChargLog(chargeLog);
                      if (!errorAdd.equals("0"))
                      {
                        this.br.setLength(0);
                        this.br.append("Rollback add money fail: MSISDN=").append(moRecord.getMsisdn());
                        
                        this.logger.error(this.br);
                      }
                      if (!isRegistedInCycle)
                      {
                        String errRemovePriceplan = this.services.removePricePlan(moRecord.getMsisdn(), addOn.getPricePlan(), new Date());
                        if (!errRemovePriceplan.equals("0"))
                        {
                          this.br.setLength(0);
                          this.br.append("Rollback remove PricePlan OCS fail: MSISDN=").append(moRecord.getMsisdn());
                          
                          this.logger.error(this.br);
                        }
                        if (oldAddOnSub != null)
                        {
                          errorAdd = this.services.addPricePlan(moRecord.getMsisdn(), oldPricePlan, oldAddOnSub.getEndTime(), new Date());
                          if (!errorAdd.equals("0"))
                          {
                            this.br.setLength(0);
                            this.br.append("Rollback add old Price Plan fail: MSISDN=").append(moRecord.getMsisdn());
                            
                            this.logger.error(this.br);
                          }
                        }
                      }
                      moRecord.setErrCode("5");
                      moRecord.setErrOcs("Add Data balance Error");
                      String message = Commons.getInstance("PROCESS").getConfig("system_fail_" + language, this.logger);
                      
                      moRecord.setMessage(message);
                    }
                    else
                    {
                      if ((addOn.getPcrfName() != null) && (!addOn.getPcrfName().isEmpty()))
                      {
                        String addPcrf = "";
                        

                        addPcrf = this.services.pcrfAddSubOCS(moRecord.getMsisdn(), addOn.getPcrfBalanceId(), addOn.getAddData(), addOn.getPricePlan(), proSdf.format((Date)expireMap.get("END_TIME")).toString());
                        if (!addPcrf.equals("0"))
                        {
                          this.logger.error("Add PCRF error: MSISDN=" + moRecord.getMsisdn());
                          moRecord.setErrOcs("Add PCRF error");
                        }
                      }
                      if (isRegistedInCycle)
                      {
                        oldAddOnSub.setNumberBuyCycle(oldAddOnSub.getNumberBuyCycle() + 1);
                        oldAddOnSub.setExtendCycle(addOn.getExtendCycle());
                        listUpdateAddOn.add(oldAddOnSub);
                      }
                      else
                      {
                        AddOnSub newAddOnSub = new AddOnSub();
                        newAddOnSub.setMsisdn(moRecord.getMsisdn());
                        newAddOnSub.setName(addOn.getName());
                        newAddOnSub.setNumberBuyCycle(1);
                        newAddOnSub.setEndTime((Date)expireMap.get("END_TIME"));
                        newAddOnSub.setStartTime((Date)expireMap.get("START_TIME"));
                        newAddOnSub.setSubId(moRecord.getSubId().longValue());
                        newAddOnSub.setSubType(moRecord.getSubType().intValue());
                        newAddOnSub.setExtendCycle(addOn.getExtendCycle());
                        listInsertAddOn.add(newAddOnSub);
                        if (oldAddOnSub != null)
                        {
                          oldAddOnSub.setStatus(0);
                          oldAddOnSub.setEndTime(new Date());
                          listUpdateAddOn.add(oldAddOnSub);
                        }
                      }
                      this.br.setLength(0);
                      this.br.append(this.loggerLabel).append("Buy data success: MSISDN=").append(moRecord.getMsisdn());
                      

                      this.logger.info(this.br);
                      moRecord.setErrCode("0");
                      String message = Commons.getInstance("PROCESS").getConfig("msg_register_success_" + addOn.getName().toLowerCase() + "_" + language, this.logger);
                      
                      message = message.replace("%expire_date%", formatMsg.format((Date)expireMap.get("END_TIME")));
                      moRecord.setErrOcs("Register success " + addOn.getName());
                      moRecord.setFee(Double.valueOf(Double.parseDouble(Long.toString(addOn.getFee()))));
                      moRecord.setMessage(message);
                      if ("1".equals(moRecord.getHashMap().get("HYBRID")))
                      {
                        PostPaid postPaid = new PostPaid();
                        postPaid.setActionType(1);
                        postPaid.setFee(addOn.getFee());
                        postPaid.setIsdn(moRecord.getMsisdn());
                        postPaid.setVasCode(addOn.getName());
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
            }
          }
        }
      }
    }
    if (!listInsertDataSubcriber.isEmpty()) {
      this.db.insertDataSubcriber(listInsertDataSubcriber);
    }
    if (!listInsertAddOn.isEmpty()) {
      this.db.insertAddOnSub(listInsertAddOn);
    }
    if (!listUpdateAddOn.isEmpty()) {
      this.db.updateAddOnSub(listUpdateAddOn);
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
      DataServicePos dataServicePos = new DataServicePos(moRecord.getMsisdn(), pricePlanNew, pricePlanOld, currentVas, proSdf.format(new Date()), hlrTplOld, hlrTplNew);
      

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
      if (dataServicePos.getNewTplHlr().trim().length() == 0)
      {
        this.logger.error("[!] No add hlr template on Product with exchange_type=hlrbroadband_exchange_id");
        return null;
      }
      if (dataSubscriber != null)
      {
        if ((dataServicePos.getRemovePpOcsPostList().trim().length() == 0) && (!dataOld.getName().equals(dataNew.getName())))
        {
          this.logger.error("[!] No remove price plan on Product with exchange_type=priceplan_pos_exchange_id");
          return null;
        }
        if (dataServicePos.getOldTplHlr().trim().length() == 0)
        {
          this.logger.error("[!] No remove hlr template on Product with exchange_type=hlrbroadband_exchange_id");
          return null;
        }
      }
      return dataServicePos;
    }
    catch (Exception ex)
    {
      this.logger.error("Error getPricePlan", ex);
    }
    return null;
  }
}
