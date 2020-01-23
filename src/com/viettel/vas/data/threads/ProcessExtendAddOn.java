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
import com.viettel.vas.data.obj.AddOn;
import com.viettel.vas.data.obj.AddOnSub;
import com.viettel.vas.data.obj.ChargeLog;
import com.viettel.vas.data.obj.Subscriber;
import com.viettel.vas.data.service.Services;
import com.viettel.vas.data.utils.Commons;
import com.viettel.vas.util.ConnectionPoolManager;
import com.viettel.vas.util.ExchangeClientChannel;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.ResourceBundle;
import java.util.regex.Pattern;
import org.apache.log4j.Logger;
import utils.Config;

public class ProcessExtendAddOn
        extends ProcessRecordAbstract {

    private String loggerLabel = ProcessExtendAddOn.class.getSimpleName() + ": ";
    private DbProcessor db;
    private DbProduct dbProduct;
    private DbPost dbPost;
    private DbPre dbPre;
    private Services services;
    private StringBuilder br = new StringBuilder();
    private String countryCode;
    private String channelExtend;
    private static SimpleDateFormat formatPro = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public boolean startProcessRecord() {
        return true;
    }

    public void initBeforeStart()
            throws Exception {
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
            throws Exception {
        return list;
    }

    public List<Record> processListRecord(List<Record> list)
            throws Exception {
        for (Record record : list) {
            AddOnSub addOnSub = (AddOnSub) record;
            String language = Commons.defaultLang;
            addOnSub.setChannel(this.channelExtend);
            AddOn addOnCurrent = Commons.getInstance("PROCESS").getAddOn(addOnSub.getName(), addOnSub.getSubType());
            if (addOnCurrent == null) {
                this.br.setLength(0);
                this.br.append(this.loggerLabel).append("AddOn Packet not found or packet not active: MSISDN=").append(addOnSub.getMsisdn());

                this.logger.info(this.br);
                addOnSub.setErr(3);
                addOnSub.setExtendCycle(0);
            } else {
                this.logger.info("DATA_PACKET:\n" + addOnCurrent);

                String subInfo = this.dbPost.getSubInfoMobile(addOnSub.getMsisdn().substring(this.countryCode.length()));
                if (subInfo == null) {
                    this.br.setLength(0);
                    this.br.append(this.loggerLabel).append("Fail to get subscriber info on CM_POS: MSISDN=").append(addOnSub.getMsisdn());

                    this.logger.error(this.br);
                    addOnSub.setErr(-1);
                } else {
                    if (subInfo.equals("NO_INFO_SUB")) {
                        subInfo = this.dbPre.getSubInfoMobile(addOnSub.getMsisdn().substring(this.countryCode.length()));
                        if (subInfo == null) {
                            this.br.setLength(0);
                            this.br.append(this.loggerLabel).append("Fail to get subscriber info on CM_PRE: MSISDN=").append(addOnSub.getMsisdn());

                            this.logger.error(this.br);
                            addOnSub.setErr(-1);
                            continue;
                        }
                        if (subInfo.equals("NO_INFO_SUB")) {
                            this.br.setLength(0);
                            this.br.append(this.loggerLabel).append("Subscriber is not mobile number: MSISDN=").append(addOnSub.getMsisdn());

                            this.logger.info(this.br);

                            addOnSub.setErr(4);
                            continue;
                        }
                    }
                    this.logger.info("SUB_INFO:\n" + subInfo);
                    Subscriber subscriber = new Subscriber(addOnSub.getMsisdn(), subInfo);
                    language = subscriber.getLanguage();

                    boolean isAdd = true;

                    double fee = addOnCurrent.getExtendFee();
                    if (fee > 0.0D) {
                        String errCharge = this.services.chargeMoney(addOnSub.getMsisdn(), fee);
                        ChargeLog chargeLog = new ChargeLog(addOnSub.getMsisdn(), addOnSub.getSubId(), addOnCurrent.getName(), 11, "1", "-" + fee, errCharge);

                        this.db.insertChargLog(chargeLog);
                        if (errCharge.equals(Commons.getInstance("PROVISIONING").getConfig("error_not_enought", this.logger))) {
                            this.br.setLength(0);
                            this.br.append("Balance not enough: MSISDN=").append(addOnSub.getMsisdn());

                            this.logger.info(this.br);

                            addOnSub.setMessage(Commons.getInstance("AUTO_EXTEND").getConfig("msg_extend_addon_not_enough_" + addOnCurrent.getName().toLowerCase() + "_" + language, this.logger, ""));

                            addOnSub.setErr(2);
                            continue;
                        }
                        if (!errCharge.equals("0")) {
                            this.br.setLength(0);
                            this.br.append("Charge extend error: MSISDN=").append(addOnSub.getMsisdn());

                            this.logger.info(this.br);
                            addOnSub.setErr(-1);
                            continue;
                        }
                    }
                    HashMap<String, Date> expireMap = addOnCurrent.getExpireTime(this.logger);
                    Calendar cal = Calendar.getInstance();
                    cal.setTimeInMillis(System.currentTimeMillis());
                    cal.add(10, 1);
                    String error = this.services.addPricePlan(addOnSub.getMsisdn(), addOnCurrent.getPricePlan(), (Date) expireMap.get("END_TIME"), (Date) expireMap.get("START_TIME"));
                    if (!error.equals("0")) {
                        this.br.setLength(0);
                        this.br.append("Extend AddOn fail: MSISDN=").append(addOnSub.getMsisdn());

                        this.logger.error(this.br);
                        if (fee > 0.0D) {
                            this.br.setLength(0);
                            this.br.append("Rollback add money: MSISDN=").append(addOnSub.getMsisdn()).append("MONEY=").append(fee);

                            this.logger.info(this.br);
                            String errorAdd = this.services.changeBalance(addOnSub.getMsisdn(), "1", true, Services.doubleToString(fee), null);

                            ChargeLog chargeLog = new ChargeLog(addOnSub.getMsisdn(), addOnSub.getSubId(), addOnCurrent.getName(), 11, "1", "" + fee, errorAdd);

                            this.db.insertChargLog(chargeLog);
                            if (!errorAdd.equals("0")) {
                                this.br.setLength(0);
                                this.br.append("Rollback add money fail: MSISDN=").append(addOnSub.getMsisdn());

                                this.logger.error(this.br);
                            }
                        }
                        addOnSub.setErr(-1);
                        addOnSub.setEndTime(cal.getTime());
                    } else {
                        this.logger.info("Extend Data success: MSISDN=" + addOnSub.getMsisdn());
                        if (Long.valueOf(addOnCurrent.getExtendData()).longValue() > 0L) {
                            String err = "";

                            int add_on_enable_reset = 0;
                            try {
                                add_on_enable_reset = Integer.parseInt(Commons.getInstance("PROCESS").getConfig("addon_enable_reset_" + addOnSub.getName().toLowerCase().trim(), this.logger).trim());
                            } catch (Exception e) {

                            }

                            if ((add_on_enable_reset == 1)) {
                                err = this.services.changeBalance(addOnSub.getMsisdn(), addOnCurrent.getBalanceDataId(), true, addOnCurrent.getExtendData(), Services.sdf.format((Date) expireMap.get("END_TIME")));
                            } else {
                                err = this.services.changeBalance(addOnSub.getMsisdn(), addOnCurrent.getBalanceDataId(), false, addOnCurrent.getExtendData(), Services.sdf.format((Date) expireMap.get("END_TIME")));
                            }

                            ChargeLog chargeLogExtra = new ChargeLog(addOnSub.getMsisdn(), addOnSub.getSubId(), addOnCurrent.getName(), 11, addOnCurrent.getBalanceId(), addOnCurrent.getExtendData(), err);

                            this.db.insertChargLog(chargeLogExtra);
                        }
                        addOnSub.setErr(1);
                        addOnSub.setEndTime((Date) expireMap.get("END_TIME"));
                        addOnSub.setPaidTime(new Date());
                        addOnSub.setExtendCycle(addOnSub.getExtendCycle() - 1);

                        addOnSub.setMessage(Commons.getInstance("AUTO_EXTEND").getConfig("msg_extend_success_addon_" + addOnCurrent.getName().toLowerCase() + "_" + language, this.logger, ""));
                    }
                }
            }
        }
        return list;
    }

    public void printListRecord(List<Record> list)
            throws Exception {
        this.br.setLength(0);
        this.br.append("Process list record").append(String.format("|%1$-11s|%2$-15s|%3$-10s|%4$-5s|%5$-15s", new Object[]{"ID", "MSISDN", "ADD_ON_NAME", "EXTEND_CYCLE", "EXPIRE"})).append("\n");
        for (Record record : list) {
            AddOnSub addOnSub = (AddOnSub) record;
            this.br.append(String.format("|%1$-11s|%2$-15s|%3$-10s|%4$-5s|%5$-15s", new Object[]{Long.valueOf(addOnSub.getID()), addOnSub.getMsisdn(), addOnSub.getName(), Integer.valueOf(addOnSub.getExtendCycle()), addOnSub.getEndTime()}));
        }
        this.logger.info(this.br);
    }

    public List<Record> processException(List<Record> list) {
        return list;
    }
}
