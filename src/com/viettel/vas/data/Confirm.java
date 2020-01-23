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
import com.viettel.vas.data.database.DbProcessor;
import com.viettel.vas.data.obj.ConfirmObj;
import com.viettel.vas.data.utils.Commons;
import com.viettel.vas.util.ConnectionPoolManager;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;
import org.apache.log4j.Logger;
import utils.Config;

public class Confirm
  extends ProcessRecordAbstract
{
  private String loggerLabel = Confirm.class.getSimpleName() + ": ";
  private DbProcessor db;
  private StringBuilder br = new StringBuilder();
  private static SimpleDateFormat formatPro = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  private static SimpleDateFormat formatMsg = new SimpleDateFormat("dd/MM/yyyy");
  private String countryCode;
  private Register register;
  private RegAddOn regAddOn;
  private Revoke revoke;
  
  public void initBeforeStart()
    throws Exception
  {
    ConnectionPoolManager.loadConfig(Config.configDir + File.separator + "database.xml");
    this.db = new DbProcessor("database", this.logger);
    Commons.createInstance("PROCESS", this.db, this.logger);
    this.countryCode = ResourceBundle.getBundle("vas").getString("country_code");
    this.register = new Register();
    this.register.initBeforeStart();
    this.revoke = new Revoke();
    this.revoke.initBeforeStart();
    this.regAddOn = new RegAddOn();
    this.regAddOn.initBeforeStart();
  }
  
  public List<Record> validateContraint(List<Record> listMo)
    throws Exception
  {
    for (Record record : listMo)
    {
      MoRecord moRecord = (MoRecord)record;
      ConfirmObj confirmObj = this.db.getConfirm(moRecord.getMsisdn());
      if (confirmObj == null)
      {
        this.br.setLength(0);
        this.br.append(this.loggerLabel).append("Error check confirm transaction: MSISDN=").append(moRecord.getMsisdn());
        

        this.logger.info(this.br);
        moRecord.setErrCode("1");
        moRecord.setErrOcs("Error check confirm transaction");
        moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("system_fail", this.logger));
      }
      else
      {
        moRecord.setObject(confirmObj);
        moRecord.setErrCode("0");
        confirmObj.setContent(moRecord.getCommand());
      }
    }
    return listMo;
  }
  
  public List<Record> processListRecord(List<Record> listMo)
    throws Exception
  {
    List<ConfirmObj> listConfirmDelete = new ArrayList();
    for (Record record : listMo)
    {
      MoRecord moRecord = (MoRecord)record;
      if (moRecord.getErrCode().equals("0")) {
        try
        {
          ConfirmObj confirmObj = (ConfirmObj)moRecord.getObject();
          if (confirmObj.getId() == 0L)
          {
            moRecord.setErrCode("19");
            moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("no_waiting_confirm", this.logger));
          }
          if ((moRecord.getCommand().equalsIgnoreCase("YES")) || (moRecord.getCommand().equalsIgnoreCase("1")))
          {
            if (confirmObj.getActionType() == 1)
            {
              MoRecord mo = new MoRecord();
              mo.setMsisdn(moRecord.getMsisdn());
              mo.setCommand(confirmObj.getCommand());
              mo.setActionType(Integer.valueOf(1));
              mo.setChannel("865");
              mo.setParam(null);
              List<Record> listInsertMo = new ArrayList();
              
              listInsertMo.add(mo);
              List<Record> listResult = new ArrayList();
              String listAddOnPackage = "FN1,FN2,FS1,FS2,FS3,FS4,FS5,FF1,FF2,FF3,FF4,FF5";
              if (listAddOnPackage.contains(mo.getCommand())) {
                listResult = this.regAddOn.processListRecord(this.regAddOn.validateContraint(listInsertMo));
              } else {
                listResult = this.register.processListRecord(this.register.validateContraint(listInsertMo));
              }
              MoRecord moResult = (MoRecord)listResult.get(0);
              
              moRecord.setErrCode(moResult.getErrCode());
              moRecord.setMessage(moResult.getMessage());
              moRecord.setCommand(confirmObj.getCommand());
              confirmObj.setErrorCode(moResult.getErrCode());
              listConfirmDelete.add(confirmObj);
              continue;
            }
            if (confirmObj.getActionType() == 2)
            {
              MoRecord mo = new MoRecord();
              mo.setMsisdn(moRecord.getMsisdn());
              mo.setCommand(confirmObj.getCommand());
              mo.setActionType(Integer.valueOf(1));
              mo.setChannel("865");
              mo.setParam(null);
              List<Record> listInsertMo = new ArrayList();
              listInsertMo.add(mo);
              List<Record> listResult = this.revoke.processListRecord(this.revoke.validateContraint(listInsertMo));
              
              MoRecord moResult = (MoRecord)listResult.get(0);
              
              moRecord.setErrCode(moResult.getErrCode());
              moRecord.setMessage(moResult.getMessage());
              confirmObj.setErrorCode(moResult.getErrCode());
              listConfirmDelete.add(confirmObj);
            }
          }
          else
          {
            moRecord.setErrCode("0");
            moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("confirm_no", this.logger));
            
            confirmObj.setErrorCode("0");
            listConfirmDelete.add(confirmObj);
            continue;
          }
        }
        catch (Exception ex)
        {
          this.logger.error("ERROR process: MSISDN=" + moRecord.getMsisdn(), ex);
          moRecord.setErrCode("99");
          moRecord.setErrOcs("Error process: " + ex.getMessage());
          String message = Commons.getInstance("PROCESS").getConfig("system_fail", this.logger);
          
          moRecord.setMessage(message);
        }
      }
    }
    if (listConfirmDelete.size() > 0)
    {
      this.db.deleteConfirm(listConfirmDelete);
      this.db.insertConfirmHis(listConfirmDelete);
    }
    return listMo;
  }
  
  public boolean startProcessRecord()
  {
    return true;
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
    return list;
  }
}
