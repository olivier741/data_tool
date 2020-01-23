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
import com.viettel.vas.data.database.DbProcessor;
import com.viettel.vas.data.obj.ConfirmObj;
import com.viettel.vas.data.utils.Commons;
import com.viettel.vas.util.ConnectionPoolManager;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;
import org.apache.log4j.Logger;
import utils.Config;

public class DeleteConfirm
  extends ProcessRecordAbstract
{
  private String loggerLabel = DeleteConfirm.class.getSimpleName() + ": ";
  private DbProcessor db;
  private StringBuilder br = new StringBuilder();
  private List<ConfirmObj> listConfirm;
  
  public boolean startProcessRecord()
  {
    return true;
  }
  
  public void initBeforeStart()
    throws Exception
  {
    ConnectionPoolManager.loadConfig(Config.configDir + File.separator + "database.xml");
    this.db = new DbProcessor("database", this.logger);
    Commons.createInstance("PROCESS", this.db, this.logger);
    Commons.createInstance("PROVISIONING", this.db, this.logger);
    Commons.createInstance("AUTO_EXTEND", this.db, this.logger);
    Commons.defaultLang = ResourceBundle.getBundle("vas").getString("default_lang");
    this.listConfirm = new ArrayList();
  }
  
  public List<Record> validateContraint(List<Record> list)
    throws Exception
  {
    return list;
  }
  
  public List<Record> processListRecord(List<Record> list)
    throws Exception
  {
    for (Record record : list)
    {
      ConfirmObj confirmObj = (ConfirmObj)record;
      confirmObj.setContent("EXPIRE");
      this.listConfirm.add(confirmObj);
    }
    if (!this.listConfirm.isEmpty())
    {
      this.db.insertConfirmHis(this.listConfirm);
      this.listConfirm.clear();
    }
    return list;
  }
  
  public void printListRecord(List<Record> list)
    throws Exception
  {
    this.br.setLength(0);
    this.br.append("Process list record").append(String.format("|%1$-11s|%2$-15s|%3$-10s|%4$-5s|%5$-15s", new Object[] { "ID", "MSISDN", "COMMAND", "RECEIVE_TIME", "ACTION_TYPE" })).append("\n");
    for (Record record : list)
    {
      ConfirmObj confirmObj = (ConfirmObj)record;
      this.br.append(String.format("|%1$-11s|%2$-15s|%3$-10s|%4$-5s|%5$-15s", new Object[] { Long.valueOf(confirmObj.getID()), confirmObj.getMsisdn(), confirmObj.getCommand(), confirmObj.getReceiveTime(), Integer.valueOf(confirmObj.getActionType()) }));
    }
    this.logger.info(this.br);
  }
  
  public List<Record> processException(List<Record> list)
  {
    return list;
  }
}

