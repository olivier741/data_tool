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
import com.viettel.vas.data.utils.Commons;
import java.util.List;
import org.apache.log4j.Logger;

public class Help
  extends ProcessRecordAbstract
{
  private String loggerLabel = Help.class.getSimpleName() + ": ";
  private StringBuilder br = new StringBuilder();
  
  public boolean startProcessRecord()
  {
    return true;
  }
  
  public void initBeforeStart()
    throws Exception
  {}
  
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
      MoRecord moRecord = (MoRecord)record;
      String language = Commons.defaultLang;
      if (moRecord.getActionType().intValue() == 10)
      {
        this.logger.info("Get guide for using Data");
        moRecord.setErrCode("0");
        moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("msg_help_3g_" + language, this.logger));
      }
      else
      {
        this.logger.info("Get guide for using Data by Action");
        moRecord.setErrCode("0");
        moRecord.setMessage(Commons.getInstance("PROCESS").getConfig("msg_help_3g_" + moRecord.getActionType() + "_" + language, this.logger));
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
    return list;
  }
}
