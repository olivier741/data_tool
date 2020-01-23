/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.data.ws.obj;

/**
 *
 * @author olivier.tatsinkou
 */
import java.sql.Timestamp;

public class ConfirmObj
{
  public static String ID = "ID";
  public static String MSISDN = "MSISDN";
  public static String COMMAND = "COMMAND";
  public static String RECEIVE_TIME = "RECEIVE_TIME";
  public static String ACTION_TYPE = "ACTION_TYPE";
  public static String TRANSACTIN_ID = "TRANSACTIN_ID";
  private String id;
  private String msisdn;
  private String command;
  private Timestamp receiveTime;
  private int actionType;
  private String transactionId;
  
  public String getId()
  {
    return this.id;
  }
  
  public void setId(String id)
  {
    this.id = id;
  }
  
  public String getMsisdn()
  {
    return this.msisdn;
  }
  
  public void setMsisdn(String msisdn)
  {
    this.msisdn = msisdn;
  }
  
  public String getCommand()
  {
    return this.command;
  }
  
  public void setCommand(String command)
  {
    this.command = command;
  }
  
  public Timestamp getReceiveTime()
  {
    return this.receiveTime;
  }
  
  public void setReceiveTime(Timestamp receiveTime)
  {
    this.receiveTime = receiveTime;
  }
  
  public int getActionType()
  {
    return this.actionType;
  }
  
  public void setActionType(int actionType)
  {
    this.actionType = actionType;
  }
  
  public String getTransactionId()
  {
    return this.transactionId;
  }
  
  public void setTransactionId(String transactionId)
  {
    this.transactionId = transactionId;
  }
}
