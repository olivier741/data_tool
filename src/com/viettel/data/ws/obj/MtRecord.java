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

public class MtRecord
{
  public static String MT_ID = "MT_ID";
  public static String MO_HIS_ID = "MO_HIS_ID";
  public static String MSISDN = "MSISDN";
  public static String MESSAGE = "MESSAGE";
  public static String RECEIVE_TIME = "RECEIVE_TIME";
  public static String RETRY_NUM = "RETRY_NUM";
  public static String CHANNEL = "CHANNEL";
  private long id;
  private String msisdn;
  private String message;
  private Timestamp receiveTime;
  private int retryNum;
  private String channel;
  
  public long getId()
  {
    return this.id;
  }
  
  public void setId(long id)
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
  
  public String getMessage()
  {
    return this.message;
  }
  
  public void setMessage(String message)
  {
    this.message = message;
  }
  
  public Timestamp getReceiveTime()
  {
    return this.receiveTime;
  }
  
  public void setReceiveTime(Timestamp receiveTime)
  {
    this.receiveTime = receiveTime;
  }
  
  public int getRetryNum()
  {
    return this.retryNum;
  }
  
  public void setRetryNum(int retryNum)
  {
    this.retryNum = retryNum;
  }
  
  public String getChannel()
  {
    return this.channel;
  }
  
  public void setChannel(String channel)
  {
    this.channel = channel;
  }
}

