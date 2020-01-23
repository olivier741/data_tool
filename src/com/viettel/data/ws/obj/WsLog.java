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

public class WsLog
{
  private long id;
  private long userId;
  private String ipClient;
  private Timestamp requestTime;
  private String request;
  private String msisdn;
  private int type;
  private Timestamp responseTime;
  private String response;
  private String responseCode;
  private double fee;
  private String channel;
  
  public String getChannel()
  {
    return this.channel;
  }
  
  public void setChannel(String channel)
  {
    this.channel = channel;
  }
  
  public long getId()
  {
    return this.id;
  }
  
  public void setId(long id)
  {
    this.id = id;
  }
  
  public double getFee()
  {
    return this.fee;
  }
  
  public void setFee(double fee)
  {
    this.fee = fee;
  }
  
  public String getIpClient()
  {
    return this.ipClient;
  }
  
  public void setIpClient(String ipClient)
  {
    this.ipClient = ipClient;
  }
  
  public String getMsisdn()
  {
    return this.msisdn;
  }
  
  public void setMsisdn(String msisdn)
  {
    this.msisdn = msisdn;
  }
  
  public String getRequest()
  {
    return this.request;
  }
  
  public void setRequest(String request)
  {
    this.request = request;
  }
  
  public Timestamp getRequestTime()
  {
    return this.requestTime;
  }
  
  public void setRequestTime(Timestamp requestTime)
  {
    this.requestTime = requestTime;
  }
  
  public String getResponse()
  {
    return this.response;
  }
  
  public void setResponse(String response)
  {
    this.response = response;
  }
  
  public String getResponseCode()
  {
    return this.responseCode;
  }
  
  public void setResponseCode(String responseCode)
  {
    this.responseCode = responseCode;
  }
  
  public Timestamp getResponseTime()
  {
    return this.responseTime;
  }
  
  public void setResponseTime(Timestamp responseTime)
  {
    this.responseTime = responseTime;
  }
  
  public int getType()
  {
    return this.type;
  }
  
  public void setType(int type)
  {
    this.type = type;
  }
  
  public long getUserId()
  {
    return this.userId;
  }
  
  public void setUserId(long userId)
  {
    this.userId = userId;
  }
}

