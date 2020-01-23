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
public class Request
{
  private String msisdn;
  private String username;
  private String ip;
  private String packageName;
  private String sendSms;
  
  public String getMsisdn()
  {
    return this.msisdn;
  }
  
  public void setMsisdn(String msisdn)
  {
    this.msisdn = msisdn;
  }
  
  public String getSendSms()
  {
    return this.sendSms;
  }
  
  public void setSendSms(String sendSms)
  {
    this.sendSms = sendSms;
  }
  
  public String toString()
  {
    StringBuilder br = new StringBuilder();
    br.append("REQUEST :\n");
    br.append("<MSISDN>").append(this.msisdn != null ? this.msisdn : "").append("</MSISDN>").append("\n");
    br.append("<USERNAME>").append(this.username != null ? this.username : "").append("</USERNAME>").append("\n");
    br.append("<IP>").append(this.ip != null ? this.ip : "").append("</IP>").append("\n");
    return br.toString();
  }
}

