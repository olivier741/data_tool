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
public class Response
{
  private String errCode;
  private String msgSend;
  private String errOcs;
  private String fee;
  private String description;
  private String listSrcNum;
  
  public String getErrCode()
  {
    return this.errCode;
  }
  
  public void setErrCode(String errCode)
  {
    this.errCode = errCode;
  }
  
  public String getMsgSend()
  {
    return this.msgSend;
  }
  
  public void setMsgSend(String msgSend)
  {
    this.msgSend = msgSend;
  }
  
  public String getErrOcs()
  {
    return this.errOcs;
  }
  
  public void setErrOcs(String errOcs)
  {
    this.errOcs = errOcs;
  }
  
  public String getFee()
  {
    return this.fee;
  }
  
  public void setFee(String fee)
  {
    this.fee = fee;
  }
  
  public String getDescription()
  {
    return this.description;
  }
  
  public void setDescription(String description)
  {
    this.description = description;
  }
  
  public String getListSrcNum()
  {
    return this.listSrcNum;
  }
  
  public void setListSrcNum(String listSrcNum)
  {
    this.listSrcNum = listSrcNum;
  }
  
  public String toString()
  {
    StringBuilder br = new StringBuilder();
    br.append("RESPONSE :\n");
    br.append("<ERR_CODE>").append(this.errCode != null ? this.errCode : "").append("</ERR_CODE>").append("\n");
    br.append("<DESCRIPTION>").append(this.description != null ? this.description : "").append("</DESCRIPTION>").append("\n");
    br.append("<FEE>").append(this.fee != null ? this.fee : "0").append("</FEE>").append("\n");
    br.append("<LIST_SRCNUM>").append(this.listSrcNum != null ? this.listSrcNum : "").append("</LIST_SRCNUM>").append("\n");
    return br.toString();
  }
}
