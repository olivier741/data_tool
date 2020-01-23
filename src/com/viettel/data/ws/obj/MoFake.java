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
public class MoFake
{
  public static String COMMAND = "COMMAND";
  public static String PARAM = "PARAM";
  public static String CHANNEL = "CHANNEL";
  public static String ACTION_TYPE = "ACTION_TYPE";
  public static String SYNTAX = "SYNTAX";
  private String command;
  private String param;
  private String channel;
  private int actionType;
  private String syntax;
  
  public String getCommand()
  {
    return this.command;
  }
  
  public void setCommand(String command)
  {
    this.command = command;
  }
  
  public String getParam()
  {
    return this.param;
  }
  
  public void setParam(String param)
  {
    this.param = param;
  }
  
  public String getChannel()
  {
    return this.channel;
  }
  
  public void setChannel(String channel)
  {
    this.channel = channel;
  }
  
  public int getActionType()
  {
    return this.actionType;
  }
  
  public void setActionType(int actionType)
  {
    this.actionType = actionType;
  }
  
  public String getSyntax()
  {
    return this.syntax;
  }
  
  public void setSyntax(String syntax)
  {
    this.syntax = syntax;
  }
}