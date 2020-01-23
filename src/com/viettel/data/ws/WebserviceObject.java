/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.data.ws;

/**
 *
 * @author olivier.tatsinkou
 */
public class WebserviceObject
{
  private String name;
  private String ip;
  private String port;
  private String path;
  private String implementClass;
  private String url;
  
  public String getIp()
  {
    return this.ip;
  }
  
  public void setIp(String ip)
  {
    this.ip = ip;
  }
  
  public String getName()
  {
    return this.name;
  }
  
  public void setName(String name)
  {
    this.name = name;
  }
  
  public String getPort()
  {
    return this.port;
  }
  
  public void setPort(String port)
  {
    this.port = port;
  }
  
  public String getPath()
  {
    return this.path;
  }
  
  public void setPath(String path)
  {
    this.path = path;
  }
  
  public String getImplementClass()
  {
    return this.implementClass;
  }
  
  public void setImplementClass(String implementClass)
  {
    this.implementClass = implementClass;
  }
  
  public String getUrl()
  {
    return this.url;
  }
  
  public void setUrl(String url)
  {
    this.url = url;
  }
  
  public void makeUrl()
  {
    this.url = ("http://" + this.ip + ":" + this.port + "/" + this.path);
  }
}

