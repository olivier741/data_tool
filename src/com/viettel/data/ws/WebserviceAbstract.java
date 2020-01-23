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
import com.sun.net.httpserver.HttpExchange;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import javax.annotation.Resource;
import javax.jws.WebMethod;
import javax.jws.WebParam;
import javax.jws.WebService;
import javax.xml.ws.WebServiceContext;
import javax.xml.ws.handler.MessageContext;
import org.apache.log4j.Logger;

@WebService
public abstract class WebserviceAbstract
{
  public Logger logger;
  public static int USER_NOT_FOUND = -1;
  public static int WRONG_PASSWORD = -2;
  public static int NOT_ALLOW = -3;
  public static int MSISDN_NOT_VALID = -4;
  public static int PARAM_NOT_ENOUGH = -5;
  public static int EXCEPTION = -6;
  @Resource
  WebServiceContext wsContext;
  
  public WebserviceAbstract(String logName)
  {
    this.logger = Logger.getLogger(logName);
  }
  
  public String getIpClient()
  {
    MessageContext msgCtxt = this.wsContext.getMessageContext();
    HttpExchange httpEx = (HttpExchange)msgCtxt.get("com.sun.xml.ws.http.exchange");
    return httpEx.getRemoteAddress().getAddress().getHostAddress();
  }
  
  public boolean pair(String ipClient, String ipConfig)
  {
    if ((ipClient == null) || (ipClient.equals("")) || (ipConfig == null) || (ipConfig.equals(""))) {
      return false;
    }
    ipConfig = ipConfig.replaceAll("x", "\\\\d+");
    return ipClient.matches(ipConfig);
  }
  
  @WebMethod(operationName="testWebService")
  public String testWebService(@WebParam(name="input") String input)
  {
    return "Test Webservice Success!!, return input: " + input;
  }
}
