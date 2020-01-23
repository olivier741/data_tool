/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.data.ws.process;

/**
 *
 * @author olivier.tatsinkou
 */

import com.viettel.cluster.agent.integration.Record;
import com.viettel.data.database.DbProcessor;
import com.viettel.data.utils.Encrypt;
import com.viettel.data.ws.WebserviceAbstract;
import com.viettel.data.ws.obj.ConfirmObj;
import com.viettel.data.ws.obj.MtRecord;
import com.viettel.data.ws.obj.Request;
import com.viettel.data.ws.obj.Response;
import com.viettel.data.ws.obj.UserInfo;
import com.viettel.data.ws.obj.WsLog;
import com.viettel.smsfw.database.ConnectionPoolManager;
import com.viettel.smsfw.exchange.ExchangeClientChannel;
import com.viettel.smsfw.utils.MoRecord;
import com.viettel.vas.data.Register;
import com.viettel.vas.data.Revoke;
import com.viettel.vas.data.utils.Commons;
import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import javax.jws.WebMethod;
import javax.jws.WebParam;
import javax.jws.WebService;
import org.openide.util.Exceptions;

@WebService
public class DataWs
  extends WebserviceAbstract
{
  private DbProcessor dbProcessor;
  private Register register;
  private Revoke revoke;
  private HashMap mapUser;
  
  public DataWs()
  {
    super("DataWs");
    try
    {
      ConnectionPoolManager.loadConfig("../etc/database.xml");
      com.viettel.smsfw.manager.AppManager.pathExch = "../etc/service_client.cfg";
      ExchangeClientChannel.getInstance();
      this.dbProcessor = new DbProcessor("database", this.logger);
      this.mapUser = this.dbProcessor.getUser("database");
      this.register = new Register();
      this.register.initBeforeStart();
      this.revoke = new Revoke();
      this.revoke.initBeforeStart();
    }
    catch (Exception ex)
    {
      Exceptions.printStackTrace(ex);
    }
  }
  
  private UserInfo authenticate(String userName, String password, String ipAddress)
    throws NoSuchAlgorithmException
  {
    UserInfo userInfo = (UserInfo)this.mapUser.get(userName);
    if ((userInfo == null) || (userInfo.getId() <= 0L))
    {
      this.logger.info("User khong ton tai: " + userName);
      userInfo = new UserInfo();
      userInfo.setId(USER_NOT_FOUND);
      return userInfo;
    }
    if ((userInfo.getIp() != null) && (userInfo.getIp().length() > 0))
    {
      String[] ip = userInfo.getIp().split(",");
      boolean pass = false;
      for (String ipConfig : ip) {
        if (pair(ipAddress, ipConfig.trim()))
        {
          pass = true;
          break;
        }
      }
      if (!pass)
      {
        userInfo.setId(NOT_ALLOW);
        return userInfo;
      }
    }
    if ((userInfo.getPassword() != null) && (userInfo.getPassword().trim().length() > 0)) {
      try
      {
        if (password.equals(userInfo.getPassword())) {
          return userInfo;
        }
        String passEncript = Encrypt.MD5(password);
        if (passEncript.equals(userInfo.getPassword())) {
          return userInfo;
        }
        userInfo.setId(WRONG_PASSWORD);
      }
      catch (UnsupportedEncodingException ex)
      {
        Exceptions.printStackTrace(ex);
      }
    }
    return userInfo;
  }
  
  @WebMethod(operationName="register")
  public Response register(@WebParam(name="msisdn") String msisdn, @WebParam(name="package") String packageName, @WebParam(name="sendSms") String sendSms, @WebParam(name="username") String username, @WebParam(name="password") String password)
  {
    Request request = new Request();
    Response response = new Response();
    long startTime = System.currentTimeMillis();
    try
    {
      String ipAddress = getIpClient();
      UserInfo userInfo = new UserInfo();
      userInfo.setUsername(username);
      userInfo.setPassword(password);
      userInfo.setIp(ipAddress);
      this.logger.info("IP Client request:" + ipAddress);
      authenticate(username, password, ipAddress);
      request.setMsisdn(msisdn);
      this.logger.info(request.toString());
      
      MoRecord moRecord = new MoRecord();
      moRecord.setId(Long.valueOf(this.dbProcessor.getMoId()));
      moRecord.setMsisdn(msisdn);
      moRecord.setCommand(packageName);
      moRecord.setActionType(Integer.valueOf(1));
      moRecord.setChannel("865");
      
      moRecord.setReceiveTime(new Timestamp(Calendar.getInstance().getTime().getTime()));
      moRecord.setClusterName("WS");
      moRecord.setParam(null);
      List<Record> listMo = new ArrayList();
      List<Record> listResult = new ArrayList();
      listMo.add(moRecord);
      listResult = this.register.processListRecord(this.register.validateContraint(listMo));
      MoRecord moResult = new MoRecord();
      moResult = (MoRecord)listResult.get(0);
      response.setDescription(moResult.getErrOcs());
      response.setErrCode(moResult.getErrCode());
      response.setFee(String.valueOf(moResult.getFee() == null ? 0.0D : moResult.getFee().doubleValue()));
      if ((sendSms != null) && (sendSms.equals("1"))) {
        this.dbProcessor.insertQueueOutput(listResult);
      }
      this.dbProcessor.insertQueueHis(listResult);
      
      WsLog wsLog = new WsLog();
      wsLog.setFee(moResult.getFee() == null ? 0.0D : moResult.getFee().doubleValue());
      wsLog.setIpClient(ipAddress);
      wsLog.setMsisdn(msisdn);
      wsLog.setRequest(request.toString());
      wsLog.setResponse(response.toString());
      wsLog.setResponseCode(response.getErrCode());
      wsLog.setRequestTime(new Timestamp(startTime));
      wsLog.setResponseTime(new Timestamp(System.currentTimeMillis()));
      wsLog.setType(1);
      wsLog.setUserId(userInfo.getId());
      wsLog.setChannel(userInfo.getChannel());
      this.dbProcessor.insertWsLog(wsLog);
    }
    catch (Exception ex)
    {
      this.logger.error("Exception ", ex);
      response.setErrCode("-9");
      response.setDescription("System error");
    }
    return response;
  }
  
  
  @WebMethod(operationName="register_provider")
  public Response register_provider(@WebParam(name="msisdn") String msisdn, @WebParam(name="package") String packageName, @WebParam(name="sendSms") String sendSms, @WebParam(name="username") String username, @WebParam(name="password") String password,@WebParam(name="provider") String provider)
  {
    Request request = new Request();
    Response response = new Response();
    long startTime = System.currentTimeMillis();
    try
    {
      String ipAddress = getIpClient();
      UserInfo userInfo = new UserInfo();
      userInfo.setUsername(username);
      userInfo.setPassword(password);
      userInfo.setIp(ipAddress);
      this.logger.info("IP Client request:" + ipAddress);
      authenticate(username, password, ipAddress);
      request.setMsisdn(msisdn);
      this.logger.info(request.toString());
      
      MoRecord moRecord = new MoRecord();
      moRecord.setId(Long.valueOf(this.dbProcessor.getMoId()));
      moRecord.setMsisdn(msisdn);
      moRecord.setCommand(packageName);
      moRecord.setActionType(Integer.valueOf(1));
      moRecord.setChannel("865");
      
      moRecord.setReceiveTime(new Timestamp(Calendar.getInstance().getTime().getTime()));
      moRecord.setClusterName("WS-"+provider.trim());
      moRecord.setParam(null);
      List<Record> listMo = new ArrayList();
      List<Record> listResult = new ArrayList();
      listMo.add(moRecord);
      listResult = this.register.processListRecord(this.register.validateContraint(listMo));
      MoRecord moResult = new MoRecord();
      moResult = (MoRecord)listResult.get(0);
      response.setDescription(moResult.getErrOcs());
      response.setErrCode(moResult.getErrCode());
      response.setFee(String.valueOf(moResult.getFee() == null ? 0.0D : moResult.getFee().doubleValue()));
      if ((sendSms != null) && (sendSms.equals("1"))) {
        this.dbProcessor.insertQueueOutput(listResult);
      }
      this.dbProcessor.insertQueueHis(listResult);
      
      WsLog wsLog = new WsLog();
      wsLog.setFee(moResult.getFee() == null ? 0.0D : moResult.getFee().doubleValue());
      wsLog.setIpClient(ipAddress);
      wsLog.setMsisdn(msisdn);
      wsLog.setRequest(request.toString());
      wsLog.setResponse(response.toString());
      wsLog.setResponseCode(response.getErrCode()+"-"+provider.trim());
      wsLog.setRequestTime(new Timestamp(startTime));
      wsLog.setResponseTime(new Timestamp(System.currentTimeMillis()));
      wsLog.setType(1);
      wsLog.setUserId(userInfo.getId());
      wsLog.setChannel(userInfo.getChannel());
      this.dbProcessor.insertWsLog(wsLog);
    }
    catch (Exception ex)
    {
      this.logger.error("Exception ", ex);
      response.setErrCode("-9");
      response.setDescription("System error");
    }
    return response;
  }
  
  @WebMethod(operationName="revoke")
  public Response revoke(@WebParam(name="msisdn") String msisdn, @WebParam(name="sendSms") String sendSms, @WebParam(name="username") String username, @WebParam(name="password") String password)
  {
    Request request = new Request();
    Response response = new Response();
    long startTime = System.currentTimeMillis();
    try
    {
      String ipAddress = getIpClient();
      UserInfo userInfo = new UserInfo();
      userInfo.setUsername(username);
      userInfo.setPassword(password);
      userInfo.setIp(ipAddress);
      this.logger.info("IP Client request:" + ipAddress);
      authenticate(username, password, ipAddress);
      request.setMsisdn(msisdn);
      this.logger.info(request.toString());
      
      MoRecord moRecord = new MoRecord();
      moRecord.setId(Long.valueOf(this.dbProcessor.getMoId()));
      moRecord.setMsisdn(msisdn);
      moRecord.setCommand("OFF");
      moRecord.setActionType(Integer.valueOf(2));
      moRecord.setChannel("865");
      
      moRecord.setReceiveTime(new Timestamp(Calendar.getInstance().getTime().getTime()));
      moRecord.setClusterName("WS");
      moRecord.setParam(null);
      List<Record> listMo = new ArrayList();
      List<Record> listResult = new ArrayList();
      listMo.add(moRecord);
      listResult = this.revoke.processListRecord(this.revoke.validateContraint(listMo));
      MoRecord moResult = new MoRecord();
      moResult = (MoRecord)listResult.get(0);
      response.setDescription(moResult.getErrOcs());
      response.setErrCode(moResult.getErrCode());
      response.setFee("0");
      if ((sendSms != null) && (sendSms.equals("1"))) {
        this.dbProcessor.insertQueueOutput(listResult);
      }
      this.dbProcessor.insertQueueHis(listResult);
      
      WsLog wsLog = new WsLog();
      wsLog.setFee(Double.valueOf("0").doubleValue());
      wsLog.setIpClient(ipAddress);
      wsLog.setMsisdn(msisdn);
      wsLog.setRequest(request.toString());
      wsLog.setResponse(response.toString());
      wsLog.setResponseCode(response.getErrCode());
      wsLog.setRequestTime(new Timestamp(startTime));
      wsLog.setResponseTime(new Timestamp(System.currentTimeMillis()));
      wsLog.setType(1);
      wsLog.setUserId(userInfo.getId());
      wsLog.setChannel(userInfo.getChannel());
      this.dbProcessor.insertWsLog(wsLog);
    }
    catch (Exception ex)
    {
      this.logger.error("Exception ", ex);
      response.setErrCode("-9");
      response.setDescription("System error");
    }
    return response;
  }
  
  @WebMethod(operationName="registerConfirm")
  public Response registerConfirm(@WebParam(name="msisdn") String msisdn, @WebParam(name="package") String packageName, @WebParam(name="username") String username, @WebParam(name="password") String password, @WebParam(name="transactionId") String transactionId)
  {
    Request request = new Request();
    Response response = new Response();
    long startTime = System.currentTimeMillis();
    try
    {
      String ipAddress = getIpClient();
      UserInfo userInfo = new UserInfo();
      userInfo.setUsername(username);
      userInfo.setPassword(password);
      userInfo.setIp(ipAddress);
      this.logger.info("IP Client request:" + ipAddress);
      authenticate(username, password, ipAddress);
      request.setMsisdn(msisdn);
      this.logger.info(request.toString());
      
      ConfirmObj confirmObj = new ConfirmObj();
      confirmObj.setMsisdn(msisdn);
      confirmObj.setCommand(packageName);
      confirmObj.setActionType(1);
      confirmObj.setTransactionId(transactionId);
      
      boolean result = this.dbProcessor.insertConfirm(confirmObj);
      if (result)
      {
        MtRecord mtRecord = new MtRecord();
        mtRecord.setMsisdn(msisdn);
        String message = Commons.getInstance("PROCESS").getConfig("send_confirm_register", this.logger);
        message = message.replaceAll("%package%", packageName);
        mtRecord.setMessage(message);
        this.dbProcessor.insertMt(mtRecord);
        
        response.setErrCode("0");
        response.setDescription("Register " + packageName + " successful");
      }
      else
      {
        response.setErrCode("15");
        response.setDescription("Register " + packageName + " unsuccessful. There is one transaction need to be confirmed");
      }
      WsLog wsLog = new WsLog();
      wsLog.setFee(0.0D);
      wsLog.setIpClient(ipAddress);
      wsLog.setMsisdn(msisdn);
      wsLog.setRequest(request.toString());
      wsLog.setResponse(response.toString());
      wsLog.setResponseCode(response.getErrCode());
      wsLog.setRequestTime(new Timestamp(startTime));
      wsLog.setResponseTime(new Timestamp(System.currentTimeMillis()));
      wsLog.setType(1);
      wsLog.setUserId(userInfo.getId());
      wsLog.setChannel(userInfo.getChannel());
      this.dbProcessor.insertWsLog(wsLog);
    }
    catch (Exception ex)
    {
      this.logger.error("Exception ", ex);
      response.setErrCode("-9");
      response.setDescription("System error");
    }
    return response;
  }
  
  @WebMethod(operationName="revokeConfirm")
  public Response revokeConfirm(@WebParam(name="msisdn") String msisdn, @WebParam(name="username") String username, @WebParam(name="password") String password, @WebParam(name="transactionId") String transactionId)
  {
    Request request = new Request();
    Response response = new Response();
    long startTime = System.currentTimeMillis();
    try
    {
      String ipAddress = getIpClient();
      UserInfo userInfo = new UserInfo();
      userInfo.setUsername(username);
      userInfo.setPassword(password);
      userInfo.setIp(ipAddress);
      this.logger.info("IP Client request:" + ipAddress);
      authenticate(username, password, ipAddress);
      request.setMsisdn(msisdn);
      this.logger.info(request.toString());
      
      ConfirmObj confirmObj = new ConfirmObj();
      confirmObj.setMsisdn(msisdn);
      confirmObj.setCommand("OFF");
      confirmObj.setActionType(2);
      confirmObj.setTransactionId(transactionId);
      
      boolean result = this.dbProcessor.insertConfirm(confirmObj);
      if (result)
      {
        MtRecord mtRecord = new MtRecord();
        mtRecord.setMsisdn(msisdn);
        String message = Commons.getInstance("PROCESS").getConfig("send_confirm_revoke", this.logger);
        mtRecord.setMessage(message);
        this.dbProcessor.insertMt(mtRecord);
        
        response.setErrCode("0");
        response.setDescription("Revoke Fly successful");
      }
      else
      {
        response.setErrCode("15");
        response.setDescription("Revoke Fly unsuccessful. There is one transaction need to be confirmed");
      }
      WsLog wsLog = new WsLog();
      wsLog.setFee(0.0D);
      wsLog.setIpClient(ipAddress);
      wsLog.setMsisdn(msisdn);
      wsLog.setRequest(request.toString());
      wsLog.setResponse(response.toString());
      wsLog.setResponseCode(response.getErrCode());
      wsLog.setRequestTime(new Timestamp(startTime));
      wsLog.setResponseTime(new Timestamp(System.currentTimeMillis()));
      wsLog.setType(1);
      wsLog.setUserId(userInfo.getId());
      wsLog.setChannel(userInfo.getChannel());
      this.dbProcessor.insertWsLog(wsLog);
    }
    catch (Exception ex)
    {
      this.logger.error("Exception ", ex);
      response.setErrCode("-9");
      response.setDescription("System error");
    }
    return response;
  }
}

