/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.data.database;

/**
 *
 * @author olivier.tatsinkou
 */
import com.viettel.data.ws.obj.ConfirmObj;
import com.viettel.data.ws.obj.MtRecord;
import com.viettel.data.ws.obj.UserInfo;
import com.viettel.data.ws.obj.WsLog;
import com.viettel.smsfw.database.DbProcessorFW;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import org.apache.log4j.Logger;

public class DbProcessor
  extends DbProcessorFW
{
  private Logger logger;
  private String loggerLabel = DbProcessor.class.getSimpleName() + ": ";
  private StringBuilder br = new StringBuilder();
  private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
  private int breakQuery = 120000;
  private int dbQueryTimeout = 60;
  private int TIME_ERRORS = 3;
  private String SQL_GET_MO_ID = "select mo_seq.nextval from dual ";
  private static final String SQL_INSERT_WSLOG = "INSERT INTO ws_log (ws_log_id,user_id,ip_client,request_time,request_content,msisdn,request_type,response_time,response_content,response_code,request_fee,CHANNEL)  VALUES   (ws_log_seq.nextval,?,?,?,?,?,?,?,?,?,?,?)";
  private static final String SQL_GET_MO_FAKE = "SELECT COMMAND, PARAM, ACTION_TYPE, CHANNEL , SYNTAX FROM MO_FAKE ";
  private static final String SQL_GET_USER = "select id, username , password , ip_address, channel , status from ws_user ";
  private static final String SQL_INSERT_CONFIRM = "insert into confirm(id, msisdn, command,action_type, receive_time, transaction_id) values (confirm_seq.nextval, ? , ? ,?, sysdate,?) ";
  private static final String SQL_INSERT_MT = "insert into mt values (mt_seq.nextval, mt_seq.nextval, ?, ?, sysdate, 0, '865')";
  
  public DbProcessor(String dbName, Logger logger)
  {
    this.dbName = dbName;
    this.logger = logger;
    init(dbName, logger);
  }
  
  public HashMap getUser(String dbId)
  {
    HashMap mConfig = null;
    long timeBegin = System.currentTimeMillis();
    int count = 0;
    while (mConfig == null)
    {
      try
      {
        count++;
        mConfig = iGetUser(dbId);
      }
      catch (SQLException ex)
      {
        this.br.setLength(0);
        this.br.append(this.loggerLabel).append("ERROR - ").append(count).append("\n").append("select id, username , password , ip_address, channel , status from ws_user ");
        
        this.logger.error(this.br, ex);
      }
      catch (Exception ex)
      {
        this.br.setLength(0);
        this.br.append(this.loggerLabel).append(new Date()).append("\nERROR select WS_USER");
        

        this.logger.error(this.br, ex);
        break;
      }
      if (((mConfig == null) && (System.currentTimeMillis() - timeBegin > 60000L)) || (count < 3))
      {
        this.br.setLength(0);
        this.br.append(this.loggerLabel).append(new Date()).append("==> BREAK query select WS_USER\n");
        
        this.logger.error(this.br);
      }
    }
    return mConfig;
  }
  
  private HashMap iGetUser(String dbId)
    throws SQLException
  {
    ResultSet rs = null;
    PreparedStatement ps = null;
    Connection connection = null;
    HashMap mapUser = null;
    long startTime = System.currentTimeMillis();
    try
    {
      connection = getConnection(dbId);
      ps = connection.prepareStatement("select id, username , password , ip_address, channel , status from ws_user ");
      rs = ps.executeQuery();
      mapUser = new HashMap();
      while (rs.next())
      {
        UserInfo userInfo = new UserInfo();
        userInfo.setId(rs.getInt("ID"));
        userInfo.setIp(rs.getString("IP_ADDRESS"));
        userInfo.setChannel(rs.getString("CHANNEL"));
        userInfo.setUsername(rs.getString("USERNAME"));
        userInfo.setPassword(rs.getString("PASSWORD"));
        mapUser.put(userInfo.getUsername(), userInfo);
      }
    }
    catch (SQLException ex)
    {
      throw ex;
    }
    finally
    {
      closeResultSet(rs);
      closeStatement(ps);
      closeConnection(connection);
      logTime("select id, username , password , ip_address, channel , status from ws_user ", startTime);
    }
    return mapUser;
  }
  
  public long getMoId()
    throws SQLException
  {
    ResultSet rs = null;
    PreparedStatement ps = null;
    Connection connection = null;
    long moId = 0L;
    long startTime = System.currentTimeMillis();
    try
    {
      connection = getConnection(this.dbName);
      ps = connection.prepareStatement(this.SQL_GET_MO_ID);
      ps.setQueryTimeout(this.dbQueryTimeout);
      rs = ps.executeQuery();
      if (rs.next()) {
        moId = rs.getLong("NEXTVAL");
      }
      return moId;
    }
    catch (SQLException ex)
    {
      throw ex;
    }
    finally
    {
      closeResultSet(rs);
      closeStatement(ps);
      closeConnection(connection);
      logTime("Time to select mo_seq.nextval from dual", startTime);
    }
  }
  
  public int insertWsLog(WsLog ws)
  {
    int result = -1;
    PreparedStatement ps = null;
    Connection connection = null;
    long startTime = System.currentTimeMillis();
    try
    {
      connection = getConnection(this.dbName);
      
      ps = connection.prepareStatement("INSERT INTO ws_log (ws_log_id,user_id,ip_client,request_time,request_content,msisdn,request_type,response_time,response_content,response_code,request_fee,CHANNEL)  VALUES   (ws_log_seq.nextval,?,?,?,?,?,?,?,?,?,?,?)");
      ps.setQueryTimeout(this.dbQueryTimeout);
      ps.setLong(1, ws.getUserId());
      
      ps.setString(2, ws.getIpClient());
      
      ps.setTimestamp(3, ws.getRequestTime());
      
      ps.setString(4, ws.getRequest());
      
      ps.setString(5, ws.getMsisdn());
      
      ps.setInt(6, ws.getType());
      
      ps.setTimestamp(7, ws.getResponseTime());
      
      ps.setString(8, ws.getResponse());
      
      ps.setString(9, ws.getResponseCode());
      
      ps.setDouble(10, ws.getFee());
      

      ps.setString(11, ws.getChannel());
      
      ps.execute();
      result = 0;
    }
    catch (Exception ex)
    {
      this.logger.error("Exception insert into ws_log : ", ex);
    }
    finally
    {
      closeStatement(ps);
      closeConnection(connection);
      logTime("Time to insert into ws_log", startTime);
    }
    return result;
  }
  
  public boolean insertConfirm(ConfirmObj confirmObj)
  {
    boolean result = false;
    long timeBegin = System.currentTimeMillis();
    int count = 0;
    while (!result)
    {
      try
      {
        count++;
        result = iInsertConfirm(confirmObj);
      }
      catch (SQLException ex)
      {
        this.br.setLength(0);
        this.br.append(this.loggerLabel).append("ERROR - ").append(count).append("\n").append("insert into confirm(id, msisdn, command,action_type, receive_time, transaction_id) values (confirm_seq.nextval, ? , ? ,?, sysdate,?) ");
        
        this.logger.error(this.br, ex);
      }
      catch (Exception ex)
      {
        this.br.setLength(0);
        this.br.append(this.loggerLabel).append(new Date()).append("\nERROR insert CONFIRM ");
        

        this.logger.error(this.br, ex);
        break;
      }
      if (((!result) && (System.currentTimeMillis() - timeBegin > 60000L)) || (count < 3))
      {
        this.br.setLength(0);
        this.br.append(this.loggerLabel).append(new Date()).append("==> BREAK query insert CONFIRM \n");
        
        this.logger.error(this.br);
      }
    }
    return result;
  }
  
  private boolean iInsertConfirm(ConfirmObj confirmObj)
    throws SQLException
  {
    ResultSet rs = null;
    PreparedStatement ps = null;
    Connection connection = null;
    boolean result = false;
    long startTime = System.currentTimeMillis();
    try
    {
      connection = getConnection(this.dbName);
      ps = connection.prepareStatement("insert into confirm(id, msisdn, command,action_type, receive_time, transaction_id) values (confirm_seq.nextval, ? , ? ,?, sysdate,?) ");
      ps.setQueryTimeout(10);
      ps.setString(1, confirmObj.getMsisdn());
      ps.setString(2, confirmObj.getCommand());
      ps.setInt(3, confirmObj.getActionType());
      ps.setString(4, confirmObj.getTransactionId());
      ps.execute();
      result = true;
    }
    catch (SQLException ex)
    {
      throw ex;
    }
    finally
    {
      closeResultSet(rs);
      closeStatement(ps);
      closeConnection(connection);
      logTime("insert into confirm(id, msisdn, command,action_type, receive_time, transaction_id) values (confirm_seq.nextval, ? , ? ,?, sysdate,?) ", startTime);
    }
    return result;
  }
  
  public boolean insertMt(MtRecord mtRecord)
  {
    boolean result = false;
    long timeBegin = System.currentTimeMillis();
    int count = 0;
    while (!result)
    {
      try
      {
        count++;
        result = iInsertMt(mtRecord);
      }
      catch (SQLException ex)
      {
        this.br.setLength(0);
        this.br.append(this.loggerLabel).append("ERROR - ").append(count).append("\n").append("insert into mt values (mt_seq.nextval, mt_seq.nextval, ?, ?, sysdate, 0, '865')");
        
        this.logger.error(this.br, ex);
      }
      catch (Exception ex)
      {
        this.br.setLength(0);
        this.br.append(this.loggerLabel).append(new Date()).append("\nERROR insert MT ");
        

        this.logger.error(this.br, ex);
        break;
      }
      if (((!result) && (System.currentTimeMillis() - timeBegin > 60000L)) || (count < 3))
      {
        this.br.setLength(0);
        this.br.append(this.loggerLabel).append(new Date()).append("==> BREAK query insert MT \n");
        
        this.logger.error(this.br);
      }
    }
    return result;
  }
  
  private boolean iInsertMt(MtRecord mtRecord)
    throws SQLException
  {
    ResultSet rs = null;
    PreparedStatement ps = null;
    Connection connection = null;
    boolean result = false;
    long startTime = System.currentTimeMillis();
    try
    {
      connection = getConnection(this.dbName);
      ps = connection.prepareStatement("insert into mt values (mt_seq.nextval, mt_seq.nextval, ?, ?, sysdate, 0, '865')");
      ps.setString(1, mtRecord.getMsisdn());
      ps.setString(2, mtRecord.getMessage());
      ps.execute();
      result = true;
    }
    catch (SQLException ex)
    {
      throw ex;
    }
    finally
    {
      closeResultSet(rs);
      closeStatement(ps);
      closeConnection(connection);
      logTime("insert into mt values (mt_seq.nextval, mt_seq.nextval, ?, ?, sysdate, 0, '865')", startTime);
    }
    return result;
  }
}
