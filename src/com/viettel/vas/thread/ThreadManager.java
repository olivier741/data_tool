/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.vas.thread;

/**
 *
 * @author olivier.tatsinkou
 */


import com.viettel.mmserver.base.ProcessManager;
import com.viettel.mmserver.base.ProcessThread;
import com.viettel.mmserver.base.ProcessThreadMX;
import com.viettel.smsfw.utils.ViettelException;
import com.viettel.utility.PropertiesUtils;
import com.viettel.vas.util.ConnectionPoolManager;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;
import utils.Config;

public class ThreadManager  extends ProcessThreadMX
{
  private static ThreadManager instance;
  public static String appId;
  public static String ipAddress;
  private List<Integer> listId = new ArrayList();
  
  public ThreadManager()
    throws Exception
  {
    super(ThreadManager.class.getSimpleName());
    PropertiesUtils pro = new PropertiesUtils();
    pro.loadProperties(Config.configDir + File.separator + "app.conf", false);
    appId = pro.getProperty("APP_ID");
    String loadC3p0 = pro.getProperty("C3P0");
    registerAgent(appId + ":type=ThreadManager");
    
    ipAddress = System.getProperty("com.viettel.mmserver.agent.ip");
    if ((ipAddress == null) || (ipAddress.trim().length() == 0)) {
      ipAddress = "127.0.0.1";
    }
    if ((loadC3p0 != null) && (!loadC3p0.equals("false"))) {
      try
      {
        loadDb();
      }
      catch (ClassNotFoundException ex)
      {
        this.logger.error("Not using c3p0");
      }
      catch (Exception ex)
      {
        this.logger.error("Not load database for c3p0 in database.xml");
      }
    }
    pro.loadProperties(Config.configDir + File.separator + "threads.conf", false);
    int i = 1;
    ClassLoader cl = new ClassLoader() {};
    String className = "";
    for (;;)
    {
      className = pro.getProperty("thread.class." + i);
      if ((className == null) || (className.trim().length() == 0)) {
        break;
      }
      String insStr = pro.getProperty("thread.instance." + i);
      if ((insStr == null) || (insStr.trim().length() == 0)) {
        insStr = "1";
      }
      int instances = Integer.parseInt(insStr);
      for (int j = 0; j < instances; j++) {
        try
        {
          Class c = cl.loadClass(className);
          ThreadInterface th = (ThreadInterface)c.newInstance();
          th.config(j, instances);
          this.listId.add(th.getId());
        }
        catch (Exception ex)
        {
          this.logger.error("Error init Thread", ex);
          throw new ViettelException("Error init Thread");
        }
      }
      i++;
    }
    if (this.listId.isEmpty()) {
      throw new ViettelException("No process config or init thread error ==> Stop thread");
    }
  }
  
  public void loadDb()
    throws Exception
  {
    ConnectionPoolManager.loadConfig(Config.configDir + File.separator + "database.xml");
  }
  
  public static ThreadManager getInstance()
    throws Exception
  {
    if (instance == null) {
      instance = new ThreadManager();
    }
    return instance;
  }
  
  public void start()
  {
    for (int i = 0; i < this.listId.size(); i++) {
      ProcessManager.getInstance().getMmProcess((Integer)this.listId.get(i)).start();
    }
    super.start();
    this.logger.info("START THREAD SUCCESS");
  }
  
  public void stop()
  {
    for (int i = 0; i < this.listId.size(); i++) {
      ProcessManager.getInstance().getMmProcess((Integer)this.listId.get(i)).stop();
    }
    super.stop();
    this.logger.info("STOP THREAD SUCCESS");
  }
  
  protected void process()
  {
    try
    {
      Thread.sleep(100000L);
    }
    catch (Exception ex) {}
  }
}
