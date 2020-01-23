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
import com.viettel.mmserver.base.ProcessThreadMX;
import com.viettel.utility.PropertiesUtils;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.ws.Endpoint;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import utils.Config;

public class WebserviceManager
  extends ProcessThreadMX
{
  private static WebserviceManager instance;
  private static List<WebserviceObject> listWS;
  public static HashMap<String, Integer> listWSname;
  private static List<Endpoint> listEndpoint;
  private String connectorsFile;
  public static String processClass;
  public static String appId;
  public static int maxRow;
  public static int queryDbTimeout;
  public static int breakQuery;
  public static int dbTimeOut;
  public static boolean exchangeEnable;
  public static boolean enableQueryDbTimeout;
  public static String pathDatabase;
  public static String pathExch;
  public static long[] timesDbLevel;
  public static long[] timesOcsLevel;
  public static long minTimeDb;
  public static long minTimeOcs;
  public static HashMap loggerDbMap;
  public static HashMap loggerOcsMap;
  private StringBuffer br = new StringBuffer();
  private boolean exception = false;
  
  public static WebserviceManager getInstance()
    throws Exception
  {
    if (instance == null) {
      instance = new WebserviceManager();
    }
    return instance;
  }
  
  public WebserviceManager()
    throws Exception
  {
    super("WebserviceManager");
    registerAgent("WebserviceFW:type=WebserviceManager");
    
    String config = Config.configDir + File.separator + "app.conf";
    FileReader fileReader = null;
    fileReader = new FileReader(config);
    Properties pro = new Properties();
    pro.load(fileReader);
    
    this.br.setLength(0);
    try
    {
      appId = pro.getProperty("APP_ID").toUpperCase();
    }
    catch (Exception ex)
    {
      this.br.append("APP_ID not found in app.conf\n");
      this.exception = true;
    }
    try
    {
      maxRow = Integer.parseInt(pro.getProperty("MAX_ROW"));
    }
    catch (Exception ex)
    {
      this.br.append("MAX_ROW not found in app.conf => Default value: 100\n");
      maxRow = 100;
    }
    try
    {
      exchangeEnable = Boolean.parseBoolean(pro.getProperty("EXCHANGE_ENABLE"));
    }
    catch (Exception ex)
    {
      this.br.append("EXCHANGE_ENABLE not found in app.conf => Default value: false\n");
      exchangeEnable = false;
    }
    if (exchangeEnable) {
      try
      {
        pathExch = pro.getProperty("PATH_EXCH");
      }
      catch (Exception ex)
      {
        this.br.append("PATH_EXCH not found in app.conf\n");
        this.exception = true;
      }
    }
    try
    {
      pathDatabase = pro.getProperty("PATH_DB");
    }
    catch (Exception ex)
    {
      this.br.append("PATH_DB not found in app.conf\n");
      this.exception = true;
    }
    try
    {
      enableQueryDbTimeout = Boolean.parseBoolean(pro.getProperty("ENABLE_QUERY_DB_TIMEOUT", "false"));
    }
    catch (Exception ex)
    {
      this.br.append("ENABLE_QUERY_DB_TIMEOUT not found in app.conf => Default value: ENABLE_QUERY_DB_TIMEOUT = FALSE\n");
      
      enableQueryDbTimeout = false;
    }
    if (enableQueryDbTimeout)
    {
      try
      {
        queryDbTimeout = Integer.parseInt(pro.getProperty("QUERY_DB_TIMEOUT"));
      }
      catch (Exception ex)
      {
        this.br.append("QUERY_DB_TIMEOUT not found in app.conf\n");
        this.exception = true;
      }
      try
      {
        breakQuery = Integer.parseInt(pro.getProperty("BREAK_QUERY")) * 1000;
      }
      catch (Exception ex)
      {
        this.br.append("BREAK_QUERY not found in app.conf\n");
        this.exception = true;
      }
    }
    try
    {
      dbTimeOut = Integer.parseInt(pro.getProperty("DB_TIME_OUT"));
    }
    catch (Exception ex)
    {
      this.br.append("DB_TIME_OUT not found in app.conf => Default value: 300\n");
      dbTimeOut = 300;
    }
    fileReader.close();
    
    loadLogLevelWarnning();
    this.connectorsFile = (Config.configDir + File.separator + "webservices.xml");
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db = dbf.newDocumentBuilder();
    Document dc = db.parse(this.connectorsFile);
    Element root = dc.getDocumentElement();
    
    NodeList list = root.getElementsByTagName("webservice");
    if (list.getLength() < 1) {
      throw new Exception("No webservice to publish");
    }
    listWS = new ArrayList();
    listWSname = new HashMap();
    for (int i = 0; i < list.getLength(); i++)
    {
      Element element = (Element)list.item(i);
      
      String name = element.getAttribute("name");
      if (listWSname.containsKey(name)) {
        throw new Exception("same webservice name: " + name);
      }
      WebserviceObject webserviceObject = new WebserviceObject();
      
      this.logger.info("===> get config for webservice: " + name);
      webserviceObject.setName(name);
      webserviceObject.setIp(element.getAttribute("ip"));
      webserviceObject.setPort(element.getAttribute("port"));
      webserviceObject.setPath(element.getAttribute("path"));
      webserviceObject.setImplementClass(element.getAttribute("implementClass"));
      webserviceObject.makeUrl();
      listWSname.put(name, Integer.valueOf(1));
      listWS.add(webserviceObject);
    }
  }
  
  protected void process()
  {
    try
    {
      Thread.sleep(10000L);
    }
    catch (InterruptedException ex) {}
  }
  
  public void start()
  {
    super.start();
    ClassLoader cl = new ClassLoader() {};
    listEndpoint = new ArrayList();
    for (WebserviceObject webserviceObject : listWS) {
      try
      {
        Class c = cl.loadClass(webserviceObject.getImplementClass());
        this.logger.info("===> Load class: " + c.getName());
        WebserviceAbstract webserviceAbstract = (WebserviceAbstract)c.newInstance();
        Endpoint service = Endpoint.publish(webserviceObject.getUrl(), webserviceAbstract);
        
        this.logger.info("Publish service " + webserviceObject.getName() + " success!");
        this.logger.info("URL: " + webserviceObject.getUrl() + "?wsdl");
        listEndpoint.add(service);
      }
      catch (Exception e)
      {
        this.logger.error("Publish service " + webserviceObject.getName() + " error!", e);
      }
    }
    this.logger.info("+++ SYSTEM PROCESS STARTED  +++");
  }
  
  public void stop()
  {
    for (Endpoint endpoint : listEndpoint) {
      try
      {
        endpoint.stop();
      }
      catch (Exception e)
      {
        this.logger.error("Stop endpoint " + endpoint.getClass().toString() + " error!", e);
      }
    }
    super.stop();
    this.logger.info("+++ SYSTEM PROCESS STOPPED  +++");
  }
  
  private void loadLogLevelWarnning()
    throws Exception
  {
    PropertiesUtils pros = new PropertiesUtils();
    pros.loadProperties("../etc/loglevel.conf", false);
    try
    {
      String[] dbTimes = pros.getProperty("DB_TIMES").split(",");
      String[] dbKey = pros.getProperty("DB_MESSAGE_KEY").split(",");
      
      loggerDbMap = new HashMap();
      timesDbLevel = new long[dbTimes.length];
      minTimeDb = Long.parseLong(dbTimes[0].trim());
      for (int i = 0; i < dbTimes.length; i++)
      {
        timesDbLevel[i] = Long.parseLong(dbTimes[i].trim());
        loggerDbMap.put(Integer.valueOf(i), dbKey[i].trim());
      }
    }
    catch (Exception ex)
    {
      this.logger.error("Loi lay thong tin DB_TIMES, DB_MESSAGE_KEY trong loglevel.conf");
      loggerDbMap = null;
      throw ex;
    }
    if (exchangeEnable) {
      try
      {
        String[] ocsTimes = pros.getProperty("OCS_TIMES").split(",");
        String[] ocsKey = pros.getProperty("OCS_MESSAGE_KEY").split(",");
        
        loggerOcsMap = new HashMap();
        timesOcsLevel = new long[ocsTimes.length];
        minTimeOcs = Long.parseLong(ocsTimes[0].trim());
        for (int i = 0; i < ocsTimes.length; i++)
        {
          timesOcsLevel[i] = Long.parseLong(ocsTimes[i].trim());
          loggerOcsMap.put(Integer.valueOf(i), ocsKey[i].trim());
        }
      }
      catch (Exception ex)
      {
        this.logger.error("Loi lay thong tin OCS_TIMES, OCS_MESSAGE_KEY trong loglevel.conf");
        loggerOcsMap = null;
        throw ex;
      }
    }
  }
  
  public static String getTimeLevelDb(long times)
  {
    if (loggerDbMap != null)
    {
      int key = Arrays.binarySearch(timesDbLevel, times);
      if (key < 0) {
        key = -key - 2;
      }
      String label = (String)loggerDbMap.get(Integer.valueOf(key));
      
      return label == null ? "-" : label;
    }
    return null;
  }
  
  public static String getTimeLevelOcs(long times)
  {
    if (loggerOcsMap != null)
    {
      int key = Arrays.binarySearch(timesOcsLevel, times);
      if (key < 0) {
        key = -key - 2;
      }
      String label = (String)loggerOcsMap.get(Integer.valueOf(key));
      return label == null ? "-" : label;
    }
    return null;
  }
}

