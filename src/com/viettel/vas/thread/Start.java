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
public class Start
{
  public static void main(String[] a)
    throws Exception
  {
    String configDir = System.getProperty("com.viettel.vas.config.path");
    if ((configDir != null) && (configDir.trim().length() > 0))
    {
      configDir = configDir.trim();
      utils.Config.configDir = (configDir.endsWith("/")) || (configDir.endsWith("\\")) ? configDir.substring(0, configDir.length() - 1) : configDir;
    }
    ThreadManager.getInstance().start();
  }
}
