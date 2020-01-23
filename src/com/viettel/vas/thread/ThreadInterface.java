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
import com.viettel.mmserver.base.ProcessThreadMX;
import java.util.Date;

public abstract class ThreadInterface
  extends ProcessThreadMX
{
  protected int order;
  protected int numMembers;
  
  public ThreadInterface(String threadName)
  {
    super(threadName);
  }
  
  protected void config(int order, int numMemebers)
    throws Exception
  {
    this.order = order;
    this.numMembers = numMemebers;
    
    registerAgent(ThreadManager.appId + ":type=ThreadManager,name=" + this.threadName + "-" + order);
  }
  
  protected abstract void processThread();
  
  protected void process()
  {
    this.buStartTime = new Date();
    processThread();
    this.buStartTime = new Date();
  }
}