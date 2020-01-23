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
public class Stop
{
  public static void main(String[] a)
    throws Exception
  {
    ThreadManager.getInstance().stop();
  }
}

