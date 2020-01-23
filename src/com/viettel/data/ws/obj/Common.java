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
public class Common
{
  public static String getDescription(String errorCode)
  {
    String description = "";
    if ((errorCode.equals("0")) || (errorCode.equals("12"))) {
      return "Successful";
    }
    if (errorCode.equals("-9")) {
      return "System fail";
    }
    if (errorCode.equals("-7")) {
      return "System fail";
    }
    if (errorCode.equals("-5")) {
      return "Wrong param";
    }
    if (errorCode.equals("5")) {
      return "System fail";
    }
    if (errorCode.equals("6")) {
      return "Is exist family number";
    }
    if (errorCode.equals("7")) {
      return "Limit family number";
    }
    if (errorCode.equals("8")) {
      return "Wrong format family number";
    }
    if (errorCode.equals("9")) {
      return "Product code not allow use service";
    }
    if (errorCode.equals("10")) {
      return "System fail";
    }
    if (errorCode.equals("11")) {
      return "Balance not enough";
    }
    if (errorCode.equals("13")) {
      return "Family number not exist";
    }
    if (errorCode.equals("14")) {
      return "Subcriber not information";
    }
    return description;
  }
  
  public static class ErrorCode
  {
    public static final String DB_ERROR = "-7";
    public static final String WRONG_PARAM = "-5";
    public static final String WS_TIMEOUT = "-6";
    public static final String SUCCESS = "0";
    public static final String SYSTEM_FAIL = "-9";
    public static final String WRONG_SYNTAX = "-1";
    public static final String DUPLICATE_REQUEST = "-2";
    public static final String SERVICE_INACTIVE = "-3";
    public static final String OVERLOAD = "-4";
    public static final String DB_CM_ERROR = "5";
    public static final String IS_EXIST_FN = "6";
    public static final String MAX_FN = "7";
    public static final String FN_ERROR_SYNTAX = "8";
    public static final String NOT_PERMITTED_PRODUCT_CODE = "9";
    public static final String PRO_ERROR = "10";
    public static final String NOT_ENOUGH_MONEY_ADD = "11";
    public static final String SUCCESS_ADD = "12";
    public static final String IS_NOT_EXIST_FN = "13";
    public static final String NOT_INFO = "14";
    public static final String CONFIRM_EXIST = "15";
  }
}