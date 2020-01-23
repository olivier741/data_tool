/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.viettel.data.utils;

/**
 *
 * @author olivier.tatsinkou
 */
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Encrypt
{
  private static String convertToHex(byte[] data)
  {
    StringBuffer buf = new StringBuffer();
    for (int i = 0; i < data.length; i++)
    {
      int halfbyte = data[i] >>> 4 & 0xF;
      int two_halfs = 0;
      do
      {
        if ((0 <= halfbyte) && (halfbyte <= 9)) {
          buf.append((char)(48 + halfbyte));
        } else {
          buf.append((char)(97 + (halfbyte - 10)));
        }
        halfbyte = data[i] & 0xF;
      } while (two_halfs++ < 1);
    }
    return buf.toString();
  }
  
  public static String MD5(String text)
    throws NoSuchAlgorithmException, UnsupportedEncodingException
  {
    MessageDigest md = MessageDigest.getInstance("MD5");
    byte[] md5hash = new byte[32];
    md.update(text.getBytes("iso-8859-1"), 0, text.length());
    md5hash = md.digest();
    return convertToHex(md5hash);
  }
}

