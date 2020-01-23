/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author olivier.tatsinkou
 */
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class Test
{
  public static void main(String[] a)
    throws Exception
  {
    Calendar cal = Calendar.getInstance();
    SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy");
    cal.setTime(sdf.parse("28-02-2014"));
    
    cal.add(2, -1);
    System.out.println(sdf.format(cal.getTime()));
  }
}
