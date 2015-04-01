package org.wso2.siddhi.debs2015.handlerChaining.processors;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;


public class TimeStampProcessor {

	private static final Calendar CachedCalendar = new GregorianCalendar();
	static {
	   CachedCalendar.setTimeZone(TimeZone.getTimeZone("GMT"));
	   CachedCalendar.clear();
	}
    


	public long execute(Object data) {
    	String dateTime = (String) data;
	   try {
	      int y = Integer.parseInt(dateTime.substring(0, 4));
	      int m = Integer.parseInt(dateTime.substring(5, 7));
	      --m;
	      int d = Integer.parseInt(dateTime.substring(8, 10));
	      int h = Integer.parseInt(dateTime.substring(11, 13));
	      int mm = Integer.parseInt(dateTime.substring(14, 16));
	      int s = Integer.parseInt(dateTime.substring(17));
		  
	 
	      CachedCalendar.set(y, m, d, h, mm, s);
	 
	      if (CachedCalendar.get(Calendar.YEAR) != y) {
	         return 0;
	      }
	      if (CachedCalendar.get(Calendar.MONTH) != m) {
	         return 0;
	      }
	      if (CachedCalendar.get(Calendar.DATE) != d) {
	         return 0;
	      }
	 
	      if (h < 0 || m > 23) {
	         return 0;
	      }
	      if (mm < 0 || mm > 59) {
	         return 0;
	      }
	      if (s < 0 || s > 59) {
	         return 0;
	      }
	      
	      return CachedCalendar.getTime().getTime();
	   } catch (Exception e) {
		   e.printStackTrace();
	      return 0;
	   }
    }
}
