package org.wso2.siddhi.debs2015.extensions.timeStamp;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

/**
 * Created by sachini on 1/19/15.
 */

public class TimeStampFunction extends FunctionExecutor {
	private static final Logger logger = Logger.getLogger(TimeStampFunction.class);
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    //2013-01-02 08:36:00
    public void init(Attribute.Type[] attributeTypes, SiddhiContext siddhiContext) {

    }
/*
    @Override
    protected Object process(Object data) {
    	//+Miyuru : We can directly cast the object to String while parsing
//        String dateAndTime = (String) data;
//
        Date d = null;
        
        try {
//            d = sdf.parse(dateAndTime);
            
            d = sdf.parse((String)data);
        } catch (ParseException e) {
        	logger.error(e.getMessage());
        }

//+Miyuru : It would be better to get the time value directly from the Date object
//        Calendar c = Calendar.getInstance();
//        c.setTime(d);
//        return c.getTimeInMillis();
        
        return d.getTime();
    }*/
    
    protected Object process(Object data) {
    	return null;
    }

	private static final Calendar CachedCalendar = new GregorianCalendar();
	static {
	   CachedCalendar.setTimeZone(TimeZone.getTimeZone("GMT"));
	   CachedCalendar.clear();
	}
    
    public void destroy() {
    }

    public Attribute.Type getReturnType() {
        return Attribute.Type.LONG;
    }

	public void start() {
    }

	public void stop() {
    }

	public Object[] currentState() {
	    return null;
    }

	public void restoreState(Object[] state) {
    }

	@Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors,
                        ExecutionPlanContext executionPlanContext) {
    }

	@Override
    protected Object execute(Object[] data) {
		
		System.out.println("len:" + data.length);
		
		return null;
    }

	@Override
    protected Object execute(Object data) {
    	//String dateTime = "2013-12-21 22:34:23";//(String) data;
    	String dateTime = (String) data;
//    	System.out.println("-------------------------->"+dateTime);
    	//System.out.println("dateTime:" + dateTime);
		//String[] items = dateTime.split(" "); //Input is like '2013-01-01 00:04:00'
		//String date = dateTime.indexOf(str)
		   try {
		      int y = Integer.parseInt(dateTime.substring(0, 4));
		      int m = Integer.parseInt(dateTime.substring(5, 7));
		      --m;
		      int d = Integer.parseInt(dateTime.substring(8, 10));
		      int h = Integer.parseInt(dateTime.substring(11, 13));
		      int mm = Integer.parseInt(dateTime.substring(14, 16));
		      int s = Integer.parseInt(dateTime.substring(17));
			  
//		      System.out.println("y:" + y);
//		      System.out.println("m:" + m);
//		      System.out.println("d:" + d);
//		      System.out.println("h:" + h);
//		      System.out.println("mm:" + mm);
//		      System.out.println("s:" + s);
		      //System.exit(0);
		      
//		      int y = Integer.parseInt(items[0].substring(0, 4));
//		      int m = Integer.parseInt(items[0].substring(5, 7));
//		      --m;
//		      int d = Integer.parseInt(items[0].substring(8, 10));
//		      int h = Integer.parseInt(items[1].substring(0, 2));
//		      int mm = Integer.parseInt(items[1].substring(3, 5));
//		      int s = Integer.parseInt(items[1].substring(6, 6));
			   
//			  String[] dateArr = items[0].split("-");
//			  String[] timeArr = items[1].split(":");
////			  System.out.println(dateArr[2]);
//		      int y = Integer.parseInt(dateArr[0]);
//		      int m = Integer.parseInt(dateArr[1]);
//		      --m;
//		      int d = Integer.parseInt(dateArr[2]);
//		      int h = Integer.parseInt(timeArr[0]);
//		      int mm = Integer.parseInt(timeArr[1]);
//		      int s = Integer.parseInt(timeArr[2]);
		 
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
