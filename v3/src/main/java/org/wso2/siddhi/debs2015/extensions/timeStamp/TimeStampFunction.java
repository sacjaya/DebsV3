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
