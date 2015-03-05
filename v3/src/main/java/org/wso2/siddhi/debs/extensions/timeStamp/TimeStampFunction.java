package org.wso2.siddhi.debs.extensions.timeStamp;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by sachini on 1/19/15.
 */

public class TimeStampFunction extends FunctionExecutor {
	private static final Logger logger = Logger.getLogger(TimeStampFunction.class);
    private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


    public Attribute.Type getReturnType() {
        return Attribute.Type.LONG;
    }

    //2013-01-02 08:36:00
    @Override
    protected void init(ExpressionExecutor[] expressionExecutors, ExecutionPlanContext executionPlanContext) {

    }

    @Override
    protected Object execute(Object[] objects) {
        return null;
    }

    @Override
    protected Object execute(Object o) {

        Date date = null;

        try {
            date = dateFormat.parse((String)o);
        } catch (ParseException e) {
        	logger.error(e.getMessage());
        }

    //+Miyuru : It would be better to get the time value directly from the Date object
    //        Calendar c = Calendar.getInstance();
    //        c.setTime(d);
    //        return c.getTimeInMillis();

        return date.getTime();
    }

    public void start() {

    }

    public void stop() {

    }

    public Object[] currentState() {
        return new Object[0];
    }

    public void restoreState(Object[] objects) {

    }
}
