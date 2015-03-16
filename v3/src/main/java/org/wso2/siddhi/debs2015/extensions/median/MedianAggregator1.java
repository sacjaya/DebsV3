package org.wso2.siddhi.debs2015.extensions.median;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.selector.attribute.aggergator.AttributeAggregator;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
* Created by sachini on 1/9/15.
*/
public class MedianAggregator1 extends AttributeAggregator{
    List<Float> values  = new ArrayList<Float>();

    @Override
    protected void init(ExpressionExecutor[] expressionExecutors, ExecutionPlanContext executionPlanContext) {

    }

    @Override
    public Attribute.Type getReturnType() {
        return Attribute.Type.FLOAT;
    }

    @Override
    public Object processAdd(Object o) {
        values.add((Float) o);
        return getMedian();
    }

    @Override
    public Object processAdd(Object[] objects) {
        values.add((Float) objects[0]);
        return getMedian();
    }

    @Override
    public Object processRemove(Object o) {
        values.remove((Float)o);
        return getMedian();
    }

    @Override
    public Object processRemove(Object[] objects) {
        values.remove((Float)objects[0]);
        return getMedian();
    }

    @Override
    public Object reset() {
        values.clear();
        return values;
    }

    public void start() {

    }

    public void stop() {

    }

    public Object[] currentState() {
        return new Object[]{values};
    }

    public void restoreState(Object[] objects) {
      values = (List<Float>) objects[0];
    }


//
//    public OutputAttributeAggregator newInstance() {
//        return new MedianAggregator1();
//    }

    private float getMedian(){
        Collections.sort(values);
        int size = values.size();
        if(size==0){
            return 0;
        }

        if(size==1){
            return values.get(0);
        } else if(size%2==1){
            return values.get(size/2);
        } else {
            return (values.get((size/2)-1)+values.get(size/2))/2;
        }

    }
}
