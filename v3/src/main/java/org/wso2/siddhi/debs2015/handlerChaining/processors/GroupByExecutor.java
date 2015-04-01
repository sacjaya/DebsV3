package org.wso2.siddhi.debs2015.handlerChaining.processors;

import java.util.HashMap;
import java.util.Map;

public class GroupByExecutor {

    protected Map<Integer, BucketingBasedMedianAggregateProcessor> aggregatorMap = new HashMap<Integer, BucketingBasedMedianAggregateProcessor>();

    public float execute(int groupByKey ,float value, boolean current) {
        BucketingBasedMedianAggregateProcessor currentAttributeAggregator = aggregatorMap.get(groupByKey);
        if (currentAttributeAggregator == null) {
            currentAttributeAggregator = new BucketingBasedMedianAggregateProcessor();
            aggregatorMap.put(groupByKey, currentAttributeAggregator);
        }

        if(current){
            return currentAttributeAggregator.processAdd(value);
        } else {
            return currentAttributeAggregator.processRemove(value);
        }
    }


}
