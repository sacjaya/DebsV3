package org.wso2.siddhi.debs2015.extensions.median;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.selector.attribute.aggergator.AttributeAggregator;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.Attribute.Type;

/**
* Created by sachini on 1/9/15.
*/
public class BucketingBasedMedianAggregator extends AttributeAggregator {	
    int multiplexer =1;
    int size = 3016 * multiplexer;
    int[] mediationArray = new int[size];
    int totalElements = 0;

    float lastReturnedMedian = 0;

    public float getMedian() {

        if (totalElements % 2 == 0) {
            int firstMedianIndex = ((totalElements) / 2);
            int secondMedianIndex = ((totalElements) / 2) + 1;

            int firstMedianValue = 0;
            int secondMedianValue = 0;
            boolean flag = true;

            int count = 0;
            int loopCount = 0;
            for (int occurrenceCount : mediationArray) {
                count = count + occurrenceCount;

                if (firstMedianIndex <= count && flag) {
                    firstMedianValue = loopCount;
                    flag = false;
                    loopCount++;
                    continue;
                }
                if (secondMedianIndex <= count) {
                    secondMedianValue = loopCount;
                    break;
                }
                loopCount++;
            }
            lastReturnedMedian =  (firstMedianValue + secondMedianValue) / 2f;


        } else {
            int medianIndex = ((totalElements - 1) / 2) + 1;
            int count = 0;
            int medianValue = 0;
            int loopCount = 0;
            for (int medianCount : mediationArray) {
                count = count + medianCount;
                if (medianIndex <= count) {
                    medianValue = loopCount;
                    break;
                }
                loopCount++;
            }
            lastReturnedMedian =  medianValue/multiplexer;
        }

        return lastReturnedMedian;
    }

	public void start() {
    }

	public void stop() {
    }

	public Object[] currentState() {
		return new Object[]{totalElements};
    }

	public void restoreState(Object[] state) {
		totalElements = (Integer)state[0];	    
    }

	@Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors,
                        ExecutionPlanContext executionPlanContext) {
    }

	@Override
    public Type getReturnType() {
		return Attribute.Type.FLOAT;
    }

	@Override
    public Object processAdd(Object data) {
		Float  element = (Float) data;

		 if(element<0){
              return lastReturnedMedian;
         } else {

             int roundValue = Math.round(element * multiplexer);
             if (roundValue < size - 1) {
                 mediationArray[roundValue] += 1;
             } else {
                 mediationArray[size - 1] += 1;
             }
             totalElements++;

             return getMedian();
         }
    }
	
//    public void addElement(float element) {
//        int roundValue = Math.round(element*multiplexer);
//        if (roundValue < size - 1) {
//            mediationArray[roundValue] += 1;
//        } else {
//            mediationArray[size - 1] += 1;
//        }
//        totalElements++;
//
//    }

	@Override
    public Object processAdd(Object[] data) {
		Float  element = (Float) data[0];

        if(element<0){
            return lastReturnedMedian;
        } else {
            int roundValue = Math.round(element * multiplexer);
            if (roundValue < size - 1) {
                mediationArray[roundValue] += 1;
            } else {
                mediationArray[size - 1] += 1;
            }
            totalElements++;

            return getMedian();
        }
    }

	@Override
    public Object processRemove(Object data) {
		Float  element = (Float) data;

        if(element<0){
            return lastReturnedMedian;
        } else {
            int roundValue = Math.round(element * multiplexer);
            if (roundValue < size - 1) {
                mediationArray[roundValue] -= 1;
            } else {
                mediationArray[size - 1] -= 1;
            }
            totalElements--;

            return getMedian();
        }
    }
	
//    public void removeElement(float element) {
//        int roundValue = Math.round(element*multiplexer);
//        if (roundValue < size - 1) {
//            mediationArray[roundValue] -= 1;
//        } else {
//            mediationArray[size - 1] -= 1;
//        }
//        totalElements--;
//    }

	@Override
    public Object processRemove(Object[] data) {
		Float  element = (Float) data[0];

        if(element<0){
            return lastReturnedMedian;
        } else {
            int roundValue = Math.round(element * multiplexer);
            if (roundValue < size - 1) {
                mediationArray[roundValue] -= 1;
            } else {
                mediationArray[size - 1] -= 1;
            }
            totalElements--;

            return getMedian();
        }
    }

	@Override
    public Object reset() {
		totalElements = 0;
		
	    return null;
    }
	
}