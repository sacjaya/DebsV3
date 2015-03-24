package org.wso2.siddhi.debs2015.extensions.median;

import com.google.common.collect.MinMaxPriorityQueue;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.selector.attribute.aggergator.AttributeAggregator;
import org.wso2.siddhi.query.api.definition.Attribute;

/**
* Created by sachini on 1/9/15.
*/
public class MedianAggregator extends AttributeAggregator {
    MinMaxPriorityQueue<Float> minHeap = MinMaxPriorityQueue.<Float>create();
    MinMaxPriorityQueue<Float> maxHeap = MinMaxPriorityQueue.<Float>create();
    int totalElements = 0;

    @Override
    protected void init(ExpressionExecutor[] expressionExecutors, ExecutionPlanContext executionPlanContext) {

    }

    public Attribute.Type getReturnType() {
        return Attribute.Type.FLOAT;
    }

    @Override
    public Object processAdd(Object obj) {
        Float  element = (Float) obj;
        if (totalElements % 2 == 0) {
            maxHeap.add(element);
            totalElements++;

            if (minHeap.isEmpty()) {
                return getMedian();
            }
            if (maxHeap.peekFirst() < minHeap.peekLast()) {
                float minValue = maxHeap.removeFirst();
                float maxValue = minHeap.removeLast();
                minHeap.add(minValue);
                maxHeap.add(maxValue);
            }
        } else {
            maxHeap.add(element);
            float minValue = maxHeap.removeFirst();
            minHeap.add(minValue);
            totalElements++;

        }

        return getMedian();
    }

    @Override
    public Object processAdd(Object[] objects) {
        Float  element = (Float) objects[0];
        if (totalElements % 2 == 0) {
            maxHeap.add(element);
            totalElements++;

            if (minHeap.isEmpty()) {
                return getMedian();
            }
            if (maxHeap.peekFirst() < minHeap.peekLast()) {
                float minValue = maxHeap.removeFirst();
                float maxValue = minHeap.removeLast();
                minHeap.add(minValue);
                maxHeap.add(maxValue);
            }
        } else {
            maxHeap.add(element);
            float minValue = maxHeap.removeFirst();
            minHeap.add(minValue);
            totalElements++;

        }

        return getMedian();
    }

    @Override
    public Object processRemove(Object obj) {
        Float  element = (Float) obj;

        if (totalElements % 2 == 0) {
            totalElements--;

            if (maxHeap.peekFirst() >= element) {
                maxHeap.remove();
                float value = minHeap.removeLast();
                maxHeap.add(value);
            } else {
                if (!minHeap.isEmpty()) {
                    minHeap.remove(element);
                }
            }
        } else {
            totalElements--;
            if (maxHeap.peekFirst() >= element) {
                maxHeap.remove();
            } else {
                float value = maxHeap.removeFirst();
                minHeap.add(value);
            }

        }


        return getMedian();
    }

    @Override
    public Object processRemove(Object[] objects) {
        Float  element = (Float) objects[0];

        if (totalElements % 2 == 0) {
            totalElements--;

            if (maxHeap.peekFirst() >= element) {
                maxHeap.remove();
                float value = minHeap.removeLast();
                maxHeap.add(value);
            } else {
                if (!minHeap.isEmpty()) {
                    minHeap.remove(element);
                }
            }
        } else {
            totalElements--;
            if (maxHeap.peekFirst() >= element) {
                maxHeap.remove();
            } else {
                float value = maxHeap.removeFirst();
                minHeap.add(value);
            }

        }


        return getMedian();
    }

    @Override
    public Object reset() {
        minHeap.clear();
        maxHeap.clear();
        totalElements = 0;
        return null;
    }


    public float getMedian() {
        if(totalElements == 0)
            return 0;
        if (totalElements % 2 == 0) {
            return (maxHeap.peekFirst() + minHeap.peekLast()) / 2;
        } else {
            return maxHeap.peekFirst();
        }
    }

    public void start() {

    }

    public void stop() {

    }

    public Object[] currentState() {
        return new Object[]{minHeap,maxHeap,totalElements};
    }

    public void restoreState(Object[] objects) {
       minHeap = (MinMaxPriorityQueue<Float>) objects[0];
       maxHeap = (MinMaxPriorityQueue<Float>) objects[1];
       totalElements = (Integer)objects[2];
    }

}
