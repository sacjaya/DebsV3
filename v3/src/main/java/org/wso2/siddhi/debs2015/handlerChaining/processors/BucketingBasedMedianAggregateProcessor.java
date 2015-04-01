package org.wso2.siddhi.debs2015.handlerChaining.processors;

/**
* Created by sachini on 1/9/15.
*/
public class BucketingBasedMedianAggregateProcessor {
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


    public float processAdd(float element) {
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


    public float processRemove(float element) {

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



}
