/*
* Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org)
* All Rights Reserved.
*
* WSO2 Inc. licenses this file to you under the Apache License,
* Version 2.0 (the "License"); you may not use this file except
* in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package org.wso2.siddhi.debs2015.handlerChaining.processors.maxK;

import org.wso2.siddhi.debs2015.handlerChaining.DebsEvent;

import java.util.LinkedList;

public class MaxKQ1Processor {

    private static final String NULL_VALUE = "null";
    //The K value
    private int kValue = 10;
    private MaxKStoreQ1 maxKStore = new MaxKStoreQ1();


    public void process(DebsEvent debsEvent) {
        debsEvent.setTopK(processEventForMaxK(debsEvent.isCurrent(), debsEvent.getStartCellNo(), debsEvent.getEndCellNo()));
    }


    private Object[] processEventForMaxK(boolean isCurrent, int startCell, int endCell) {


        Object[] data = new Object[2 * kValue];


        int startCellValue = Math.round((float) ((startCell / 1000)) / 2) * 1000 + Math.round((float) ((startCell % 1000)) / 2);
        int endCellValue = Math.round((float) (endCell / 1000) / 2) * 1000 + Math.round((float) ((endCell % 1000)) / 2);


        LinkedList<Long> currentTopK;

        //The method getMaxK() accepts the "<start cell ID>:<end cell ID>" and the trip count found for this route.
        currentTopK = maxKStore.getMaxK(((long) (startCellValue)) * 1000000 + (long) endCellValue, isCurrent, kValue);

        if (currentTopK == null) {
            return null;
        } else {

            //From here onwards we prepare the output data tuple from this operator.
            int position = 0;

            //This will be restricted to to k number of lists. Therefore, we do not need to check whether we have exceeded the top k.
            //We do this until top-k is 10 (kValue==10)
            for (long cell : currentTopK) {
                //String[] splitValues = cell.split(":");
                //System.out.println("cell|"+cell+"|"+cell.substring(0, colonIndex)+"|"+cell.substring(colonIndex+1));
                long startCellIntValue = cell / 1000000;
                data[position++] = (startCellIntValue / 1000) + "." + (startCellIntValue % 1000);//profitable_cell_id_

                long endCellIntValue = cell % 1000000;
                data[position++] = (endCellIntValue / 1000) + "." + (endCellIntValue % 1000);//profitable_cell_id_
            }

            //Populating remaining elements for the payload of the stream with null if we could not find the top-k number of routes.
            while (position < (2 * kValue)) {
                data[position++] = NULL_VALUE;
                data[position++] = NULL_VALUE;
            }




            return data;
        }

    }


}
