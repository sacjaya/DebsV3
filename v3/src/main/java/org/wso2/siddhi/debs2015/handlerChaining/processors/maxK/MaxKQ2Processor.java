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

import org.apache.log4j.Logger;
import org.wso2.siddhi.debs2015.handlerChaining.processors.ProfitObj;

import java.util.LinkedList;

/**
 * Created by sachini on 3/23/15.
 */
public class MaxKQ2Processor {

    private static final Logger LOGGER = Logger.getLogger(MaxKQ2Processor.class);
    private boolean debugEnabled = false;

    private static final String NULL_VALUE = "null";
    //The K value
    private int kValue = 10;


    private MaxKStoreQ2 maxKStore = new MaxKStoreQ2();


    public Object[] processEventForMaxK(ProfitObj profitObj, boolean isCurrent) {
        Object[] data = new Object[4 * kValue];

        //Map<Double, List<CustomObj>> currentTopK;
        LinkedList<ProfitObj> currentTopK;

        //The method getMaxK() accepts the "<start cell ID>:<end cell ID>" and the trip count found for this route.

        //The method getMaxK() accepts the "<start cell ID>:<end cell ID>" and the trip count found for this route.

        currentTopK = maxKStore.getMaxK(profitObj, isCurrent, kValue);


        if (currentTopK == null || currentTopK.size()<1) {
            return null;
        } else {

            //From here onwards we prepare the output data tuple from this operator.
            int position = 0;

            //This will be restricted to to k number of lists. Therefore, we do not need to check whether we have exceeded the top k.
            //We do this until top-k is 10 (kValue==10)
            for (ProfitObj customObj : currentTopK) {

                //We do this until top-k is 10 (kValue==10)
                //for (int i = cellList.size()-1 ; i >= 0 ; i--){
                //CustomObj customObj = cellList.get(i);
                int cellIntValue = customObj.getCellID();
                data[position++] = (cellIntValue / 1000) + "." + (cellIntValue % 1000);//profitable_cell_id_
                data[position++] = customObj.getEmptyTaxiCount();//empty_taxies_in_cell_id_
                data[position++] = customObj.getProfit();//median_profit_in_cell_id_
                data[position++] = customObj.getProfit_per_taxi();//profitability_of_cell_

            }

            //Populating remaining elements for the payload of the stream with null if we could not find the top-k number of routes.
            while (position < (4 * kValue)) {
                data[position++] = NULL_VALUE;
                data[position++] = NULL_VALUE;
                data[position++] = NULL_VALUE;
                data[position++] = NULL_VALUE;
            }

            if (debugEnabled) {
                LOGGER.debug("Latest Top-K elements with frequency" + data);
            }

            return data;
        }

    }


}
