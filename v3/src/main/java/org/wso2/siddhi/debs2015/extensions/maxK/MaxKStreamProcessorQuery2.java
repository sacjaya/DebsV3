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

package org.wso2.siddhi.debs2015.extensions.maxK;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.debs2015.extensions.maxK.util.CustomObj;
import org.wso2.siddhi.debs2015.extensions.maxK.util.MaxKStoreForStreamProcessorQuery2;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
* Created by sachini on 3/23/15.
*/
public class MaxKStreamProcessorQuery2 extends StreamProcessor {

    private static final Logger LOGGER = Logger.getLogger(MaxKStreamProcessorQuery2.class);
    private boolean debugEnabled = false;

    private static final String NULL_VALUE = "null";
    //The K value
    private int kValue = 0;


    private MaxKStoreForStreamProcessorQuery2 maxKStore = null;

    @Override
    protected List<Attribute> init(AbstractDefinition abstractDefinition, ExpressionExecutor[] expressionExecutors, ExecutionPlanContext executionPlanContext) {

        kValue = (Integer)((ConstantExpressionExecutor) expressionExecutors[4]).getValue();

        List<Attribute> attributeList = new ArrayList<Attribute>();



        for (int i = 1; i <= kValue; i++) {
            attributeList.add(new Attribute("profitable_cell_id_" + i , Attribute.Type.STRING));
            attributeList.add(new Attribute("empty_taxies_in_cell_id_" + i , Attribute.Type.FLOAT));
            attributeList.add(new Attribute("median_profit_in_cell_id_" + i , Attribute.Type.FLOAT));
            attributeList.add(new Attribute("profitability_of_cell_" + i , Attribute.Type.FLOAT));
        }

        maxKStore = new MaxKStoreForStreamProcessorQuery2();

        return attributeList;
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor, StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        while (streamEventChunk.hasNext()) {
            ComplexEvent complexEvent = streamEventChunk.next();

            Object[] inputData = new Object[attributeExpressionLength+1];
            inputData[0] = complexEvent.getType() == ComplexEvent.Type.CURRENT;
            for (int i = 1; i < attributeExpressionLength+1; i++) {
                inputData[i] = attributeExpressionExecutors[i - 1].execute(complexEvent);
            }

                Object[] data = processEventForMaxK(inputData);

            if(data == null){
                streamEventChunk.remove();
            } else {
                complexEventPopulater.populateComplexEvent(complexEvent, data);
            }
        }
        nextProcessor.process(streamEventChunk);
    }


    private Object[] processEventForMaxK(Object[] object) {
        Object[] data =  new Object[4 * kValue+1];

        boolean isCurrent = (Boolean)object[0];
        float eventKeyValue = (Float) object[1];//This is the profit_per_taxi
        Object profitValue = object[2];
        Object emptyTaxiCountValue = object[3];
        int cellValue = (Integer)object[4];

        //Map<Double, List<CustomObj>> currentTopK;
        LinkedList<CustomObj> currentTopK;

        //The method getMaxK() accepts the "<start cell ID>:<end cell ID>" and the trip count found for this route.

        //The method getMaxK() accepts the "<start cell ID>:<end cell ID>" and the trip count found for this route.

        currentTopK = maxKStore.getMaxK(new CustomObj(cellValue,eventKeyValue,profitValue,emptyTaxiCountValue), isCurrent,kValue);


        if(currentTopK == null ){
            return null;
        }  else {

            //From here onwards we prepare the output data tuple from this operator.
            int position = 0;

            //This will be restricted to to k number of lists. Therefore, we do not need to check whether we have exceeded the top k.
            //We do this until top-k is 10 (kValue==10)
            for (CustomObj customObj : currentTopK) {

                //We do this until top-k is 10 (kValue==10)
                //for (int i = cellList.size()-1 ; i >= 0 ; i--){
                //CustomObj customObj = cellList.get(i);
                int cellIntValue = customObj.getCellID();
                data[position++] =(cellIntValue/1000)+"."+(cellIntValue%1000);//profitable_cell_id_
                data[position++] = customObj.getEmptyTaxiCount();//empty_taxies_in_cell_id_
                data[position++] = customObj.getProfit();//median_profit_in_cell_id_
                data[position++] = customObj.getProfit_per_taxi();//profitability_of_cell_

            }

            //Populating remaining elements for the payload of the stream with null if we could not find the top-k number of routes.
            while (position < (4 * kValue)) {
                data[position++] = "null";
                data[position++] = "null";
                data[position++] = "null";
                data[position++] = "null";
            }

            long timeDifference = System.currentTimeMillis() - (Long) object[6];
            data[position++] = timeDifference;

            if (debugEnabled) {
                LOGGER.debug("Latest Top-K elements with frequency" + data);
            }

            return data;
        }

    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public Object[] currentState() {
        return new Object[]{kValue,maxKStore};
    }

    @Override
    public void restoreState(Object[] objects) {
        if ((objects.length == 2) &&
                (objects[0] instanceof Integer) &&
                (objects[1] instanceof MaxKStoreForStreamProcessorQuery2) ) {

            this.kValue = (Integer) objects[0];
            this.maxKStore = (MaxKStoreForStreamProcessorQuery2) objects[1];

        } else {
            //LOGGER.error("Failed in restoring the Max-K Transformer.");
            System.out.println("Failed in restoring the Max-K Transformer.");
        }
    }
}