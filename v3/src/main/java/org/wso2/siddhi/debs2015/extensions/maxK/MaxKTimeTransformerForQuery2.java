/*
*  Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
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
import org.wso2.siddhi.core.exception.ExecutionPlanCreationException;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.processor.stream.function.StreamFunctionProcessor;
import org.wso2.siddhi.debs2015.extensions.maxK.util.CustomObj;
import org.wso2.siddhi.debs2015.extensions.maxK.util.MaxKStoreQuery2;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.debs2015.extensions.maxK.util.MaxKStoreCopy;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


public class MaxKTimeTransformerForQuery2 extends StreamFunctionProcessor {

    private static final Logger LOGGER = Logger.getLogger(MaxKTimeTransformerForQuery2.class);
    private boolean debugEnabled = false;

    private String value = "";
    private String profit = "";
    private String emptyTaxiCount = "";
    private String cellNo = "";
    private String iijTimeStamp = "";
    //The desired attribute position of value in input stream
    private int valuePosition = 0;
    private int cellPosition = 0;
    private int iijTimeStampPosition = 0;
    private int profitPosition = 0;
    private int emptyTaxiCountPosition = 0;

    //The K value
    private int kValue = 0;

    //An array of Objects to manipulate output stream elements
    private Object[] data = null;
    private Object[] previousData = null;
    private boolean duplicate =true;


    private MaxKStoreQuery2 maxKStoreQuery2 = null;

    @Override
    protected Object[] process(Object[] objects) {
        processEventForMaxK(objects);
        return data;
    }

    @Override
    protected Object[] process(Object o) {
        throw new IllegalStateException("pol2Cart cannot execute for single data " + o);
    }



//    pickup_datetime, dropoff_datetime, profitable_cell_id_1, empty_taxies_in_cell_id_1, median_profit_in_cell_id_1, profitability_of_cell_1, ... ,
//    profitable_cell_id_10, empty_taxies_in_cell_id_10, median_profit_in_cell_id_10, profitability_of_cell_10, delay
    @Override
    protected List<Attribute> init(AbstractDefinition abstractDefinition, ExpressionExecutor[] expressionExecutors, ExecutionPlanContext executionPlanContext) {
        if (expressionExecutors.length != 6) {
            LOGGER.error("Required Parameters : Six");
            throw new ExecutionPlanCreationException("Mismatching Parameter count.");
        }

        //Getting all the parameters and assign those to instance variables
        //profit_per_taxi, profit, emptyTaxiCount, cellNo,10, iij_timestamp


        value = ((VariableExpressionExecutor) expressionExecutors[0]).getAttribute().getName();
        profit = ((VariableExpressionExecutor) expressionExecutors[1]).getAttribute().getName();
        emptyTaxiCount = ((VariableExpressionExecutor) expressionExecutors[2]).getAttribute().getName();
        cellNo = ((VariableExpressionExecutor) expressionExecutors[3]).getAttribute().getName();
        kValue = (Integer)((ConstantExpressionExecutor) expressionExecutors[4]).getValue();

        iijTimeStamp = ((VariableExpressionExecutor) expressionExecutors[5]).getAttribute().getName();

//        System.out.println("iijTimeStamp : " + iijTimeStamp);
//        System.exit(0);

        valuePosition = abstractDefinition.getAttributePosition(value);
        profitPosition = abstractDefinition.getAttributePosition(profit);
        emptyTaxiCountPosition = abstractDefinition.getAttributePosition(emptyTaxiCount);
        cellPosition = abstractDefinition.getAttributePosition(cellNo);
        iijTimeStampPosition = abstractDefinition.getAttributePosition(iijTimeStamp);

        List<Attribute> attributeList = new ArrayList<Attribute>();



        for (int i = 1; i <= kValue; i++) {
            attributeList.add(new Attribute("profitable_cell_id_" + i , Attribute.Type.STRING));
            attributeList.add(new Attribute("empty_taxies_in_cell_id_" + i , Attribute.Type.FLOAT));
            attributeList.add(new Attribute("median_profit_in_cell_id_" + i , Attribute.Type.FLOAT));
            attributeList.add(new Attribute("profitability_of_cell_" + i , Attribute.Type.FLOAT));
        }


        //Finally, we add a flag that indicates whether this is a duplicate event or not.
        attributeList.add(new Attribute("duplicate", Attribute.Type.BOOL));

        data = new Object[4 * kValue+1]; //This will be (4*10 + 1)=41
        previousData = new Object[kValue];
        for(int i=0;i<kValue;i++) {
            previousData[i]= "null";
        }

        maxKStoreQuery2 = new MaxKStoreQuery2();

        return attributeList;
    }

    public void start() {

    }

    public void stop() {

    }

    public Object[] currentState() {
        return new Object[]{value,valuePosition, profit, profitPosition, emptyTaxiCount,emptyTaxiCountPosition, cellNo, cellPosition,kValue, maxKStoreQuery2};
    }

    public void restoreState(Object[] objects) {
        if ((objects.length == 8) &&
                (objects[0] instanceof String) && (objects[1] instanceof Integer) &&
                (objects[2] instanceof String) && (objects[3] instanceof Integer) &&
                (objects[4] instanceof String) && (objects[5] instanceof Integer) &&
                (objects[6] instanceof Integer) &&
                (objects[7] instanceof MaxKStoreCopy) ) {
            //tripCount, startCellNo, endCellNo, 10, iij_timestamp
            this.value = (String) objects[0]; //value corresponds to trip count
            this.valuePosition = (Integer) objects[1];

            this.profit = (String)objects[2];
            this.profitPosition = (Integer)objects[3];

            this.emptyTaxiCount = (String) objects[4];
            this.emptyTaxiCountPosition = (Integer) objects[5];

            this.cellNo = (String) objects[6];
            this.cellPosition = (Integer) objects[7];


            this.kValue = (Integer) objects[8];
            this.maxKStoreQuery2 = (MaxKStoreQuery2) objects[9];

        } else {
            //LOGGER.error("Failed in restoring the Max-K Transformer.");
            System.out.println("Failed in restoring the Max-K Transformer.");
        }
    }

    private void processEventForMaxK(Object[] object) {

        // //profit_per_taxi, profit, emptyTaxiCount, cellNo,10, iij_timestamp

        Double eventKeyValue = (Double) object[0];//This is the profit_per_taxi
        Object profitValue = object[1];
        Object emptyTaxiCountValue = object[2];
        String cellValue = (String) object[3];

        //Map<Double, List<CustomObj>> currentTopK;
        LinkedList<CustomObj> currentTopK;

        //The method getMaxK() accepts the "<start cell ID>:<end cell ID>" and the trip count found for this route.

        currentTopK = maxKStoreQuery2.getMaxK(new CustomObj(cellValue,eventKeyValue,profitValue,emptyTaxiCountValue), kValue);

        //From here onwards we prepare the output data tuple from this operator.
        int position = 0;

        for(CustomObj customObj: currentTopK){
            //We do this until top-k is 10 (kValue==10)
            //for (int i = cellList.size()-1 ; i >= 0 ; i--){
                //CustomObj customObj = cellList.get(i);
                data[position++] = customObj.getCellID();//profitable_cell_id_
                data[position++] = customObj.getEmptyTaxiCount();//empty_taxies_in_cell_id_
                data[position++] = customObj.getProfit();//median_profit_in_cell_id_
                data[position++] = customObj.getProfit_per_taxi();//profitability_of_cell_
                
                if(position>=kValue*4){
                    break;
                }
            //}

            if(position>=kValue*4){
                break;
            }
        }

        //Populating remaining elements for the payload of the stream with null if we could not find the top-k number of routes.
        while (position < (4 * kValue)) {
            data[position++] = "null";
            data[position++] = "null";
            data[position++] = "null";
            data[position++] = "null";
        }

        for(int i=0;i<position;i=i+4){
            //If the previous data we recorded was null or we have different data recorded in this tuple, we set the duplicate flag to false.
            //Note that by default, the duplicate flag is set to true.
            //But in most of the cases where previous data item is set to null or its updated, the duplicate flag is set to false indicating that this is not a duplicate output.
            if(!previousData[i/4].equals(data[i])) {
                duplicate = false;
            }
            previousData[i/4] = data[i];
        }

//        //In this for loop we are copying the input data tuple which triggered this event to the output stream.
//        for (Object value:event.getData()) {
//            data[position++] = value;
//        }
//        data[position++] = event.getData()[pickUpDateTimeFieldID];
//        data[position++] = event.getData()[dropOffDataTimeFieldID];
//        data[position++] = event.getData()[timeStampFieldID];

        //And finally we set the duplicate flag.
        data[position++] = duplicate;
        duplicate = true;	//we reset the Duplicate flag here.

//        System.out.println("-----------------------------------------------------------\r\n");
//        for(int i=0; i<data.length; i++){
//        	System.out.print(data[i] + ",");
//        }
//        System.out.println("\r\n-----------------------------------------------------------");

        if (debugEnabled) {
            LOGGER.debug("Latest Top-K elements with frequency" + data);
        }

    }

}
