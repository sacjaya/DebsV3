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
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.debs2015.extensions.maxK.util.MaxKStoreCopy;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;


public class MaxKTimeTransformerQuery1 extends StreamFunctionProcessor {

   private static final Logger LOGGER = Logger.getLogger(MaxKTimeTransformerQuery1.class);
    private boolean debugEnabled = false;

    private String value = "";
    private String startCell = "";
    private String endCell = "";
    private String iijTimeStamp = "";
    private String pickupDateTime = "";
    private String dropoffDateTime = "";
    
    //The desired attribute position of value in input stream
    private int valuePosition = 0;
    private int startCellPosition = 0;
    private int endCellPosition = 0;
    private int iijTimeStampPosition = 0;
    private int pickupDateTimePosition = 0;
    private int dropoffDateTimePosition = 0;
    
    //The K value
    private int kValue = 0;

    //An array of Objects to manipulate output stream elements
    private Object[] data = null;
    private Object[] previousData = null;
    private boolean duplicate =true;


    private MaxKStoreCopy maxKStore = null;

    @Override
    protected Object[] process(Object[] objects) {
        processEventForMaxK(objects);
        return data;
    }

    @Override
    protected Object[] process(Object o) {
        throw new IllegalStateException("pol2Cart cannot execute for single data " + o);
    }

    @Override
    protected List<Attribute> init(AbstractDefinition abstractDefinition, ExpressionExecutor[] expressionExecutors, ExecutionPlanContext executionPlanContext) {

        if (expressionExecutors.length != 7) {
            LOGGER.error("Required Parameters : Seven");
            throw new ExecutionPlanCreationException("Mismatching Parameter count.");
        }

        //Getting all the parameters and assign those to instance variables
        //tripCount, startCellNo, endCellNo, 10, iij_timestamp
        //tripCount, startCellNo, endCellNo, 10, iij_timestamp, pickup_datetime_org, dropoff_datetime_org

        value = ((VariableExpressionExecutor) expressionExecutors[0]).getAttribute().getName();
        startCell = ((VariableExpressionExecutor) expressionExecutors[1]).getAttribute().getName();
        endCell = ((VariableExpressionExecutor) expressionExecutors[2]).getAttribute().getName();
        kValue = (Integer)((ConstantExpressionExecutor) expressionExecutors[3]).getValue();

        iijTimeStamp = ((VariableExpressionExecutor) expressionExecutors[4]).getAttribute().getName();
        pickupDateTime = ((VariableExpressionExecutor) expressionExecutors[5]).getAttribute().getName();
        dropoffDateTime = ((VariableExpressionExecutor) expressionExecutors[6]).getAttribute().getName();

//        System.out.println("iijTimeStamp : " + iijTimeStamp);
//        System.exit(0);

        valuePosition = abstractDefinition.getAttributePosition(value);
        startCellPosition = abstractDefinition.getAttributePosition(startCell);
        endCellPosition = abstractDefinition.getAttributePosition(endCell);
        iijTimeStampPosition = abstractDefinition.getAttributePosition(iijTimeStamp);
        pickupDateTimePosition = abstractDefinition.getAttributePosition(pickupDateTime);
        dropoffDateTimePosition = abstractDefinition.getAttributePosition(dropoffDateTime);
        
        List<Attribute> attributeList = new ArrayList<Attribute>();

//        attributeList.add(new Attribute("pickup_datetime_org", Attribute.Type.STRING));
//        attributeList.add(new Attribute("dropoff_datetime_org", Attribute.Type.STRING));

        for (int i = 1; i <= kValue; i++) {
            attributeList.add(new Attribute("startCell" + i , Attribute.Type.STRING));
            attributeList.add(new Attribute("endCell" + i , Attribute.Type.STRING));
        }

//        //By adding the input attributes to the output stream we are copying the events we received from the previous operator to down stream.
//        for(Attribute attribute:inStreamDefinition.getAttributeList()){
//            this.outStreamDefinition.attribute(attribute.getName(),attribute.getType());
//        }
        
//        this.outStreamDefinition.attribute("pickup_datetime", Attribute.Type.STRING);
//        this.outStreamDefinition.attribute("dropoff_datetime", Attribute.Type.STRING);
//        this.outStreamDefinition.attribute("iij_timestamp", Attribute.Type.LONG);

//        attributeList.add(new Attribute("pickup_datetime_org", Attribute.Type.STRING));
//        attributeList.add(new Attribute("dropoff_datetime_org", Attribute.Type.STRING));
        
        //Finally, we add a flag that indicates whether this is a duplicate event or not.
        attributeList.add(new Attribute("duplicate", Attribute.Type.BOOL));

        data = new Object[2 * kValue+4]; //This will be (2*10 + 4)=24
        previousData = new Object[2 * kValue];
        maxKStore = new MaxKStoreCopy();

        return attributeList;
    }

    public void start() {

    }

    public void stop() {

    }

    public Object[] currentState() {
        return new Object[]{value,valuePosition,startCell,startCellPosition,endCell,endCellPosition, kValue,maxKStore};
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

            this.startCell = (String) objects[2];
            this.startCellPosition = (Integer) objects[3];

            this.endCell = (String) objects[4];
            this.endCellPosition = (Integer) objects[5];

            this.kValue = (Integer) objects[6];
            this.maxKStore = (MaxKStoreCopy) objects[7];

        } else {
            //LOGGER.error("Failed in restoring the Max-K Transformer.");
        	System.out.println("Failed in restoring the Max-K Transformer.");
        }
    }

    private void processEventForMaxK(Object[] object) {
    	//tripCount, startCellNo, endCellNo, 10, iij_timestamp
    	//tripCount, startCellNo, endCellNo, 10, iij_timestamp, pickup_original, dropoff_original
        Long eventKeyValue = (Long)object[0];//This is the trip count
        Object startCellValue = object[1];
        Object endCellValue = object[2];

//        if(eventLengthFlag){
//        	eventLengthFlag = false;
//        	timeStampFieldID = event.getData().length - 1;
//        	pickUpDateTimeFieldID = timeStampFieldID -
//        }

        //Map<Long, List<String>> currentTopK;
        
        HashSet<String> currentTopK;

        //The method getMaxK() accepts the "<start cell ID>:<end cell ID>" and the trip count found for this route.
        currentTopK = maxKStore.getMaxK(startCellValue+":"+endCellValue, eventKeyValue.intValue(), kValue);

        //From here onwards we prepare the output data tuple from this operator.
        int position = 0;

        //This will be restricted to to k number of lists. Therefore, we do not need to check whether we have exceeded the top k.
       	//We do this until top-k is 10 (kValue==10)
            for (String cell:currentTopK){
                //String[] splitValues = cell.split(":");
            	int colonIndex = cell.indexOf(":");
            	//System.out.println("cell|"+cell+"|"+cell.substring(0, colonIndex)+"|"+cell.substring(colonIndex+1));
            	
                data[position++] = cell.substring(0, colonIndex);//start cell ID
                data[position++] = cell.substring(colonIndex+1);//end cell ID
                
                if(position>=kValue*2){
                    break;
                }
            }
        
        //Populating remaining elements for the payload of the stream with null if we could not find the top-k number of routes.
        while (position < (2 * kValue)) {
            data[position++] = "null";
            data[position++] = "null";
        }

        for(int i=0;i<position;i++){
        	//If the previous data we recorded was null or we have different data recorded in this tuple, we set the duplicate flag to false.
        	//Note that by default, the duplicate flag is set to true.
        	//But in most of the cases where previous data item is set to null or its updated, the duplicate flag is set to false indicating that this is not a duplicate output.
            if(previousData[i] == null || !previousData[i].equals(data[i])) {
                  duplicate = false;
            }
            previousData[i] = data[i];
        }
        
//        long pickUpTime = (Long)event.getData()[pickUpDateTimeFieldID];
//        long dropOffTime = (Long)event.getData()[dropOffDataTimeFieldID];
//        
//        
//        Date date = new Date(pickUpTime);
//        data[position++] = format.format(date);
//        date = new Date(dropOffTime);
//        data[position++] = format.format(date);
        
//        data[position++] = (String)event.getData()[pickUpOriginal];
//        data[position++] = (String)event.getData()[dropOffOriginal];
        
//        data[position++] = (String)object[5];//pickup_original
//        data[position++] = (String)object[6];//dropoff_original
        
//        System.out.println("pickUpOriginal:"+(String)event.getData()[pickUpOriginal]);
//        System.out.println("dropOffOriginal:"+(String)event.getData()[dropOffOriginal]);
        
        //data[position++] = event.getData()[timeStampFieldID];

        
        //And finally we set the duplicate flag.
        data[position++] = duplicate;
        duplicate = true;	//we reset the Duplicate flag here.
        
        long timeDifference = System.currentTimeMillis() - (Long)object[4];
        data[position++] = timeDifference;
        
        if (debugEnabled) {
            LOGGER.debug("Latest Top-K elements with frequency" + data);
        }

    }

}
