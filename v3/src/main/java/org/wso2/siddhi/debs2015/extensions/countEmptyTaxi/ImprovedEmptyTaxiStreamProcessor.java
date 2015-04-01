package org.wso2.siddhi.debs2015.extensions.countEmptyTaxi;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.*;

/**
 * Created by sachini on 1/7/15.
 */

public class ImprovedEmptyTaxiStreamProcessor extends StreamProcessor {

    private static final Logger LOGGER = Logger.getLogger(ImprovedEmptyTaxiStreamProcessor.class);
    private boolean debugEnabled = false;

    private Map<Integer, Object[]> medallionMap = new HashMap<Integer, Object[]>();

    private Map<Integer, CountProfitCustomObj> cellDataMap = new HashMap<Integer, CountProfitCustomObj>();       //key:Cell, values: (profit , count )



    private LinkedList<Object[]> taxiInfoWindow = new LinkedList<Object[]>();

    //The K value
    private int kValue = 0;

    //An array of Objects to manipulate output stream elements


    @Override
    protected List<Attribute> init(AbstractDefinition abstractDefinition, ExpressionExecutor[] expressionExecutors, ExecutionPlanContext executionPlanContext) {
//        if (expressionExecutors.length != 3) {
//            LOGGER.error("Required Parameters : Seven");
//            throw new ExecutionPlanCreationException("Mismatching Parameter count.");
//        }
        //cellNo , profit , emptyTaxiCount ,  pickup_datetime , dropoff_datetime , iij_timestamp

        List<Attribute> attributeList = new ArrayList<Attribute>();

        attributeList.add(new Attribute("cellNo" , Attribute.Type.INT));
        attributeList.add(new Attribute("lastProfit" , Attribute.Type.FLOAT));
        attributeList.add(new Attribute("emptyTaxiCount" , Attribute.Type.INT));
        attributeList.add(new Attribute("profitability" , Attribute.Type.FLOAT));
        attributeList.add(new Attribute("pickup_datetime_val" , Attribute.Type.LONG));
        attributeList.add(new Attribute("dropoff_datetime_val" , Attribute.Type.LONG));
        attributeList.add(new Attribute("iij_timestamp_val" , Attribute.Type.LONG));

        return attributeList;
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor, StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        ComplexEventChunk<StreamEvent> eventChunk = new ComplexEventChunk<StreamEvent>();
        streamEventChunk.reset();
        while (streamEventChunk.hasNext()) {
            ComplexEvent complexEvent = streamEventChunk.next();

            Object[] inputData = new Object[attributeExpressionLength];
            for (int i = 0; i < attributeExpressionLength ; i++) {
                inputData[i] = attributeExpressionExecutors[i].execute(complexEvent);
            }

            process(inputData, eventChunk);

        }
        nextProcessor.process(eventChunk);
    }

    //    endCellNo, medallion, dropoff_datetime


    //select  cellNo , emptyTaxiCount



   // endCellNo, medallion, dropoff_datetime, startCellNo , profit, pickup_datetime, iij_timestamp , dropoff_datetime_org


    //cellNo , profit , emptyTaxiCount , profit pickup_datetime , dropoff_datetime , iij_timestamp     : output


//            "from profitStream#debs:emptyTaxi(endCellNo, medallion, dropoff_datetime, startCellNo , profit )  " +
//            "select  cellNo , profit , emptyTaxiCount , pickup_datetime, dropoff_datetime , _timestamp" +
//            " insert into taxiCountUpdateStream ;";

    private void process(Object[] inputData, ComplexEventChunk<StreamEvent> eventChunk) {
         Set<Integer> changedCell = new LinkedHashSet<Integer>();//key:Cell, values: (profit , count )


        //Adding new Count
         int  endCell =(Integer) inputData[0];
          CountProfitCustomObj previousData=  cellDataMap.get(endCell);
          if(previousData != null){
              previousData.count++;
          }  else {
              cellDataMap.put(endCell, new CountProfitCustomObj(0,1));
          }
         changedCell.add(endCell);


        //Adding new Profit
        int startCell = (Integer)inputData[3];
        CountProfitCustomObj previousDataStartValue=  cellDataMap.get(startCell);
        if(previousDataStartValue != null){
            previousDataStartValue.profit = (Float)inputData[4];
            changedCell.add(startCell);
        }  else {
            cellDataMap.put(startCell,  new CountProfitCustomObj((Float) inputData[4],0));
        }




        //remove previous trip of the same taxi
        Object[] previousDropoff = medallionMap.put((Integer) inputData[1], inputData);   // previousDropOff has endCellNo, medallion, dropoff_datetime
        if(previousDropoff != null){
            int cell = (Integer)previousDropoff[0];
            cellDataMap.get(cell).count--;
                changedCell.add(cell);

        }

        long currentTimeStamp = (Long)inputData[2];

        while (true){
            Object[] trip = taxiInfoWindow.peekFirst();        //endCellNo, medallion, dropoff_datetime, startCellNo , profit, pickup_datetime, iij_timestamp
            if(trip != null && currentTimeStamp -(Long)trip[2] >= 1800000){
                taxiInfoWindow.removeFirst();
                Integer medallionKey =  (Integer)trip[1];
                Object[] medallionMapEntry = medallionMap.get(medallionKey);

                if(medallionMapEntry != null && medallionMapEntry[2] ==trip[2] ){
                    int cell =   (Integer)trip[0];
                    cellDataMap.get(cell).count--;
                    changedCell.add(cell);
                    medallionMap.remove(medallionKey);

                }

            }  else {
                break;
            }

        }
        taxiInfoWindow.add(inputData);

        ////cellNo , profit , emptyTaxiCount ,  pickup_datetime , dropoff_datetime , iij_timestamp,

        for(Integer cell: changedCell){
            int count = cellDataMap.get(cell).count;
            if(count >.0) {
                float profit = cellDataMap.get(cell).profit;

                eventChunk.add(createEvent(new Object[]{cell, profit,count,profit/count,inputData[5],inputData[7], inputData[6]}));
            } else {
                cellDataMap.remove(cell);
            }
        }



    }

    private StreamEvent createEvent(Object[] data){
        StreamEvent streamEvent = new StreamEvent(0,0,7);
        complexEventPopulater.populateComplexEvent(streamEvent,data);
        return streamEvent;

    }


    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public Object[] currentState() {
        return new Object[]{};   //TODO
    }

    @Override
    public void restoreState(Object[] objects) {
        //TODO
    }


    class CountProfitCustomObj{
        private  float profit;
        private int count;

        public CountProfitCustomObj(float profit, int count){
          this.profit = profit;
          this.count = count;
        }

        public float getProfit() {
            return profit;
        }

        public int getCount() {
            return count;
        }
    }
}