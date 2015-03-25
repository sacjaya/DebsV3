package org.wso2.siddhi.debs2015.extensions.countEmptyTaxi;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.exception.ExecutionPlanCreationException;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.*;

/**
 * Created by sachini on 1/7/15.
 */

public class EmptyTaxiStreamProcessor extends StreamProcessor {

    private static final Logger LOGGER = Logger.getLogger(EmptyTaxiStreamProcessor.class);
    private boolean debugEnabled = false;

    private String endCell = "";
    private String medallion = "";
    private String dropoffDatetime = "";

    private int endCellPosition = 0;
    private int medallionPosition = 0;
    private int dropoffDatetimePosition = 0;

    private Map<String, Object[]> medallionMap = new HashMap<String, Object[]>();
    private LinkedList<Object[]> taxiInfo = new LinkedList<Object[]>();

    //The K value
    private int kValue = 0;

    //An array of Objects to manipulate output stream elements


    @Override
    protected List<Attribute> init(AbstractDefinition abstractDefinition, ExpressionExecutor[] expressionExecutors, ExecutionPlanContext executionPlanContext) {
        if (expressionExecutors.length != 3) {
            LOGGER.error("Required Parameters : Seven");
            throw new ExecutionPlanCreationException("Mismatching Parameter count.");
        }

        endCell = ((VariableExpressionExecutor) expressionExecutors[0]).getAttribute().getName();
        medallion = ((VariableExpressionExecutor) expressionExecutors[1]).getAttribute().getName();
        dropoffDatetime = ((VariableExpressionExecutor) expressionExecutors[2]).getAttribute().getName();

        endCellPosition = abstractDefinition.getAttributePosition(endCell);
        medallionPosition = abstractDefinition.getAttributePosition(medallion);
        dropoffDatetimePosition = abstractDefinition.getAttributePosition(dropoffDatetime);

        List<Attribute> attributeList = new ArrayList<Attribute>();

        attributeList.add(new Attribute("cellNo" , Attribute.Type.STRING));
        attributeList.add(new Attribute("emptyTaxiCount" , Attribute.Type.INT));

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

    private void process(Object[] inputData, ComplexEventChunk<StreamEvent> eventChunk) {

        eventChunk.add(createEvent(new Object[]{inputData[0],1}));

        Object[] previousDropoff = medallionMap.put((String) inputData[0], inputData);
        if(previousDropoff != null){
            eventChunk.add(createEvent(new Object[]{previousDropoff[0],-1}));
        }

        long currentTimeStamp = (Long)inputData[2];

        while (true){
            Object[] trip = taxiInfo.peekFirst();
            if(trip != null && currentTimeStamp -(Long)trip[2] >= 1800000){
                taxiInfo.removeFirst();
                String medallionKey = (String) trip[1];
                Object[] medallionMapEntry = medallionMap.get(medallionKey);

                if(medallionMapEntry != null && medallionMapEntry[2] ==trip[2] ){
                    eventChunk.add(createEvent(new Object[]{trip[0],-1}));
                    medallionMap.remove(medallionKey);

                }

            }  else {
                break;
            }

        }
        taxiInfo.add(inputData);

    }

    private StreamEvent createEvent(Object[] data){
        StreamEvent streamEvent = new StreamEvent(0,0,2);
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
}