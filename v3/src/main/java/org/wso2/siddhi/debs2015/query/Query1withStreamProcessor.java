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

package org.wso2.siddhi.debs2015.query;

import com.google.common.base.Splitter;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.debs2015.extensions.cellId.CellIdFunction;
import org.wso2.siddhi.debs2015.extensions.maxK.MaxKStreamProcessor;
import org.wso2.siddhi.debs2015.extensions.median.BucketingBasedMedianAggregator;
import org.wso2.siddhi.debs2015.extensions.timeStamp.TimeStampFunction;
import org.wso2.siddhi.debs2015.input.DataLoderThread;
import org.wso2.siddhi.debs2015.input.EventSenderThread;
import org.wso2.siddhi.debs2015.performance.PerformanceMonitoringThreadInput;
import org.wso2.siddhi.debs2015.performance.PerformanceMonitoringThreadOutput;
import org.wso2.siddhi.debs2015.util.Config;
import org.wso2.siddhi.debs2015.util.Constants;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

//import org.wso2.siddhi.core.config.SiddhiConfiguration;


public class Query1withStreamProcessor {
    //private static final Logger logger = Logger.getLogger(Query1.class);
    private static Splitter splitter = Splitter.on(',');
    private static LinkedBlockingQueue<String> aggregateInputList = new LinkedBlockingQueue<String>();
    private static LinkedBlockingQueue<String> aggregateOutputList = new LinkedBlockingQueue<String>();
    private static LinkedBlockingQueue<Object[]> eventBufferList = null;
    private boolean incrementalLoadingFlag = false;
    private static final int INPUT_INJECTION_TIMESTAMP_FIELD = 17;

    /**
     * @param args
     */
    public static void main(String[] args) {
        Query1withStreamProcessor query1Obj = new Query1withStreamProcessor();
        query1Obj.run();
    }

    public void run(){
        //Load the configurations
		final boolean performanceLoggingFlag = Config.getConfigurationInfo("org.wso2.siddhi.debs2015.flags.perflogging").equals("true") ? true : false;

        if(performanceLoggingFlag){
            System.out.println("Performance information collection and logging is enabled.");
        }else{
            System.out.println("Performance information collection and logging is disabled.");
        }

//	    System.out.println("Started experiment at : " + System.currentTimeMillis());

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiContext siddhiContext = siddhiManager.getSiddhiContext();
        Map<String,Class> extensions = new HashMap<String, Class>();

        extensions.put("debs:cellId",CellIdFunction.class);
        extensions.put("debs:getTimestamp", TimeStampFunction.class);
        extensions.put("MaxK:getMaxK", MaxKStreamProcessor.class);
        //extensions.put("debs:median",MedianAggregator.class);
        extensions.put("debs:median",BucketingBasedMedianAggregator.class);

        siddhiContext.setSiddhiExtensions(extensions);

        //07290D3599E7A0D62097A346EFCC1FB5,E7750A37CAB07D0DFF0AF7E3573AC141,2013-01-01 00:00:00,2013-01-01 00:02:00,120,0.44,-73.956528,40.716976,-73.962440,40.715008,CSH,3.50,0.50,0.50,0.00,0.0

        //an md5sum of the identifier of the taxi - vehicle bound
        //an md5sum of the identifier for the taxi license
        //time when the passenger(s) were picked up
        //time when the passenger(s) were dropped off
        //duration of the trip (in seconds)
        //trip distance in miles
        //longitude coordinate of the pickup location
        //latitude coordinate of the pickup location
        //longitude coordinate of the drop-off location
        //latitude coordinate of the drop-off location
        //the payment method - credit card or cash
        //fare amount in dollars
        //surcharge in dollars
        //tax in dollars
        //tip in dollars
        //bridge and tunnel tolls in dollars
        //total paid amount in dollars
        // This is an additional field used to indicate the time when the event has been injected to the query network.

        String taxiTripStream = "define stream taxi_trips ( medallion string , hack_license string ,  pickup_datetime_org string, dropoff_datetime_org string , trip_time_in_secs int, " +
                "trip_distance float, pickup_longitude float,  pickup_latitude float,  dropoff_longitude float,  dropoff_latitude float, fare_amount float, tip_amount float, iij_timestamp float); ";


        //Output stream is of the format : cell_based_taxi_trips(startCellNo, endCellNo, pickup_datetime, dropoff_datetime, iij_timestamp)
        String query1 = "@info(name = 'query1') " +
                "from taxi_trips " +
                "select debs:cellId(pickup_longitude,pickup_latitude) as startCellNo, debs:cellId(dropoff_longitude,dropoff_latitude) as endCellNo , " +
                "debs:getTimestamp(pickup_datetime_org) as pickup_datetime , debs:getTimestamp(dropoff_datetime_org) as dropoff_datetime, pickup_datetime_org, dropoff_datetime_org, iij_timestamp insert into cell_based_taxi_trips;";


        //window of 30 min calculate count groupby   startCellNo, endCellNo  -> startCellNo, endCellNo, pickup_datetime, dropoff_datetime, count
        String query3 = "@info(name = 'query3') " +
                "from cell_based_taxi_trips#window.externalTime(dropoff_datetime , 30 min) [startCellNo!='null' AND endCellNo!='null'] " +
                "select startCellNo , endCellNo,  pickup_datetime_org, dropoff_datetime_org, iij_timestamp " +
                "insert all events  into countStream ;";


        String query4 = "@info(name = 'query4') " +
                "from countStream#MaxK:getMaxK(startCellNo, endCellNo, 10, iij_timestamp) " +
                "select pickup_datetime_org, dropoff_datetime_org, startCell1 ,endCell1, startCell2, endCell2, startCell3 ,endCell3, startCell4, endCell4, startCell5, endCell5, startCell6, endCell6," +
                "startCell7 ,endCell7 , startCell8, endCell8, startCell9, endCell9, startCell10, endCell10, iij_timestamp " +
                "insert into outputStream";

        //ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(taxiTripStream+query1+query3+query4+query5);
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(taxiTripStream+query1+query3+query4);


        //Note: If less than 10 routes can be identified within the last 30 min, then NULL is to be output for all routes that lack data.

        //The attribute “delay” captures the time delay between reading the input event that triggered the output and the time when the output
        //is produced. Participants must determine the delay using the current system time right after reading the input and right before writing
        //the output. This attribute will be used in the evaluation of the submission.

        executionPlanRuntime.addCallback("outputStream", new StreamCallback() {
            long count = 1;
            long totalLatency = 0;
            long latencyFromBegining = 0;
            long latencyWithinEventCountWindow = 0;
            long startTime = System.currentTimeMillis();
            long timeDifferenceFromStart = 0;
            long timeDifference = 0; //This is the time difference for this time window.
            long currentTime = 0;
            long prevTime = 0;
            long latency = 0;
            @Override
            public void receive(Event[] events) {
                    //EventPrinter.print(events);
                //count = count+events.length;
                //System.out.println("*************************"+count);

                	/*
                    for (Event evt : events) {
                    	Object[] data = evt.getData();
                        currentTime = System.currentTimeMillis();
                        long eventOriginationTime = (Long) data[22];
                        latency = currentTime - eventOriginationTime;
                        
                        for(int i=0;i < 22; i++){
                        	System.out.print(data[i] + ",");
                        }
                        
                        System.out.println(latency);
                        
                        
                        latencyWithinEventCountWindow += latency;
                        latencyFromBegining += latency;

                        if (count % Constants.STATUS_REPORTING_WINDOW_OUTPUT == 0) {
                            timeDifferenceFromStart = currentTime - startTime;
                            timeDifference = currentTime - prevTime;
                            //<time from start(ms)><time from start(s)><overall latency (ms/event)><latency in this time window (ms/event)><over all throughput (events/s)><throughput in this time window (events/s)>
                            aggregateOutputList.add(timeDifferenceFromStart + "," + Math.round(timeDifferenceFromStart / 1000) + "," + Math.round(latencyFromBegining * 1.0d / count) + "," + Math.round(latencyWithinEventCountWindow * 1.0d / Constants.STATUS_REPORTING_WINDOW_OUTPUT) + "," + Math.round(count * 1000.0d / timeDifferenceFromStart) + "," + Math.round(Constants.STATUS_REPORTING_WINDOW_OUTPUT * 1000.0d / timeDifference));
                            prevTime = currentTime;
                            latencyWithinEventCountWindow = 0;
                        }
                        count++;
                        */
                //}
            	
            	/*
                //EventPrinter.print(events);
            	currentTime = System.currentTimeMillis();
                for (Event evt : events) {
                	//If the printoutput flag is set, we need to print the output.
                	
                	
                	Object[] data = evt.getData();
                    long eventOriginationTime = (Long) data[22];
                    latency = eventOriginationTime==-1l ? -1l:(currentTime - eventOriginationTime);
                    
                    if(printOutputFlag){
                        for(int i=0;i < 22; i++){
                        	System.out.print(data[i] + ",");
                        }
                        
                        System.out.println(latency);
                	}
                	
                	//If the performance logging flag is set, we need to print the performance measurements.
                	if(performanceLoggingFlag){
                        latencyWithinEventCountWindow += latency;
                        latencyFromBegining += latency;
    
                        if (count % Constants.STATUS_REPORTING_WINDOW_OUTPUT == 0) {
                            timeDifferenceFromStart = currentTime - startTime;
                            timeDifference = currentTime - prevTime;
                            
                            if(timeDifference!=0){
                                //<time from start(ms)><time from start(s)><overall latency (ms/event)><latency in this time window (ms/event)><over all throughput (events/s)><throughput in this time window (events/s)>
                                aggregateOutputList.add(timeDifferenceFromStart + "," + Math.round(timeDifferenceFromStart / 1000) + "," + Math.round(latencyFromBegining * 1.0d / count) + "," + Math.round(latencyWithinEventCountWindow * 1.0d / Constants.STATUS_REPORTING_WINDOW_OUTPUT) + "," + Math.round(count * 1000.0d / timeDifferenceFromStart) + "," + Math.round(Constants.STATUS_REPORTING_WINDOW_OUTPUT * 1000.0d / timeDifference));
                            }
                            prevTime = currentTime;
                            latencyWithinEventCountWindow = 0;
                        }
                    
                        count++;
                	}
                }
                */
            }
        });

        executionPlanRuntime.start();
//            //startMonitoring();
//            loadEventsFromFile(Config.getConfigurationInfo("org.wso2.siddhi.debs2015.dataset"));
//
//            InputHandler inputHandler = executionPlanRuntime.getInputHandler("taxi_trips");
//            sendEventsFromQueue(inputHandler);

        if(performanceLoggingFlag){
            PerformanceMonitoringThreadInput performanceMonitorInputThread = new PerformanceMonitoringThreadInput("query1-with-streamprocessor", aggregateInputList);
            performanceMonitorInputThread.start();
            PerformanceMonitoringThreadOutput performanceMonitorOutputThread = new PerformanceMonitoringThreadOutput("query1-with-streamprocessor", aggregateOutputList);
            performanceMonitorOutputThread.start();
        }

        incrementalLoadingFlag = Config.getConfigurationInfo("org.wso2.siddhi.debs2015.flags.incrementalloading").equals("true") ? true : false;

        //Load the data from the input data set file. If the "incrementalloading" flag is set to
        //true, the file will be read by the data loader thread in a sequence of time intervals.
        //If the flag is false, the entire data set will be read and buffered in the RAM after
        //this method gets called.
        //loadEventsFromFile(Config.getConfigurationInfo("org.wso2.siddhi.debs2015.dataset"));
        if(incrementalLoadingFlag){
            System.out.println("Incremental data loading is performed.");
            eventBufferList = new LinkedBlockingQueue<Object[]>(Constants.EVENT_BUFFER_SIZE);
            DataLoderThread dataLoaderThread = new DataLoderThread(Config.getConfigurationInfo("org.wso2.siddhi.debs2015.dataset"), eventBufferList);
            InputHandler inputHandler = executionPlanRuntime.getInputHandler("taxi_trips");
            EventSenderThread senderThread = new EventSenderThread(eventBufferList, aggregateInputList, inputHandler);

            //start the data loading process
            dataLoaderThread.start();
            //from here onwards we start sending the events
            senderThread.start();
        }else{
            System.out.println("Data set will be loaded to the memory completely.");
            eventBufferList = new LinkedBlockingQueue<Object[]>();
            //BatchDataLoaderThread dataLoaderThread = new BatchDataLoaderThread(Config.getConfigurationInfo("org.wso2.siddhi.debs2015.dataset"), eventBufferList);
            InputHandler inputHandler = executionPlanRuntime.getInputHandler("taxi_trips");
            EventSenderThread senderThread = new EventSenderThread(eventBufferList, aggregateInputList, inputHandler);
            DataLoderThread dataLoaderThread = new DataLoderThread(Config.getConfigurationInfo("org.wso2.siddhi.debs2015.dataset"), eventBufferList);

            //start the data loading process
            dataLoaderThread.start();
            //from here onwards we start sending the events
            senderThread.start();
        }

        //Just make the main thread sleep infinitely
        //Note that we cannot have an event based mechanism to exit from this infinit loop. It is
        //because even if the data sending thread has completed its task of sending the data to
        //the SiddhiManager, the SiddhiManager object may be conducting the processing of the remaining
        //data. Furthermore, since this is CEP its better have this type of mechanism, rather than
        //terminating once we are done sending the data to the CEP engine.
        while(true){
            try {
                Thread.sleep(Constants.MAIN_THREAD_SLEEP_TIME);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        //executionPlanRuntime.shutdown();
    }
}