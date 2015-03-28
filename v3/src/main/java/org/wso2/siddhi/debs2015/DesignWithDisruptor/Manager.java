package org.wso2.siddhi.debs2015.DesignWithDisruptor;

import com.google.common.base.Splitter;

import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.debs2015.performance.PerformanceMonitoringThreadOutput;
import org.wso2.siddhi.debs2015.util.Config;
import org.wso2.siddhi.debs2015.util.Constants;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;


public class Manager {

    private InputHandler inputHandlerForQ1;
    private InputHandler getInputHandlerForQ2;
    private static LinkedBlockingQueue<String> aggregateOutputListQuery1 = new LinkedBlockingQueue<String>();
    private static LinkedBlockingQueue<String> aggregateOutputListQuery2 = new LinkedBlockingQueue<String>();


    public static void main(String[] args) {
        Manager manager = new Manager();
        manager.run();
    }

    private void run() {
        final boolean performanceLoggingFlag = Config.getConfigurationInfo("org.wso2.siddhi.debs2015.flags.perflogging").equals("true") ? true : false;
        final boolean printOutputFlag = Config.getConfigurationInfo("org.wso2.siddhi.debs2015.flags.printoutput").equals("true") ? true : false;
        
        Query2Part1 query2Part1 = new Query2Part1();
        ExecutionPlanRuntime executionPlanRuntimeQ2p1 = query2Part1.addExecutionPlan();
        InputHandler taxiTripsInputHandler = executionPlanRuntimeQ2p1.getInputHandler("taxi_trips");


        Query1Part1 query1Part1 = new Query1Part1();
        ExecutionPlanRuntime executionPlanRuntimeQ1p1 = query1Part1.addExecutionPlan();
        inputHandlerForQ1 = executionPlanRuntimeQ1p1.getInputHandler("profitStream");

        executionPlanRuntimeQ1p1.addCallback("q1outputStream", new StreamCallback() {
            long count = 1;
            long currentTime = 0;
            long latency = 0;
            long totalLatency = 0;
            long latencyFromBegining = 0;
            long latencyWithinEventCountWindow = 0;
            long startTime = System.currentTimeMillis();
            long timeDifferenceFromStart = 0;
            long timeDifference = 0; //This is the time difference for this time window.
            long prevTime = 0;

            @Override
            public void receive(Event[] events) {
//                EventPrinter.print(events);
//                count = count + events.length;
//                System.out.println("query1 output event count" + count);

//                currentTime = System.currentTimeMillis();

                /*for (Event evt : events) {
                    Object[] data = evt.getData();
                    long eventOriginationTime = (Long) data[22];
                    latency = eventOriginationTime == -1l ? -1l : (currentTime - eventOriginationTime);
                    System.out.println(latency);
                }  */
            	
            	
            	currentTime = System.currentTimeMillis();
                for (Event evt : events) {
                	//If the printoutput flag is set, we need to print the output.
                	
                	
                	Object[] data = evt.getData();
                    long eventOriginationTime = (Long) data[22];
                    //latency = eventOriginationTime==-1l ? -1l:(currentTime - eventOriginationTime);
                    latency = currentTime - eventOriginationTime;
                    
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
                                //<time from start(ms)><time from start(s)><overall latency (ms/event)><latency in this time window (ms/event)><over all throughput (events/s)><throughput in this time window (events/s)><total number of events received till this time (events)>
                            	aggregateOutputListQuery1.add(timeDifferenceFromStart + "," + Math.round(timeDifferenceFromStart / 1000) + "," + Math.round(latencyFromBegining * 1.0d / count) + "," + Math.round(latencyWithinEventCountWindow * 1.0d / Constants.STATUS_REPORTING_WINDOW_OUTPUT) + "," + Math.round(count * 1000.0d / timeDifferenceFromStart) + "," + Math.round(Constants.STATUS_REPORTING_WINDOW_OUTPUT * 1000.0d / timeDifference) + "," + count);
                            }
                            prevTime = currentTime;
                            latencyWithinEventCountWindow = 0;
                        }
                    
                        count++;
                	}
                }
            	

            }

        });

        Query2Part2 query2Part2 = new Query2Part2();
        ExecutionPlanRuntime executionPlanRuntimeQ2p2 = query2Part2.addExecutionPlan();
        getInputHandlerForQ2 = executionPlanRuntimeQ2p2.getInputHandler("profitStream");

        executionPlanRuntimeQ2p2.addCallback("q2outputStream", new StreamCallback() {
            long count = 1;
            long currentTime = 0;
            long latency = 0;
            long totalLatency = 0;
            long latencyFromBegining = 0;
            long latencyWithinEventCountWindow = 0;
            long startTime = System.currentTimeMillis();
            long timeDifferenceFromStart = 0;
            long timeDifference = 0; //This is the time difference for this time window.
            long prevTime = 0;

            @Override
            public void receive(Event[] events) {
//                EventPrinter.print(events);
//                count = count + events.length;
//                System.out.println("query2 output event count" + count);
//
//                currentTime = System.currentTimeMillis();


                /*for (Event evt : events) {
                    Object[] data = evt.getData();
                    long eventOriginationTime = (Long) data[42];
                    latency = eventOriginationTime == -1l ? -1l : (currentTime - eventOriginationTime);
                    System.out.println(latency);

                }*/

            	
            	currentTime = System.currentTimeMillis();
            	
                for (Event evt : events) {
                    Object[] data = evt.getData();

                    long eventOriginationTime = (Long) data[42];
                    //System.out.println("eventOriginationTime:"+eventOriginationTime);
                    //latency = eventOriginationTime==-1l ? -1l:(currentTime - eventOriginationTime);
                    latency = currentTime - eventOriginationTime;
                    
                    if(printOutputFlag){
                        for(int i=0;i < 41; i++){
                        	System.out.print(data[i] + ",");
                        }
                        
                        System.out.println(latency);
                    }
            	
            	    if(performanceLoggingFlag){
                        latencyWithinEventCountWindow += latency;
                        latencyFromBegining += latency;
    
                        if (count % Constants.STATUS_REPORTING_WINDOW_OUTPUT == 0) {
                            timeDifferenceFromStart = currentTime - startTime;
                            timeDifference = currentTime - prevTime;
                            if(timeDifference!=0){
                                //<time from start(ms)><time from start(s)><overall latency (ms/event)><latency in this time window (ms/event)><over all throughput (events/s)><throughput in this time window (events/s)><total number of events received till this time (events)>
                            	aggregateOutputListQuery2.add(timeDifferenceFromStart + "," + Math.round(timeDifferenceFromStart / 1000) + "," + Math.round(latencyFromBegining * 1.0d / count) + "," + Math.round(latencyWithinEventCountWindow * 1.0d / Constants.STATUS_REPORTING_WINDOW_OUTPUT) + "," + Math.round(count * 1000.0d / timeDifferenceFromStart) + "," + Math.round(Constants.STATUS_REPORTING_WINDOW_OUTPUT * 1000.0d / timeDifference) + "," + count);
                            }
                            prevTime = currentTime;
                            latencyWithinEventCountWindow = 0;
                        }
                        count++;
                    }
            	}
            	
            }
        });


        executionPlanRuntimeQ2p1.addCallback("profitStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                try {
                    inputHandlerForQ1.send(events);
                    getInputHandlerForQ2.send(events);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        });


        executionPlanRuntimeQ1p1.start();
        executionPlanRuntimeQ2p2.start();
        executionPlanRuntimeQ2p1.start();
        
        if(performanceLoggingFlag){
        	PerformanceMonitoringThreadOutput performanceMonitorOutputThreadForQuery1 = new PerformanceMonitoringThreadOutput("composite-query1", aggregateOutputListQuery1);
        	performanceMonitorOutputThreadForQuery1.start();
        	
        	PerformanceMonitoringThreadOutput performanceMonitorOutputThreadForQuery2 = new PerformanceMonitoringThreadOutput("composite-query2", aggregateOutputListQuery2);
        	performanceMonitorOutputThreadForQuery2.start();
        }

        System.out.println("Data loading started.");

        loadData(Config.getConfigurationInfo("org.wso2.siddhi.debs2015.dataset"), taxiTripsInputHandler);
        //Just make the main thread sleep infinitely
        //Note that we cannot have an event based mechanism to exit from this infinit loop. It is
        //because even if the data sending thread has completed its task of sending the data to
        //the SiddhiManager, the SiddhiManager object may be conducting the processing of the remaining
        //data. Furthermore, since this is CEP its better have this type of mechanism, rather than
        //terminating once we are done sending the data to the CEP engine.
        while (true) {
            try {
                Thread.sleep(Constants.MAIN_THREAD_SLEEP_TIME);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    public void loadData(String fileName, InputHandler inputHandler) {
        Splitter splitter = Splitter.on(',');
        BufferedReader br;
        int count = 0;

        try {
            br = new BufferedReader(new FileReader(fileName), 10 * 1024 * 1024);
            String line = br.readLine();
            while (line != null) {
                //We make an assumption here that we do not get empty strings due to missing values that may present in the input data set.
                Iterator<String> dataStrIterator = splitter.split(line).iterator();
                String medallion = dataStrIterator.next();
                String hack_license = dataStrIterator.next();
                String pickup_datetime = dataStrIterator.next();
                String dropoff_datetime = dataStrIterator.next();
                String trip_time_in_secs = dataStrIterator.next();
                String trip_distance = dataStrIterator.next();
                String pickup_longitude = dataStrIterator.next();
                String pickup_latitude = dataStrIterator.next();
                String dropoff_longitude = dataStrIterator.next();
                String dropoff_latitude = dataStrIterator.next();
                dataStrIterator.next();
                String fare_amount = dataStrIterator.next();
                dataStrIterator.next();
                dataStrIterator.next();
                String tip_amount = dataStrIterator.next();

                long currentTIme = System.currentTimeMillis();
                float pickupLongitude = Float.parseFloat(pickup_longitude);
                
                try{
                    if (-74.916578f > pickupLongitude || -73.120778f < pickupLongitude) {
                        line = br.readLine();
                        continue;
                    }
    
                    float pickupLatitude = Float.parseFloat(pickup_latitude);
    
                    if (40.129715978f > pickupLatitude || 41.477182778f < pickupLatitude) {
                        line = br.readLine();
                        continue;
                    }
    
    
                    float dropoffLongitude = Float.parseFloat(dropoff_longitude);
    
                    if (-74.916578f > dropoffLongitude || -73.120778f < dropoffLongitude) {
                        line = br.readLine();
                        continue;
                    }
    
                    float dropoffLatitude = Float.parseFloat(dropoff_latitude);
    
                    if (40.129715978f > dropoffLatitude || 41.477182778f < dropoffLatitude) {
                        line = br.readLine();
                        continue;
                    }
                }catch(NumberFormatException e){
                	//We do nothing here. This is due having odd values for lat, lon values.
                	 line = br.readLine();
                	continue;
                }
                
                float fareAmount = Float.parseFloat(fare_amount);
                float tipAmount = Float.parseFloat(tip_amount);
                float totalAmount;

                //This is to address the issue where we may get fare or tip as negative values due to
                //erroneous records in the input data set.
                if (fareAmount < 0 || tipAmount < 0) {
                    totalAmount = -1f;
                } else {
                    totalAmount = fareAmount + tipAmount;
                }

                Object[] eventData = null;

                try {
                    eventData = new Object[]{medallion,
                            hack_license,
                            pickup_datetime,
                            dropoff_datetime,
                            Short.parseShort(trip_time_in_secs),
                            Float.parseFloat(trip_distance), //This can be represented by two bytes
                            pickup_longitude,
                            pickup_latitude,
                            dropoff_longitude,
                            dropoff_latitude,
                            totalAmount,
                            currentTIme
                    }; //We need to attach the time when we are injecting an event to the query network. For that we have to set a separate field which will be populated when we are injecting an event to the input stream.
                } catch (NumberFormatException e) {
                    //If we find a discrepancy in converting data, then we have to discard that
                    //particular event.
                    line = br.readLine();
                    continue;
                }

                inputHandler.send(eventData);
                line = br.readLine();
                count++;
            }
            System.out.println("Total amount of events read : " + count);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (Throwable e) {
            e.printStackTrace();
        }
        System.out.println("Now exiting from data loader thread.");
    }

}

