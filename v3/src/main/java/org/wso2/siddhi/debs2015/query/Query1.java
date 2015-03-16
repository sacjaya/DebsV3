package org.wso2.siddhi.debs2015.query;

import com.google.common.base.Splitter;

import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.debs2015.extensions.cellId.CellIdFunction;
import org.wso2.siddhi.debs2015.extensions.maxK.MaxKTimeTransformerCopy;
import org.wso2.siddhi.debs2015.extensions.median.MedianAggregator;
import org.wso2.siddhi.debs2015.extensions.timeStamp.TimeStampFunction;
import org.wso2.siddhi.debs2015.input.DataLoderThread;
import org.wso2.siddhi.debs2015.input.EventSenderThread;
import org.wso2.siddhi.debs2015.performance.PerformanceMonitoringThreadInput;
import org.wso2.siddhi.debs2015.performance.PerformanceMonitoringThreadOutput;
import org.wso2.siddhi.debs2015.util.Config;
import org.wso2.siddhi.debs2015.util.Constants;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

//import org.wso2.siddhi.core.config.SiddhiConfiguration;


public class Query1 {
	//private static final Logger logger = Logger.getLogger(Query1.class);
	private static Splitter splitter = Splitter.on(',');
	private static LinkedBlockingQueue<String> aggregateInputList = new LinkedBlockingQueue<String>();
	private static LinkedBlockingQueue<String> aggregateOutputList = new LinkedBlockingQueue<String>();
	private static LinkedBlockingQueue<Object> eventBufferList = null;
	private boolean incrementalLoadingFlag = false;
	private static final int INPUT_INJECTION_TIMESTAMP_FIELD = 17;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Query1 query1Obj = new Query1();
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
        extensions.put("MaxK:getMaxK", MaxKTimeTransformerCopy.class);
        extensions.put("debs:median",MedianAggregator.class);

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

        String taxiTripStream = "define stream taxi_trips ( medallion string , hack_license string ,  pickup_datetime string, dropoff_datetime string , trip_time_in_secs int, " +
                "trip_distance float, pickup_longitude float,  pickup_latitude float,  dropoff_longitude float,  dropoff_latitude float, fare_amount float, tip_amount float, iij_timestamp float); ";

//        
//        from sensorInput[sensorId == "ID1"]
//        		select sensorId, sensorVersion, sensorValue
//        		insert into filteredSensorStream;
        //Output stream is of the format : cell_based_taxi_trips(startCellNo, endCellNo, pickup_datetime, dropoff_datetime, iij_timestamp)
        String query1 = "@info(name = 'query1') " +
                "from taxi_trips " +
                "select debs:cellId(pickup_longitude,pickup_latitude) as startCellNo, debs:cellId(dropoff_longitude,dropoff_latitude) as endCellNo , " +
                "debs:getTimestamp(pickup_datetime) as pickup_datetime , debs:getTimestamp(dropoff_datetime) as dropoff_datetime, iij_timestamp insert into cell_based_taxi_trips;";



//        //filter the outlier cells
//        //In the first query, the cell ID must be in the range 0.0 to 300.300. The debs:cellId() function inserts '-' to the cell ID.
//        //Output stream from taskOne is of the format : cell_based_taxi_trips(startCellNo, endCellNo, pickup_datetime, dropoff_datetime)
//        //+Miyuru: This query might be not needed as described in CellIdFunction
//        String query2 = "@info(name = 'query2') " +
//                "from cell_based_taxi_trips[not(startCellNo == '-') and not(endCellNo == '-')] " +
//                "select *" +
//                "insert into filtered_cell_based_taxi_trips;";




        //window of 30 min calculate count groupby   startCellNo, endCellNo  -> startCellNo, endCellNo, pickup_datetime, dropoff_datetime, count
        String query3 = "@info(name = 'query3') " +
                "from cell_based_taxi_trips#window.externalTime(dropoff_datetime , 30 min) [startCellNo!='null' AND endCellNo!='null'] " +
                "select startCellNo , endCellNo, pickup_datetime, dropoff_datetime, count(startCellNo) as tripCount, iij_timestamp " +
                "group by startCellNo endCellNo " +
                "insert all events  into countStream ;";


        String query4 = "@info(name = 'query4') " +
                "from countStream#MaxK:getMaxK(tripCount, startCellNo, endCellNo, 10, iij_timestamp) " +
                "select * " +
                "insert into duplicate_outputStream ;";


        //discard duplicates
        String query5 = "@info(name = 'query5') " +
                "from duplicate_outputStream[duplicate == false]  " +
                "select pickup_datetime, dropoff_datetime, startCell1 ,endCell1, startCell2, endCell2, startCell3 ,endCell3, startCell4, endCell4, startCell5, endCell5, startCell6, endCell6," +
                "startCell7 ,endCell7 , startCell8, endCell8, startCell9, endCell9, startCell10, endCell10, iij_timestamp " +
                "insert into outputStream";

        //ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(taxiTripStream+query1+query3+query4+query5);
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(taxiTripStream+query1);


        //Note: If less than 10 routes can be identified within the last 30 min, then NULL is to be output for all routes that lack data.

        //The attribute “delay” captures the time delay between reading the input event that triggered the output and the time when the output
        //is produced. Participants must determine the delay using the current system time right after reading the input and right before writing
        //the output. This attribute will be used in the evaluation of the submission.

            executionPlanRuntime.addCallback("cell_based_taxi_trips", new StreamCallback() {
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
//                    for (Event evt : events) {
//                        currentTime = System.currentTimeMillis();
//                        long eventOriginationTime = (Long) evt.getData()[22];
//                        latency = currentTime - eventOriginationTime;
//                        latencyWithinEventCountWindow += latency;
//                        latencyFromBegining += latency;
//
//                        if (count % Constants.STATUS_REPORTING_WINDOW_OUTPUT == 0) {
//                            timeDifferenceFromStart = currentTime - startTime;
//                            timeDifference = currentTime - prevTime;
//                            //<time from start(ms)><time from start(s)><overall latency (ms/event)><latency in this time window (ms/event)><over all throughput (events/s)><throughput in this time window (events/s)>
//                            aggregateOutputList.add(timeDifferenceFromStart + "," + Math.round(timeDifferenceFromStart / 1000) + "," + Math.round(latencyFromBegining * 1.0d / count) + "," + Math.round(latencyWithinEventCountWindow * 1.0d / Constants.STATUS_REPORTING_WINDOW_OUTPUT) + "," + Math.round(count * 1000.0d / timeDifferenceFromStart) + "," + Math.round(Constants.STATUS_REPORTING_WINDOW_OUTPUT * 1000.0d / timeDifference));
//                            prevTime = currentTime;
//                            latencyWithinEventCountWindow = 0;
//                        }
//                        count++;
//                    }
                }
            });

           executionPlanRuntime.start();
//            //startMonitoring();
//            loadEventsFromFile(Config.getConfigurationInfo("org.wso2.siddhi.debs2015.dataset"));
//
//            InputHandler inputHandler = executionPlanRuntime.getInputHandler("taxi_trips");
//            sendEventsFromQueue(inputHandler);
            
            if(performanceLoggingFlag){
            	PerformanceMonitoringThreadInput performanceMonitorInputThread = new PerformanceMonitoringThreadInput(aggregateInputList);
            	performanceMonitorInputThread.start();
            	PerformanceMonitoringThreadOutput performanceMonitorOutputThread = new PerformanceMonitoringThreadOutput(aggregateOutputList);
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
            	eventBufferList = new LinkedBlockingQueue<Object>(Constants.EVENT_BUFFER_SIZE);
                DataLoderThread dataLoaderThread = new DataLoderThread(Config.getConfigurationInfo("org.wso2.siddhi.debs2015.dataset"), eventBufferList);
                InputHandler inputHandler = executionPlanRuntime.getInputHandler("taxi_trips");
                EventSenderThread senderThread = new EventSenderThread(eventBufferList, aggregateInputList, inputHandler);
                
                //start the data loading process
                dataLoaderThread.start();
                //from here onwards we start sending the events
                senderThread.start();
            }else{
            	System.out.println("Data set will be loaded to the memory completely.");
            	eventBufferList = new LinkedBlockingQueue<Object>();
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

//	/**
//	 * This method logs the stream statistics. This is coded as a separate Java thread so that it will remove the overhead of disk access from the main thread.
//	 */
//    private static void startMonitoring() {
//    	//Input data rate
//    	Thread monitoringThreadInput = new Thread(){
//    		FileWriter fw = null;
//			BufferedWriter bw = null;
//
//    		public void run(){
//    			String record = null;
//    			Date dNow = new Date();
//    			SimpleDateFormat ft = new SimpleDateFormat ("yyyy-MM-dd.hh:mm:ss-a-zzz");
//    			String timeStamp = ft.format(dNow);
//    			String statisticsDir = Config.getConfigurationInfo("org.wso2.siddhi.debs2015.experiment.logdir");
//        		try {
//	                fw = new FileWriter(new File(statisticsDir + "/input-stats-" + timeStamp + ".csv").getAbsoluteFile());
//	    			bw = new BufferedWriter(fw);
//	    			bw.write("time from start(ms),time from start(s),aggregate throughput (events/s),throughput in this time window (events/s),percentage completed (%)\r\n");
//                } catch (IOException e1) {
//	                e1.printStackTrace();
//                }
//
//    			while(true){
//    				record = aggregateInputList.poll();
//
//    				if(record != null){
//    					try {
//	                        bw.write(record + "\r\n");
//	                        bw.flush();
//                        } catch (IOException e) {
//	                        e.printStackTrace();
//                        }
//    				}
//
//    				//We sleep for half a second to avoid the CPU core being over utilized.
//    				//Since this is just a monitoring thread, there should not be much harm for over all performance in doing so.
//    				try {
//	                    Thread.currentThread().sleep(Constants.MONITORING_THREAD_SLEEP_TIME);
//                    } catch (InterruptedException e) {
//	                    e.printStackTrace();
//                    }
//    			}
//    		}
//    	};
//    	monitoringThreadInput.start();
//
//    	//Output
//    	Thread monitoringThreadOutput = new Thread(){
//    		FileWriter fw = null;
//			BufferedWriter bw = null;
//
//    		public void run(){
//    			String record = null;
//    			Date dNow = new Date();
//    			SimpleDateFormat ft = new SimpleDateFormat ("yyyy-MM-dd.hh:mm:ss-a-zzz");
//    			String timeStamp = ft.format(dNow);
//    			String statisticsDir = Config.getConfigurationInfo("org.wso2.siddhi.debs2015.experiment.logdir");
//        		try {
//	                fw = new FileWriter(new File(statisticsDir + "/output-stats-" + timeStamp + ".csv").getAbsoluteFile());
//	    			bw = new BufferedWriter(fw);
//	    			bw.write("time from start(ms),time from start(s), overall latency (ms/event), latency in this time window (ms/event), overall throughput(events/s), throughput in this time window (events/s)\r\n");
//                } catch (IOException e1) {
//	                e1.printStackTrace();
//                }
//
//    			while(true){
//    				record = aggregateOutputList.poll();
//
//    				if(record != null){
//    					try {
//	                        bw.write(record + "\r\n");
//	                        bw.flush();
//                        } catch (IOException e) {
//	                        e.printStackTrace();
//                        }
//    				}
//
//    				try {
//	                    Thread.currentThread().sleep(Constants.MONITORING_THREAD_SLEEP_TIME);
//                    } catch (InterruptedException e) {
//	                    e.printStackTrace();
//                    }
//    			}
//    		}
//    	};
//    	monitoringThreadOutput.start();
//    }

//    /**
//     * This method directly reads data from the data set file, constructs input events, and sends those to the query network.
//     * @param inputHandler
//     * @param fileName
//     */
//	public static void sendEventsFromFile(InputHandler inputHandler, String fileName) {
//        BufferedReader br;
//
//        long count = 1;
//        long timeDifferenceFromStart = 0;
//        long timeDifference = 0; //This is the time difference for this time window.
//        long currentTime = 0;
//        long prevTime = 0;
//        long startTime = System.currentTimeMillis();
//
//        try {
//            br = new BufferedReader(new FileReader(fileName), 10 * 1024 * 1024);
//            String line = br.readLine();
//            while (line != null) {
//            	//We make an assumption here that we do not get empty strings due to missing values that may present in the input data set.
//                Iterator<String> dataStrIterator = splitter.split(line).iterator();
//                String medallion = dataStrIterator.next();
//                String hack_license = dataStrIterator.next();
//                String pickup_datetime = dataStrIterator.next();
//                String dropoff_datetime = dataStrIterator.next();
//                String trip_time_in_secs = dataStrIterator.next();
//                String trip_distance = dataStrIterator.next();
//                String pickup_longitude = dataStrIterator.next();
//                String pickup_latitude = dataStrIterator.next();
//                String dropoff_longitude = dataStrIterator.next();
//                String dropoff_latitude = dataStrIterator.next();
//                String payment_type = dataStrIterator.next();
//                String fare_amount = dataStrIterator.next();
//                String surcharge = dataStrIterator.next();
//                String mta_tax = dataStrIterator.next();
//                String tip_amount = dataStrIterator.next();
//                String tolls_amount = dataStrIterator.next();
//                String total_amount = dataStrIterator.next();
//
//                Object[] eventData = null;
//
//                try{
//                eventData = new Object[]{medallion,
//                                                  hack_license ,
//                                                  pickup_datetime,
//                                                  dropoff_datetime,
//                                                  Short.parseShort(trip_time_in_secs),
//                                                  Float.parseFloat(trip_distance), //This can be represented by two bytes
//                                                  Float.parseFloat(pickup_longitude),
//                                                  Float.parseFloat(pickup_latitude),
//                                                  Float.parseFloat(dropoff_longitude),
//                                                  Float.parseFloat(dropoff_latitude),
//                                                  Boolean.parseBoolean(payment_type),
//                                                  Float.parseFloat(fare_amount), //These currency values can be coded to  two bytes
//                                                  Float.parseFloat(surcharge),
//                                                  Float.parseFloat(mta_tax),
//                                                  Float.parseFloat(tip_amount),
//                                                  Float.parseFloat(tolls_amount),
//                                                  Float.parseFloat(total_amount)};
//                }catch(NumberFormatException e){
//                	e.printStackTrace();
//                }
//
//                inputHandler.send(System.currentTimeMillis(), eventData);
//
//                if (count % Constants.STATUS_REPORTING_WINDOW_INPUT == 0) {
//                    float percentageCompleted = (count / Constants.EVENT_COUNT_PARTIAL_DATASET);
//                    currentTime = System.currentTimeMillis();
//                    timeDifferenceFromStart = (currentTime - startTime);
//                    timeDifference = currentTime - prevTime;
//                    //<time from start(ms)><time from start(s)><aggregate throughput (events/s)><throughput in this time window (events/s)><percentage completed (%)>
//                    aggregateInputList.add(timeDifferenceFromStart + "," + Math.round(timeDifferenceFromStart/1000) + "," + Math.round(count * 1000.0 / timeDifferenceFromStart) + "," + Math.round(Constants.STATUS_REPORTING_WINDOW_INPUT  * 1000.0/timeDifference) + "," + percentageCompleted);
//                    prevTime = currentTime;
//                }
//
//                line = br.readLine();
//                count++;
//            }
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        } catch (Throwable e) {
//            e.printStackTrace();
//        }
//    }

//    /**
//     * This method directly reads data from the data set file, constructs input events, and stores those on a queue.
//     * @param fileName
//     */
//	public static void loadEventsFromFile(final String fileName) {
//    	Thread inputDataLoaderThread = new Thread(){
//
//    		public void run(){
//                BufferedReader br;
//
//                long count = 1;
//                boolean dataLoadingFlag = true;
//                long currentEventCount = 0;
//                boolean incrementalLoadingFlag = Config.getConfigurationInfo("org.wso2.siddhi.debs2015.flags.incrementalloading").equals("true") ? true : false;
//
//                if(!incrementalLoadingFlag){
//                	System.out.println("Data set will be loaded to the memory completely.");
//                }else{
//                	System.out.println("Incremental data loading is performed.");
//                }
//
//                try {
//                    br = new BufferedReader(new FileReader(fileName), 10 * 1024 * 1024);
//                    String line = br.readLine();
//                    while (line != null) {
//                        	if(dataLoadingFlag){
//                            	//We make an assumption here that we do not get empty strings due to missing values that may present in the input data set.
//                                Iterator<String> dataStrIterator = splitter.split(line).iterator();
//                                String medallion = dataStrIterator.next();
//                                String hack_license = dataStrIterator.next();
//                                String pickup_datetime = dataStrIterator.next();
//                                String dropoff_datetime = dataStrIterator.next();
//                                String trip_time_in_secs = dataStrIterator.next();
//                                String trip_distance = dataStrIterator.next();
//                                String pickup_longitude = dataStrIterator.next();
//                                String pickup_latitude = dataStrIterator.next();
//                                String dropoff_longitude = dataStrIterator.next();
//                                String dropoff_latitude = dataStrIterator.next();
//                                String payment_type = dataStrIterator.next();
//                                String fare_amount = dataStrIterator.next();
//                                String surcharge = dataStrIterator.next();
//                                String mta_tax = dataStrIterator.next();
//                                String tip_amount = dataStrIterator.next();
//                                String tolls_amount = dataStrIterator.next();
//                                String total_amount = dataStrIterator.next();
//
//                                Object[] eventData = null;
//
//                                try{
//                                eventData = new Object[]{		  medallion,
//                                                                  hack_license ,
//                                                                  pickup_datetime,
//                                                                  dropoff_datetime,
//                                                                  Short.parseShort(trip_time_in_secs),
//                                                                  Float.parseFloat(trip_distance), //This can be represented by two bytes
//                                                                  Float.parseFloat(pickup_longitude),
//                                                                  Float.parseFloat(pickup_latitude),
//                                                                  Float.parseFloat(dropoff_longitude),
//                                                                  Float.parseFloat(dropoff_latitude),
//                                                                  Boolean.parseBoolean(payment_type),
//                                                                  Float.parseFloat(fare_amount), //These currency values can be coded to  two bytes
//                                                                  Float.parseFloat(surcharge),
//                                                                  Float.parseFloat(mta_tax),
//                                                                  Float.parseFloat(tip_amount),
//                                                                  Float.parseFloat(tolls_amount),
//                                                                  Float.parseFloat(total_amount),
//                                                                  0l}; //We need to attach the time when we are injecting an event to the query network. For that we have to set a separate field which will be populated when we are injecting an event to the input stream.
//                                }catch(NumberFormatException e){
//                                	e.printStackTrace();
//                                }
//
//                                //We keep on accumulating data on to the event queue.
//                                //eventBufferList.push(eventData);
//                                eventBufferList.put(eventData);
//                                line = br.readLine();
//                                count++;
//                        }
//
//                        //This part of the code will get activated only if we enable the incremental data loading flag in the "debs2015.properties" configuration file.
//                        if(incrementalLoadingFlag){
//                            currentEventCount = eventBufferList.size();
//
//                            if(dataLoadingFlag && (currentEventCount > Constants.EVENT_BUFFER_CEIL)){
//                            	dataLoadingFlag = false;
//                            	System.out.println("Event loading deactivated");
//                            }
//
//                            if((!dataLoadingFlag) && (currentEventCount < Constants.EVENT_BUFFER_FLOOR)){
//                            	dataLoadingFlag = true;
//                            	System.out.println("Event loading activated");
//                            }
//                        }
//                    }
//                    System.out.println("Total amount of events read : " + count);
//                } catch (FileNotFoundException e) {
//                    e.printStackTrace();
//                } catch (Throwable e) {
//                    e.printStackTrace();
//                }
//                System.out.println("Now exiting from data loader thread.");
//    		}
//    	};
//    	inputDataLoaderThread.start();
//    }

//	public static void sendEventsFromQueue(final InputHandler inputHandler){
//    	Thread inputDataPumpThread = new Thread(){
//
//    		public void run(){
//    			Object[] event = null;
//                long count = 1;
//                long timeDifferenceFromStart = 0;
//                long timeDifference = 0; //This is the time difference for this time window.
//                long currentTime = 0;
//                long prevTime = 0;
//                long startTime = System.currentTimeMillis();
//                long cTime = 0;
//
//    			while(true){
//    				try {
//	                    event = (Object[])eventBufferList.take();
//                    } catch (InterruptedException e1) {
//	                    // TODO Auto-generated catch block
//	                    e1.printStackTrace();
//                    }
//
//    				if(event != null){
//    					try {
//    						cTime = System.currentTimeMillis();
//    						event[INPUT_INJECTION_TIMESTAMP_FIELD]	= cTime; //This corresponds to the iij_timestamp
//	                        inputHandler.send(cTime, event);
//
//	                        if (count % Constants.STATUS_REPORTING_WINDOW_INPUT == 0) {
//
//	                            float percentageCompleted = ((float)count/ (Constants.EVENT_COUNT_PARTIAL_DATASET));
//	                            currentTime = System.currentTimeMillis();
//	                            timeDifferenceFromStart = (currentTime - startTime);
//	                            timeDifference = currentTime - prevTime;
//	                            //<time from start(ms)><time from start(s)><aggregate throughput (events/s)><throughput in this time window (events/s)><percentage completed (%)>
//	                            aggregateInputList.add(timeDifferenceFromStart + "," + Math.round(timeDifferenceFromStart/1000) + "," + Math.round(count * 1000.0 / timeDifferenceFromStart) + "," + Math.round(Constants.STATUS_REPORTING_WINDOW_INPUT  * 1000.0/timeDifference) + "," + percentageCompleted);
//	                            prevTime = currentTime;
//	                        }
//	                        count++;
//                        } catch (InterruptedException e) {
//	                        e.printStackTrace();
//                        }
//    				}
//    			}
//    		}
//    	};
//    	inputDataPumpThread.start();
//	}
}
