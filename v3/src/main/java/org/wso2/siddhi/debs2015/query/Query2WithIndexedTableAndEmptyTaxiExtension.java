//package org.wso2.siddhi.debs2015.query;
//
//import com.google.common.base.Splitter;
//import org.wso2.siddhi.core.ExecutionPlanRuntime;
//import org.wso2.siddhi.core.SiddhiManager;
//import org.wso2.siddhi.core.config.SiddhiContext;
//import org.wso2.siddhi.core.event.Event;
//import org.wso2.siddhi.core.stream.input.InputHandler;
//import org.wso2.siddhi.core.stream.output.StreamCallback;
//import org.wso2.siddhi.debs2015.extensions.cellId.CellIdFunctionForQuery2;
//import org.wso2.siddhi.debs2015.extensions.maxK.MaxKStreamProcessorQuery2;
//import org.wso2.siddhi.debs2015.extensions.median.BucketingBasedMedianAggregator;
//import org.wso2.siddhi.debs2015.extensions.timeStamp.TimeStampFunction;
//import org.wso2.siddhi.debs2015.input.DataLoderThread;
//import org.wso2.siddhi.debs2015.input.EventSenderThread;
//import org.wso2.siddhi.debs2015.performance.PerformanceMonitoringThreadInput;
//import org.wso2.siddhi.debs2015.performance.PerformanceMonitoringThreadOutput;
//import org.wso2.siddhi.debs2015.util.Config;
//import org.wso2.siddhi.debs2015.util.Constants;
//
//import java.util.HashMap;
//import java.util.Map;
//import java.util.concurrent.LinkedBlockingQueue;
//
///**
//*
//*/
//
//
//public class Query2WithIndexedTableAndEmptyTaxiExtension {
//    //private static final Logger logger = Logger.getLogger(Query1.class);
//    private static Splitter splitter = Splitter.on(',');
//    private static LinkedBlockingQueue<String> aggregateInputList = new LinkedBlockingQueue<String>();
//    private static LinkedBlockingQueue<String> aggregateOutputList = new LinkedBlockingQueue<String>();
//    private static LinkedBlockingQueue<Object[]> eventBufferList = null;
//    private boolean incrementalLoadingFlag = false;
//    private static final int INPUT_INJECTION_TIMESTAMP_FIELD = 17;
//
//    /**
//     * @param args
//     */
//    public static void main(String[] args) {
//        Query2WithIndexedTableAndEmptyTaxiExtension query1Obj = new Query2WithIndexedTableAndEmptyTaxiExtension();
//        query1Obj.run();
//    }
//
//    public void run(){
//        //Load the configurations
//        final boolean performanceLoggingFlag = Config.getConfigurationInfo("org.wso2.siddhi.debs2015.flags.perflogging").equals("true") ? true : false;
//
//        if(performanceLoggingFlag){
//            System.out.println("Performance information collection and logging is enabled.");
//        }else{
//            System.out.println("Performance information collection and logging is disabled.");
//        }
//
////	    System.out.println("Started experiment at : " + System.currentTimeMillis());
//
//        SiddhiManager siddhiManager = new SiddhiManager();
//        SiddhiContext siddhiContext = siddhiManager.getSiddhiContext();
//        Map<String,Class> extensions = new HashMap<String, Class>();
//
//        extensions.put("debs:cellId",CellIdFunctionForQuery2.class);
//        extensions.put("debs:getTimestamp", TimeStampFunction.class);
//        //extensions.put("debs:median",MedianAggregator.class);
//        extensions.put("debs:median",BucketingBasedMedianAggregator.class);
//        extensions.put("MaxK:getMaxK", MaxKStreamProcessorQuery2.class);
//        extensions.put("debs:emptyTaxi",EmptyTaxiStreamProcessor.class);
//
//        siddhiContext.setSiddhiExtensions(extensions);
//
//        //07290D3599E7A0D62097A346EFCC1FB5,E7750A37CAB07D0DFF0AF7E3573AC141,2013-01-01 00:00:00,2013-01-01 00:02:00,120,0.44,-73.956528,40.716976,-73.962440,40.715008,CSH,3.50,0.50,0.50,0.00,0.0
//
//        //an md5sum of the identifier of the taxi - vehicle bound
//        //an md5sum of the identifier for the taxi license
//        //time when the passenger(s) were picked up
//        //time when the passenger(s) were dropped off
//        //duration of the trip (in seconds)
//        //trip distance in miles
//        //longitude coordinate of the pickup location
//        //latitude coordinate of the pickup location
//        //longitude coordinate of the drop-off location
//        //latitude coordinate of the drop-off location
//        //the payment method - credit card or cash
//        //fare amount in dollars
//        //surcharge in dollars
//        //tax in dollars
//        //tip in dollars
//        //bridge and tunnel tolls in dollars
//        //total paid amount in dollars
//        // This is an additional field used to indicate the time when the event has been injected to the query network.
//
//        String taxiTripStream = "define stream taxi_trips ( medallion string , hack_license string ,  pickup_datetime string, dropoff_datetime string , trip_time_in_secs int, " +
//                "trip_distance float, pickup_longitude float,  pickup_latitude float,  dropoff_longitude float,  dropoff_latitude float, fare_amount float, tip_amount float, iij_timestamp float); ";
//
//
//        String emptyTaxiTable = "@IndexBy('cellNo')  " +
//                "define table emptyTaxiCountTable (cellNo string , emptyTaxiCount long); ";
//
//
//        //The profit that originates from an area is computed by calculating the median fare + tip for trips that started in the area and ended within the last 15 minutes
//
//
//        //Output stream is of the format : cell_based_taxi_trips(startCellNo, endCellNo, pickup_datetime, dropoff_datetime, fare_amount, tip_amount, medallion, iij_timestamp)
//        String query1 = " @info(name = 'query1') " +
//                "from taxi_trips " +
//                "select debs:cellId(pickup_longitude,pickup_latitude) as startCellNo, debs:cellId(dropoff_longitude,dropoff_latitude) as endCellNo , " +
//                "debs:getTimestamp(pickup_datetime) as pickup_datetime , debs:getTimestamp(dropoff_datetime) as dropoff_datetime, fare_amount, tip_amount, medallion, iij_timestamp " +
//                " insert into cell_based_taxi_trips ;";
//
//
//
//        String query2 = "@info(name = 'query2') " +
//                "from cell_based_taxi_trips [startCellNo!='null' AND endCellNo!='null'] " +
//                "insert into filtered_cell_based_taxi_trips;";
//
//        //get profit
//        //Output stream is of the format : profitStream(profit, startCellNo, pickup_datetime, dropoff_datetime, iij_timestamp)
//        //window of 15 min calculate profit  groupby   startCellNo  -> profit, startCellNo, pickup_datetime, dropoff_datetime, iij_timestamp
//        String query3 = "@info(name = 'query3') " +
//                "from filtered_cell_based_taxi_trips#window.externalTime(dropoff_datetime , 15 min)  " +
//                "select debs:median(fare_amount+tip_amount) as profit, startCellNo, pickup_datetime, dropoff_datetime, iij_timestamp " +
//                "group by startCellNo " +
//                "insert all events  into profitStream ;";
//
//
//        //The number of empty taxis in an area is the sum of taxis that had a drop-off location in that area less than 30 minutes ago and had no following pickup yet.
//
//
//
//       String  query4= "@info(name = 'query4') " +
//                "from filtered_cell_based_taxi_trips#debs:emptyTaxi(endCellNo, medallion, dropoff_datetime )  " +
//                "select  cellNo , emptyTaxiCount " +
//               " insert into taxiCountUpdateStream ;";
//
//
//
//        String  query6 = "@info(name = 'query6') " +
//                "from taxiCountUpdateStream " +
//                "select cellNo , sum(emptyTaxiCount) as emptyTaxiCount   " +
//                "group by cellNo " +
//                "insert into emptyTaxiCountTable ;";
//
//
//
//        String  query7 = "@info(name = 'query7') " +
//                "from profitStream#window.length(0) join emptyTaxiCountTable " +
//                "on profitStream.startCellNo == emptyTaxiCountTable.cellNo " +
//                "select profitStream.startCellNo as cellNo , profitStream.profit as profit , emptyTaxiCountTable.emptyTaxiCount  as emptyTaxiCount ,  profitStream.pickup_datetime, profitStream.dropoff_datetime, profitStream.iij_timestamp as iij_timestamp " +
//                "   insert into profitRawData;";
//
//
//
//        String  query8 = "@info(name = 'query8') " +
//                "from profitRawData[emptyTaxiCount != 0] " +
//                "select cellNo, profit, emptyTaxiCount,  pickup_datetime, dropoff_datetime, profit/emptyTaxiCount as profit_per_taxi, iij_timestamp " +
//                "insert into finalProfitStream;";
//
//
//
//
//        String query9 = "@info(name = 'query9') " +
//                "from finalProfitStream#MaxK:getMaxK(profit_per_taxi, profit, emptyTaxiCount, cellNo,10, iij_timestamp) " +
//                "select pickup_datetime, dropoff_datetime, " +
//                "profitable_cell_id_1, empty_taxies_in_cell_id_1, median_profit_in_cell_id_1, profitability_of_cell_1," +
//                "profitable_cell_id_2, empty_taxies_in_cell_id_2, median_profit_in_cell_id_2, profitability_of_cell_2," +
//                "profitable_cell_id_3, empty_taxies_in_cell_id_3, median_profit_in_cell_id_3, profitability_of_cell_3," +
//                "profitable_cell_id_4, empty_taxies_in_cell_id_4, median_profit_in_cell_id_4, profitability_of_cell_4," +
//                "profitable_cell_id_5, empty_taxies_in_cell_id_5, median_profit_in_cell_id_5, profitability_of_cell_5," +
//                "profitable_cell_id_6, empty_taxies_in_cell_id_6, median_profit_in_cell_id_6, profitability_of_cell_6," +
//                "profitable_cell_id_7, empty_taxies_in_cell_id_7, median_profit_in_cell_id_7, profitability_of_cell_7," +
//                "profitable_cell_id_8, empty_taxies_in_cell_id_8, median_profit_in_cell_id_8, profitability_of_cell_8," +
//                "profitable_cell_id_9, empty_taxies_in_cell_id_9, median_profit_in_cell_id_9, profitability_of_cell_9," +
//                "profitable_cell_id_10, empty_taxies_in_cell_id_10, median_profit_in_cell_id_10, profitability_of_cell_10, iij_timestamp " +
//                "insert into outputStream";
//
//
//
//        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(taxiTripStream+emptyTaxiTable+query1+query2+query3+query4+query6+query7+query8+query9);
//        //ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(taxiTripStream+emptyTaxiTable+query1+query2+query3+query4);
//        //ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(taxiTripStream+emptyTaxiTable+query1+query2+query3+query4+query6+query7+query8);
//
//        //Note: If less than 10 cells can be identified within the last 30 min, then NULL is to be output for all others that lack data.
//
//        //The attribute “delay” captures the time delay between reading the input event that triggered the output and the time when the output
//        //is produced. Participants must determine the delay using the current system time right after reading the input and right before writing
//        //the output. This attribute will be used in the evaluation of the submission.
//
//        executionPlanRuntime.addCallback("outputStream", new StreamCallback() {
//            long count = 1;
//            long totalLatency = 0;
//            long latencyFromBegining = 0;
//            long latencyWithinEventCountWindow = 0;
//            long startTime = System.currentTimeMillis();
//            long timeDifferenceFromStart = 0;
//            long timeDifference = 0; //This is the time difference for this time window.
//            long currentTime = 0;
//            long prevTime = 0;
//            long latency = 0;
//            long gggg= System.currentTimeMillis();
//            @Override
//            public void receive(Event[] events) {
//                //count = count+events.length;
////                EventPrinter.print(events);
////                long ss = System.currentTimeMillis() - gggg;
//                //System.out.println(count);
//
////                    for (Event evt : events) {
////                        currentTime = System.currentTimeMillis();
////                        long eventOriginationTime = (Long) evt.getData()[22];
////                        latency = currentTime - eventOriginationTime;
////                        latencyWithinEventCountWindow += latency;
////                        latencyFromBegining += latency;
////
////                        if (count % Constants.STATUS_REPORTING_WINDOW_OUTPUT == 0) {
////                            timeDifferenceFromStart = currentTime - startTime;
////                            timeDifference = currentTime - prevTime;
////                            //<time from start(ms)><time from start(s)><overall latency (ms/event)><latency in this time window (ms/event)><over all throughput (events/s)><throughput in this time window (events/s)>
////                            aggregateOutputList.add(timeDifferenceFromStart + "," + Math.round(timeDifferenceFromStart / 1000) + "," + Math.round(latencyFromBegining * 1.0d / count) + "," + Math.round(latencyWithinEventCountWindow * 1.0d / Constants.STATUS_REPORTING_WINDOW_OUTPUT) + "," + Math.round(count * 1000.0d / timeDifferenceFromStart) + "," + Math.round(Constants.STATUS_REPORTING_WINDOW_OUTPUT * 1000.0d / timeDifference));
////                            prevTime = currentTime;
////                            latencyWithinEventCountWindow = 0;
////                        }
////                        count++;
////                    }
//            }
//        });
//
//        executionPlanRuntime.start();
////            //startMonitoring();
////            loadEventsFromFile(Config.getConfigurationInfo("org.wso2.siddhi.debs2015.dataset"));
////
////            InputHandler inputHandler = executionPlanRuntime.getInputHandler("taxi_trips");
////            sendEventsFromQueue(inputHandler);
//
//        if(performanceLoggingFlag){
//            PerformanceMonitoringThreadOutput performanceMonitorOutputThread = new PerformanceMonitoringThreadOutput("Query2-WithIndexedTableAndEmptyTaxiExtension", aggregateOutputList);
//            performanceMonitorOutputThread.start();
//        }
//
//        incrementalLoadingFlag = Config.getConfigurationInfo("org.wso2.siddhi.debs2015.flags.incrementalloading").equals("true") ? true : false;
//
//        //Load the data from the input data set file. If the "incrementalloading" flag is set to
//        //true, the file will be read by the data loader thread in a sequence of time intervals.
//        //If the flag is false, the entire data set will be read and buffered in the RAM after
//        //this method gets called.
//        //loadEventsFromFile(Config.getConfigurationInfo("org.wso2.siddhi.debs2015.dataset"));
//        if(incrementalLoadingFlag){
//            System.out.println("Incremental data loading is performed.");
//            eventBufferList = new LinkedBlockingQueue<Object[]>(Constants.EVENT_BUFFER_SIZE);
//            DataLoderThread dataLoaderThread = new DataLoderThread(Config.getConfigurationInfo("org.wso2.siddhi.debs2015.dataset"), eventBufferList);
//            InputHandler inputHandler = executionPlanRuntime.getInputHandler("taxi_trips");
//            EventSenderThread senderThread = new EventSenderThread(eventBufferList, inputHandler);
//
//            //start the data loading process
//            dataLoaderThread.start();
//            //from here onwards we start sending the events
//            senderThread.start();
//        }else{
//            System.out.println("Data set will be loaded to the memory completely.");
//            eventBufferList = new LinkedBlockingQueue<Object[]>();
//            //BatchDataLoaderThread dataLoaderThread = new BatchDataLoaderThread(Config.getConfigurationInfo("org.wso2.siddhi.debs2015.dataset"), eventBufferList);
//            InputHandler inputHandler = executionPlanRuntime.getInputHandler("taxi_trips");
//            EventSenderThread senderThread = new EventSenderThread(eventBufferList, inputHandler);
//            DataLoderThread dataLoaderThread = new DataLoderThread(Config.getConfigurationInfo("org.wso2.siddhi.debs2015.dataset"), eventBufferList);
//
//            //start the data loading process
//            dataLoaderThread.start();
//            //from here onwards we start sending the events
//            senderThread.start();
//        }
//
//        //Just make the main thread sleep infinitely
//        //Note that we cannot have an event based mechanism to exit from this infinit loop. It is
//        //because even if the data sending thread has completed its task of sending the data to
//        //the SiddhiManager, the SiddhiManager object may be conducting the processing of the remaining
//        //data. Furthermore, since this is CEP its better have this type of mechanism, rather than
//        //terminating once we are done sending the data to the CEP engine.
//        while(true){
//            try {
//                Thread.sleep(Constants.MAIN_THREAD_SLEEP_TIME);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }
//
//        //executionPlanRuntime.shutdown();
//    }
//
//
//
//
//
//}