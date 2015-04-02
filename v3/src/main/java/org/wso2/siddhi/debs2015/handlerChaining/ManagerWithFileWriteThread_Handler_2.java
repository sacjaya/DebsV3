/*
 * Copyright (c) 2005 - 2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.wso2.siddhi.debs2015.handlerChaining;

import com.google.common.base.Splitter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import org.wso2.siddhi.debs2015.handlerChaining.processors.*;
import org.wso2.siddhi.debs2015.handlerChaining.processors.maxK.MaxKQ1Processor;
import org.wso2.siddhi.debs2015.handlerChaining.processors.maxK.MaxKQ2Processor;
import org.wso2.siddhi.debs2015.performance.PerfStats;
import org.wso2.siddhi.debs2015.util.Config;
import org.wso2.siddhi.debs2015.util.Constants;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;


public class ManagerWithFileWriteThread_Handler_2 {

    volatile long events = 0;
    volatile long dataSetSize;
    private long startTime;
    private static PerfStats perfStats1 = new PerfStats();
    private static PerfStats perfStats2 = new PerfStats();
    private static long lastEventTime1 = 0;
    private static long lastEventTime2 = 0;
    Disruptor<DebsEvent> dataReadDisruptor;
    private RingBuffer dataReadBuffer;
    final boolean performanceLoggingFlag = true;// Config.getConfigurationInfo("org.wso2.siddhi.debs2015.flags.perflogging").equals("true") ? true : false;
    final boolean printOutputFlag = true;// Config.getConfigurationInfo("org.wso2.siddhi.debs2015.flags.printoutput").equals("true") ? true : false;
    final String logDir = Config.getConfigurationInfo("org.wso2.siddhi.debs2015.experiment.logdir");
    private static String COMMA = ",";
    private static String CARRIAGERETURN_NEWLINE = "\r\n";

    public static void main(String[] args) {
        ManagerWithFileWriteThread_Handler_2 manager = new ManagerWithFileWriteThread_Handler_2();

        manager.run();

        System.exit(0);
    }

    private void run() {
        dataSetSize = Long.parseLong(Config.getConfigurationInfo("org.wso2.siddhi.debs2015.dataset.size"));
        dataReadDisruptor = new Disruptor<DebsEvent>(new com.lmax.disruptor.EventFactory<DebsEvent>() {

            public DebsEvent newInstance() {
                return new DebsEvent();
            }
        }, 1024, Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("data-reader-thread-%d").build()), ProducerType.SINGLE, new SleepingWaitStrategy());

        //******************Handlers**************************************//

        ConversionHandler conversionHandler = new ConversionHandler();
        Q1TopKHandler q1TopKHandler = new Q1TopKHandler();
        Q2ProfitHandler q2ProfitabilityHandler = new Q2ProfitHandler();
        Q2MaxKHandler q2TopKHandler = new Q2MaxKHandler();
        Q2EmptyTaxiHandler emptyTaxiHandler = new Q2EmptyTaxiHandler();

        dataReadDisruptor.handleEventsWith(conversionHandler);
        dataReadDisruptor.after(conversionHandler).handleEventsWith(q1TopKHandler,q2ProfitabilityHandler);
        dataReadDisruptor.after(q2ProfitabilityHandler).handleEventsWith(emptyTaxiHandler);
        dataReadDisruptor.after(emptyTaxiHandler).handleEventsWith(q2TopKHandler);
        dataReadBuffer = dataReadDisruptor.start();

        loadData(Config.getConfigurationInfo("org.wso2.siddhi.debs2015.dataset"));

        while (true) {
            try {
                if (lastEventTime1 == perfStats1.lastEventTime && lastEventTime2 == perfStats2.lastEventTime) {

                    System.out.println();
                    System.out.println("***** Query 1 *****");
                    long timeDifferenceFromStart = perfStats1.lastEventTime - startTime;

                    System.out.println("event outputed :" + perfStats1.count);
                    System.out.println("time to process (ms) :" + timeDifferenceFromStart);
                    System.out.println("over all throughput (events/s) :" + ((perfStats1.count * 1000) / timeDifferenceFromStart));
                    System.out.println("over all avg latency (ms) :" + (perfStats1.totalLatency / perfStats1.count));
                    System.out.println();
                    System.out.println("***** Query 2 *****");
                    timeDifferenceFromStart = perfStats2.lastEventTime - startTime;

                    System.out.println("event outputed :" + perfStats2.count);
                    System.out.println("time to process (ms) :" + timeDifferenceFromStart);
                    System.out.println("over all throughput (events/s) :" + ((perfStats2.count * 1000) / timeDifferenceFromStart));
                    System.out.println("over all avg latency (ms) :" + (perfStats2.totalLatency / perfStats2.count));
                    break;
                } else {
                    lastEventTime1 = perfStats1.lastEventTime;
                    lastEventTime2 = perfStats2.lastEventTime;
                    Thread.sleep(Constants.MAIN_THREAD_SLEEP_TIME);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void loadData(String fileName) {
        Splitter splitter = Splitter.on(',');
        BufferedReader br;
        int count = 0;
        HashMap<String, Integer> medallionMap = new HashMap<String, Integer>();
        int medallionCount = 1;

        try {
            br = new BufferedReader(new FileReader(fileName), 10 * 1024 * 1024);
            String line = br.readLine();
            startTime = System.currentTimeMillis();
            while (line != null) {

                if (dataSetSize == events) {
                    break;
                }
                events++;

                //We make an assumption here that we do not get empty strings due to missing values that may present in the input data set.
                Iterator<String> dataStrIterator = splitter.split(line).iterator();
                String medallion = dataStrIterator.next();

                Integer medallionIntVal = medallionMap.get(medallion);
                if (medallionIntVal == null) {
                    medallionIntVal = medallionCount;
                    medallionMap.put(medallion, medallionCount++);
                }
                dataStrIterator.next();//This is hack_license. Since we are not using hack_license,
                                       //we do this just for the sake of moving the curser to next attribute.
                String pickup_datetime = dataStrIterator.next();
                String dropoff_datetime = dataStrIterator.next();
                dataStrIterator.next();//This is trip_time_in_secs. But again we do not use it.
                dataStrIterator.next();//trip_distance
                String pickup_longitude = dataStrIterator.next();
                String pickup_latitude = dataStrIterator.next();
                String dropoff_longitude = dataStrIterator.next();
                String dropoff_latitude = dataStrIterator.next();
                dataStrIterator.next();//payment_type
                String fare_amount = dataStrIterator.next();
                dataStrIterator.next();//surcharge
                dataStrIterator.next();//mta_tax
                String tip_amount = dataStrIterator.next();

                //Although there are other attributes in this line after tip_amount (e.g., tolls_amount
                //and total_amount) we do not use those.
                
                //We are taking the time at this point because we have just finished reading the event data.
                //The time should include the time spent on filtering activities as well. Therefore, its
                //better to take the time stamp at this point of the code.
                long currentTIme = System.currentTimeMillis();
                float pickupLongitude = Float.parseFloat(pickup_longitude);
                float pickupLatitude;
                float dropoffLongitude;
                float dropoffLatitude;
                try {
                    if (-74.916578f > pickupLongitude || -73.120778f < pickupLongitude) {
                        line = br.readLine();
                        continue;
                    }

                    pickupLatitude = Float.parseFloat(pickup_latitude);

                    if (40.129715978f > pickupLatitude || 41.477182778f < pickupLatitude) {
                        line = br.readLine();
                        continue;
                    }

                    dropoffLongitude = Float.parseFloat(dropoff_longitude);

                    if (-74.916578f > dropoffLongitude || -73.120778f < dropoffLongitude) {
                        line = br.readLine();
                        continue;
                    }

                    dropoffLatitude = Float.parseFloat(dropoff_latitude);

                    if (40.129715978f > dropoffLatitude || 41.477182778f < dropoffLatitude) {
                        line = br.readLine();
                        continue;
                    }
                } catch (NumberFormatException e) {
                    //We do nothing here. This is due to having odd values for lat, lon values.
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

                long sequenceNo = dataReadBuffer.next();
                try {
                    DebsEvent eventHolder = dataReadDisruptor.get(sequenceNo);
                    eventHolder.setMedallion(medallionIntVal);
                    eventHolder.setPickup_datetime_org(pickup_datetime);
                    eventHolder.setDropoff_datetime_org(dropoff_datetime);
                    eventHolder.setPickup_longitude(pickupLongitude);
                    eventHolder.setPickup_latitude(pickupLatitude);
                    eventHolder.setDropoff_longitude(dropoffLongitude);
                    eventHolder.setDropoff_latitude(dropoffLatitude);
                    eventHolder.setFare_plus_ip_amount(totalAmount);
                    eventHolder.setIij_timestamp(currentTIme);

                } finally {
                    count++;
                    dataReadBuffer.publish(sequenceNo);
                }
                line = br.readLine();
            }

            long currentTime = System.currentTimeMillis();
            System.out.println("****** Input ******" + count);
            System.out.println("events read : " + events);
            System.out.println("time to read (ms) : " + (currentTime - startTime));
            System.out.println("read throughput (events/s) : " + (events * 1000 / (currentTime - startTime)));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (Throwable e) {
            e.printStackTrace();
        }
        System.out.println("Now exiting from data loader");
        try {
            Thread.sleep(100000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private class ConversionHandler implements EventHandler<DebsEvent> {
        CellIdProcessor cellIdProcessor = new CellIdProcessor();
        TimeStampProcessor timeStampProcessor = new TimeStampProcessor();

        public void onEvent(DebsEvent debsEvent, long l, boolean b) throws Exception {
            debsEvent.setStartCellNo(cellIdProcessor.execute(debsEvent.getPickup_longitude(), debsEvent.getPickup_latitude()));
            debsEvent.setEndCellNo(cellIdProcessor.execute(debsEvent.getDropoff_longitude(), debsEvent.getDropoff_latitude()));
            debsEvent.setPickup_datetime(timeStampProcessor.execute(debsEvent.getPickup_datetime_org()));
            debsEvent.setDropoff_datetime(timeStampProcessor.execute(debsEvent.getDropoff_datetime_org()));
        }
    }

    private class Q1TopKHandler implements EventHandler<DebsEvent> {
        ExternalTimeWindowProcessor externalTimeWindowProcessor = new ExternalTimeWindowProcessor(30 * 60 * 1000);
        MaxKQ1Processor maxKQ1Processor = new MaxKQ1Processor();
        StringBuilder stringBuilder = new StringBuilder();
        FileWriter fw = null;
        BufferedWriter bw = null;
        
        public Q1TopKHandler(){
        	super();
        	try {
	            fw = new FileWriter(new File(logDir + "/output-1-" + System.currentTimeMillis() + ".csv").getAbsoluteFile());
	            bw = new BufferedWriter(fw);
            } catch (IOException e) {
	            e.printStackTrace();
            }
        }
        
        public void onEvent(DebsEvent debsEvent, long l, boolean b) throws Exception {
            List<DebsEvent> afterThirtyMinWindow =externalTimeWindowProcessor.process(debsEvent);
            
            for (DebsEvent eve : afterThirtyMinWindow) {
                maxKQ1Processor.process(eve);
                
                long currentTime = System.currentTimeMillis();
                Object[] topKRoutes = eve.getTopK();
                if (topKRoutes != null) {
                    if (performanceLoggingFlag) {
                        long eventOriginationTime = debsEvent.getIij_timestamp();
                        long latency = currentTime - eventOriginationTime;

                        stringBuilder.append(eve.pickup_datetime_org);
                        stringBuilder.append(COMMA);
                        stringBuilder.append(eve.dropoff_datetime_org);
                        stringBuilder.append(COMMA);
                        
                        for(Object item:topKRoutes){
                            stringBuilder.append(item);
                            stringBuilder.append(COMMA);
                        }                       
                        
                        stringBuilder.append(latency);
                        stringBuilder.append(CARRIAGERETURN_NEWLINE);
                        
                        perfStats1.count++;
                        perfStats1.totalLatency += latency;
                        perfStats1.lastEventTime = currentTime;
                    }
                }
            }
            
            if (printOutputFlag) {
                try {
                    bw.write(stringBuilder.toString());
                    bw.flush();
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    stringBuilder.setLength(0);
                }
            }
        }
    }

    private class Q2ProfitHandler implements EventHandler<DebsEvent> {
        ExternalTimeWindowProcessor externalTimeWindowProcessor = new ExternalTimeWindowProcessor(15 * 60 * 1000);
        GroupByExecutor groupByExecutor = new GroupByExecutor();

        public void onEvent(DebsEvent debsEvent, long l, boolean b) throws Exception {
            List<DebsEvent> windowOutputList = externalTimeWindowProcessor.process(debsEvent);
            for (DebsEvent event : windowOutputList) {
                float profit = groupByExecutor.execute(event.getStartCellNo(), event.getFare_plus_ip_amount(), event.isCurrent());
                event.setProfit(profit);
            }

            debsEvent.setListAfterFirstWindow(windowOutputList);
        }
    }

    private class Q2EmptyTaxiHandler implements EventHandler<DebsEvent> {
        EmptyTaxiProcessor emptyTaxiProcessor = new EmptyTaxiProcessor();

        public void onEvent(DebsEvent debsEvent, long l, boolean b) throws Exception {
            for (DebsEvent event : debsEvent.getListAfterFirstWindow()) {
                emptyTaxiProcessor.process(event);
            }
        }
    }

    private class Q2MaxKHandler implements EventHandler<DebsEvent> {
        MaxKQ2Processor maxKQ2Processor = new MaxKQ2Processor();
        StringBuilder stringBuilder = new StringBuilder();
        FileWriter fw = null;
        BufferedWriter bw = null;
        
        public Q2MaxKHandler(){
        	super();
        	try {
	            fw = new FileWriter(new File(logDir + "/output-2-" + System.currentTimeMillis() + ".csv").getAbsoluteFile());
	            bw = new BufferedWriter(fw);
            } catch (IOException e) {
	            e.printStackTrace();
            }
        }
        public void onEvent(DebsEvent debsEvent, long l, boolean b) throws Exception {
            List<DebsEvent> after = debsEvent.getListAfterFirstWindow();
            for (DebsEvent eve : after) {
                for (ProfitObj profitObj : eve.getProfitObjList()) {
                    Object[] maxKOutPut = maxKQ2Processor.processEventForMaxK(profitObj, eve.isCurrent());
                    long currentTime = System.currentTimeMillis();
                    if (maxKOutPut != null) {
                        if (performanceLoggingFlag) {

                            long eventOriginationTime = debsEvent.getIij_timestamp();
                            long latency = currentTime - eventOriginationTime;

                            stringBuilder.append(debsEvent.pickup_datetime_org);
                            stringBuilder.append(COMMA);
                            stringBuilder.append(eve.dropoff_datetime_org);
                            stringBuilder.append(COMMA);
                            
                            for(Object item:maxKOutPut){
                                stringBuilder.append(item);
                                stringBuilder.append(COMMA);
                            }
                            
                            stringBuilder.append(latency);
                            stringBuilder.append(CARRIAGERETURN_NEWLINE);
                            
                            perfStats2.count++;
                            perfStats2.totalLatency += latency;
                            perfStats2.lastEventTime = currentTime;
                        }
                    }
                }
            }
            
            if (printOutputFlag) {
                try {
                    bw.write(stringBuilder.toString());
                    bw.flush();
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    stringBuilder.setLength(0);
                }
            }
        }
    }
}