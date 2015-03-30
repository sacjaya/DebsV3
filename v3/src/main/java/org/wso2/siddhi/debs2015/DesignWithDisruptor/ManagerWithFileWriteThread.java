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

package org.wso2.siddhi.debs2015.DesignWithDisruptor;

import com.google.common.base.Splitter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.debs2015.performance.PerfStats;
import org.wso2.siddhi.debs2015.util.Config;
import org.wso2.siddhi.debs2015.util.Constants;

import java.io.*;
import java.util.Iterator;
import java.util.concurrent.Executors;


public class ManagerWithFileWriteThread {

    private InputHandler inputHandlerForQ1;
    private InputHandler getInputHandlerForQ2;
    private InputHandler getInputHandlerForQ2_3;
    volatile long events = 0;
    volatile long dataSetSize;
    private long startTime;
    private static PerfStats perfStats1 = new PerfStats();
    private static PerfStats perfStats2 = new PerfStats();
    private static long lastEventTime1 = 0;
    private static long lastEventTime2 = 0;

    public static void main(String[] args) {
        ManagerWithFileWriteThread manager = new ManagerWithFileWriteThread();
        manager.run();
        System.exit(0);
    }

    private void run() {
        System.out.println("time from start(ms),time from start(s), overall latency (ms/event), latency in this time window (ms/event), overall throughput(events/s), throughput in this time window (events/s), total number of events received till this time (events)\r\n");
        final boolean performanceLoggingFlag = Config.getConfigurationInfo("org.wso2.siddhi.debs2015.flags.perflogging").equals("true") ? true : false;
        final boolean printOutputFlag = Config.getConfigurationInfo("org.wso2.siddhi.debs2015.flags.printoutput").equals("true") ? true : false;

        dataSetSize = Long.parseLong(Config.getConfigurationInfo("org.wso2.siddhi.debs2015.dataset.size"));
        final String logDir = Config.getConfigurationInfo("org.wso2.siddhi.debs2015.experiment.logdir");

        Query2Part1 query2Part1 = new Query2Part1();
        ExecutionPlanRuntime executionPlanRuntimeQ2p1 = query2Part1.addExecutionPlan();
        InputHandler taxiTripsInputHandler = executionPlanRuntimeQ2p1.getInputHandler("taxi_trips");

        Query1Part1 query1Part1 = new Query1Part1();
        ExecutionPlanRuntime executionPlanRuntimeQ1p1 = query1Part1.addExecutionPlan();
        inputHandlerForQ1 = executionPlanRuntimeQ1p1.getInputHandler("profitStream");

        Query2Part2 query2Part2 = new Query2Part2();
        ExecutionPlanRuntime executionPlanRuntimeQ2p2 = query2Part2.addExecutionPlan();
        getInputHandlerForQ2 = executionPlanRuntimeQ2p2.getInputHandler("profitStream");

        Query2Part3 query2Part3 = new Query2Part3();
        ExecutionPlanRuntime executionPlanRuntimeQ2p3 = query2Part3.addExecutionPlan();
        getInputHandlerForQ2_3 = executionPlanRuntimeQ2p3.getInputHandler("profitRawData");

        Disruptor<EventHolder> dataWriterDisruptor = new Disruptor<EventHolder>(new com.lmax.disruptor.EventFactory<EventHolder>() {
            @Override
            public EventHolder newInstance() {
                return new EventHolder();
            }
        }, 2 ^ 18, Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("data-writer-thread-%d").build()), ProducerType.MULTI, new SleepingWaitStrategy());

        try {

            final RingBuffer<EventHolder> dataWriterBuffer = dataWriterDisruptor.getRingBuffer();

            executionPlanRuntimeQ1p1.addCallback("q1outputStream", new StreamCallback() {

                @Override
                public void receive(Event[] events) {

                    long sequenceNo = dataWriterBuffer.next();
                    try {
                        EventHolder eventHolder = dataWriterBuffer.get(sequenceNo);
                        eventHolder.isQuery1 = true;
                        eventHolder.events = events;
                    } finally {
                        dataWriterBuffer.publish(sequenceNo);
                    }
                }

            });


            executionPlanRuntimeQ2p3.addCallback("q2outputStream", new StreamCallback() {

                @Override
                public void receive(Event[] events) {

                    long sequenceNo = dataWriterBuffer.next();
                    try {
                        EventHolder eventHolder = dataWriterBuffer.get(sequenceNo);
                        eventHolder.isQuery1 = false;
                        eventHolder.events = events;
                    } finally {
                        dataWriterBuffer.publish(sequenceNo);
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

            executionPlanRuntimeQ2p2.addCallback("profitRawData", new StreamCallback() {
                @Override
                public void receive(Event[] events) {
                    try {
                        getInputHandlerForQ2_3.send(events);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                }
            });

            dataWriterDisruptor.handleEventsWith(new EventHandler<EventHolder>() {

                public long prevTime1 = System.currentTimeMillis();
                public long latencyWithinEventCountWindow1;
                FileWriter fw1 = new FileWriter(new File(logDir + "/output-1-" + System.currentTimeMillis() + ".csv").getAbsoluteFile());
                BufferedWriter bw1 = new BufferedWriter(fw1);

                public long prevTime2 = System.currentTimeMillis();
                public long latencyWithinEventCountWindow2;
                FileWriter fw2 = new FileWriter(new File(logDir + "/output-2-" + System.currentTimeMillis() + ".csv").getAbsoluteFile());
                BufferedWriter bw2 = new BufferedWriter(fw2);

                StringBuilder stringBuilder = new StringBuilder();

                @Override
                public void onEvent(EventHolder eventHolder, long l, boolean b) throws Exception {

                    if (eventHolder.isQuery1) {
                        for (Event evt : eventHolder.events) {
                            Object[] data = evt.getData();

                            if (printOutputFlag) {
                                stringBuilder.append(data[0]);
                                for (int i = 1; i < 22; i++) {
                                    stringBuilder.append(",");
                                    stringBuilder.append(data[i]);
                                }
                                stringBuilder.append("\r\n");
                            }


                            //If the performance logging flag is set, we need to print the performance measurements.
                            if (performanceLoggingFlag) {

                                long eventOriginationTime = (Long) data[22];
                                long currentTime = System.currentTimeMillis();
                                long latency = currentTime - eventOriginationTime;

                                perfStats1.count++;
                                perfStats1.totalLatency += latency;
                                perfStats1.lastEventTime = currentTime;

                                latencyWithinEventCountWindow1 += latency;
                                long timeDifference = currentTime - prevTime1;
                                long timeDifferenceFromStart = currentTime - startTime;

                                if ((perfStats1.count % Constants.STATUS_REPORTING_WINDOW_OUTPUT_QUERY1 == 0) && (timeDifference != 0)) {
                                    //<time from start(ms)><time from start(s)><overall latency (ms/event)><latency in this time window (ms/event)><over all throughput (events/s)><throughput in this time window (events/s)><total number of events received till this time (events)>
                                    System.out.println("q1," + timeDifferenceFromStart + "," + Math.round(timeDifferenceFromStart / 1000) + "," + Math.round(perfStats1.totalLatency * 1.0d / perfStats1.count) + "," + Math.round(latencyWithinEventCountWindow1 * 1.0d / Constants.STATUS_REPORTING_WINDOW_OUTPUT_QUERY1) + "," + Math.round(perfStats1.count * 1000.0d / timeDifferenceFromStart) + "," + Math.round(Constants.STATUS_REPORTING_WINDOW_OUTPUT_QUERY1 * 1000.0d / timeDifference) + "," + perfStats1.count);
                                    prevTime1 = currentTime;
                                    latencyWithinEventCountWindow1 = 0;
                                }
                            }

                        }
                        if (printOutputFlag) {
                            try {
                                bw1.write(stringBuilder.toString());
                                bw1.flush();
                            } catch (IOException e) {
                                e.printStackTrace();
                            } finally {
                                stringBuilder.setLength(0);
                            }
                        }
                    } else {
                        for (Event evt : eventHolder.events) {
                            Object[] data = evt.getData();

                            if (printOutputFlag) {
                                stringBuilder.append(data[0]);
                                for (int i = 1; i < 41; i++) {
                                    stringBuilder.append(",");
                                    stringBuilder.append(data[i]);
                                }
                                stringBuilder.append("\r\n");
                            }

                            if (performanceLoggingFlag) {
                                long currentTime = System.currentTimeMillis();
                                long eventOriginationTime = (Long) data[42];
                                long latency = currentTime - eventOriginationTime;

                                perfStats2.count++;
                                perfStats2.totalLatency += latency;
                                perfStats2.lastEventTime = currentTime;

                                latencyWithinEventCountWindow2 += latency;
                                long timeDifference = currentTime - prevTime2;
                                long timeDifferenceFromStart = currentTime - startTime;

                                if ((perfStats2.count % Constants.STATUS_REPORTING_WINDOW_OUTPUT_QUERY2 == 0) && (timeDifference != 0)) {
                                    //<time from start(ms)><time from start(s)><overall latency (ms/event)><latency in this time window (ms/event)><over all throughput (events/s)><throughput in this time window (events/s)><total number of events received till this time (events)>
                                    System.out.println("q2," + timeDifferenceFromStart + "," + Math.round(timeDifferenceFromStart / 1000) + "," + Math.round(perfStats2.totalLatency * 1.0d / perfStats2.count) + "," + Math.round(latencyWithinEventCountWindow2 * 1.0d / Constants.STATUS_REPORTING_WINDOW_OUTPUT_QUERY2) + "," + Math.round(perfStats2.count * 1000.0d / timeDifferenceFromStart) + "," + Math.round(Constants.STATUS_REPORTING_WINDOW_OUTPUT_QUERY2 * 1000.0d / timeDifference) + "," + perfStats2.count);
                                    prevTime2 = currentTime;
                                    latencyWithinEventCountWindow2 = 0;
                                }
                            }
                        }
                        if (printOutputFlag) {
                            try {
                                bw2.write(stringBuilder.toString());
                                bw2.flush();
                            } catch (IOException e) {
                                e.printStackTrace();
                            } finally {
                                stringBuilder.setLength(0);
                            }
                        }
                    }
                }

            });

        } catch (IOException e) {
            e.printStackTrace();
        }

        dataWriterDisruptor.start();
        executionPlanRuntimeQ1p1.start();
        executionPlanRuntimeQ2p3.start();
        executionPlanRuntimeQ2p2.start();
        executionPlanRuntimeQ2p1.start();

        System.out.println("Data loading started.");

        loadData(Config.getConfigurationInfo("org.wso2.siddhi.debs2015.dataset"), taxiTripsInputHandler);

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

        executionPlanRuntimeQ2p1.shutdown();
        executionPlanRuntimeQ2p3.shutdown();
        executionPlanRuntimeQ2p2.shutdown();
        executionPlanRuntimeQ1p1.shutdown();
        dataWriterDisruptor.shutdown();

    }


    public void loadData(String fileName, InputHandler inputHandler) {
        Splitter splitter = Splitter.on(',');
        BufferedReader br;
        int count = 0;

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

                try {
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
                } catch (NumberFormatException e) {
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

            long currentTime = System.currentTimeMillis();
            System.out.println();
            System.out.println("****** Input ******");
            System.out.println("events read : " + count);
            System.out.println("time to read (ms) : " + (currentTime - startTime));
            System.out.println("read throughput (events/s) : " + (events * 1000 / (currentTime - startTime)));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (Throwable e) {
            e.printStackTrace();
        }
        System.out.println("Now exiting from data loader");
    }

    private class EventHolder {
        Event[] events;
        boolean isQuery1;
    }
}

