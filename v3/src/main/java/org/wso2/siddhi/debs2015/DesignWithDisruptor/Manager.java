package org.wso2.siddhi.debs2015.DesignWithDisruptor;

import com.google.common.base.Splitter;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.debs2015.performance.PerfStats;
import org.wso2.siddhi.debs2015.util.Config;
import org.wso2.siddhi.debs2015.util.Constants;

import java.io.*;
import java.util.Iterator;


public class Manager {

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
    private static String COMMA = ",";
    private static String CARRIAGERETURN_NEWLINE = "\r\n";

    public static void main(String[] args) {
        Manager manager = new Manager();
        manager.run();
        System.exit(0);
    }

    private void run() {
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


        try {
            executionPlanRuntimeQ1p1.addCallback("q1outputStream", new StreamCallback() {

                FileWriter fw = new FileWriter(new File(logDir + "/output-1-" + System.currentTimeMillis() + ".csv").getAbsoluteFile());
                BufferedWriter bw = new BufferedWriter(fw);
                StringBuilder stringBuilder = new StringBuilder();
                long startTimeOutput;

                @Override
                public void receive(Event[] events) {

                    if (startTimeOutput == 0) {
                        startTimeOutput = System.currentTimeMillis();
                    }
                    
                    long eventOriginationTime = 0l;
                    long currentTime = 0l;
                    long latency = 0l;
                    
                    for (Event evt : events) {

                        Object[] data = evt.getData();
                        
                        if (printOutputFlag) {
                            stringBuilder.append(data[0]);
                            for (int i = 1; i < 22; i++) {
                                stringBuilder.append(COMMA);
                                stringBuilder.append(data[i]);
                            }
                            stringBuilder.append(COMMA);
                            eventOriginationTime = (Long) data[22];
                            currentTime = System.currentTimeMillis();
                            latency = currentTime - eventOriginationTime;
                            stringBuilder.append(latency);
                            stringBuilder.append(CARRIAGERETURN_NEWLINE);
                        }

                        //If the performance logging flag is set, we need to print the performance measurements.
                        if (performanceLoggingFlag) {

                            perfStats1.count++;
                            perfStats1.totalLatency += latency;
                            perfStats1.lastEventTime = currentTime;
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
            });

            executionPlanRuntimeQ2p3.addCallback("q2outputStream", new StreamCallback() {
                FileWriter fw = new FileWriter(new File(logDir + "/output-2-" + System.currentTimeMillis() + ".csv").getAbsoluteFile());
                BufferedWriter bw = new BufferedWriter(fw);
                StringBuilder stringBuilder = new StringBuilder();
                long startTimeOutput;

                @Override
                public void receive(Event[] events) {

                    if (startTimeOutput == 0) {
                        startTimeOutput = System.currentTimeMillis();
                    }
                    
                    long currentTime = 0l;
                    long eventOriginationTime = 0l;
                    long latency = 0l;
                    
                    for (Event evt : events) {
                        Object[] data = evt.getData();
                        
                        if (printOutputFlag) {
                            stringBuilder.append(data[0]);

                            for (int i = 1; i < 42; i++) {
                                stringBuilder.append(COMMA);
                                stringBuilder.append(data[i]);
                            }
                            
                            stringBuilder.append(COMMA);
                            eventOriginationTime = (Long) data[42];
                            currentTime = System.currentTimeMillis();
                            latency = currentTime - eventOriginationTime;
                            stringBuilder.append(latency);
                            stringBuilder.append(CARRIAGERETURN_NEWLINE);
                        }

                        if (performanceLoggingFlag) {
                            perfStats2.count++;
                            perfStats2.totalLatency += latency;
                            perfStats2.lastEventTime = currentTime;
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
            });
        } catch (IOException e) {
            e.printStackTrace();
        }

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
                    long timeDifferenceFromStartQuery1 = perfStats1.lastEventTime - startTime;

                    System.out.println("event outputed :" + perfStats1.count);
                    System.out.println("time to process (ms) :" + timeDifferenceFromStartQuery1);
                    System.out.println("over all throughput (events/s) :" + ((perfStats1.count * 1000) / timeDifferenceFromStartQuery1));
                    System.out.println("over all avg latency (ms) :" + (perfStats1.totalLatency / perfStats1.count));
                    System.out.println();
                    System.out.println("***** Query 2 *****");
                    long timeDifferenceFromStartQuery2 = perfStats2.lastEventTime - startTime;

                    System.out.println("event outputed :" + perfStats2.count);
                    System.out.println("time to process (ms) :" + timeDifferenceFromStartQuery2);
                    System.out.println("over all throughput (events/s) :" + ((perfStats2.count * 1000) / timeDifferenceFromStartQuery2));
                    System.out.println("over all avg latency (ms) :" + (perfStats2.totalLatency / perfStats2.count));

                    long elapsedTime = (timeDifferenceFromStartQuery1 > timeDifferenceFromStartQuery2 ? timeDifferenceFromStartQuery1 : timeDifferenceFromStartQuery2);
                    System.out.println("overall elapsed time (ms) :" + elapsedTime);
                    System.out.println("throughput (events/s) :" + (dataSetSize * 1000 / elapsedTime));
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
                dataStrIterator.next();//hack_license
                String pickup_datetime = dataStrIterator.next();
                String dropoff_datetime = dataStrIterator.next();
                dataStrIterator.next();//trip_time_in_secs
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
                            pickup_datetime,
                            dropoff_datetime,
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
            System.out.println("total events in the data set : " + dataSetSize);
            System.out.println("events sent : " + count);
            System.out.println("time to read (ms) : " + (currentTime - startTime));
            System.out.println("read throughput (events/s) : " + (dataSetSize * 1000 / (currentTime - startTime)));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (Throwable e) {
            e.printStackTrace();
        }
        System.out.println("Now exiting from data loader");
    }
}