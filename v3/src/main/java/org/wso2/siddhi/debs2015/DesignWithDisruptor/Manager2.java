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

import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.debs2015.util.Config;
import org.wso2.siddhi.debs2015.util.Constants;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;


public class Manager2 {

    private InputHandler inputHandlerForQ1;
    private InputHandler getInputHandlerForQ2;


    public static void main(String[] args) {
        Manager2 manager = new Manager2();
        manager.run();
    }

    private void run() {

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

            @Override
            public void receive(Event[] events) {
//                EventPrinter.print(events);
                count = count + events.length;
//                System.out.println("query1 output event count" + count);

//                currentTime = System.currentTimeMillis();

                /*for (Event evt : events) {
                    Object[] data = evt.getData();
                    long eventOriginationTime = (Long) data[22];
                    latency = eventOriginationTime == -1l ? -1l : (currentTime - eventOriginationTime);
                    System.out.println(latency);
                }  */

            }

        });


        Query2Part2 query2Part2 = new Query2Part2();
        ExecutionPlanRuntime executionPlanRuntimeQ2p2 = query2Part2.addExecutionPlan();
        getInputHandlerForQ2 = executionPlanRuntimeQ2p2.getInputHandler("profitStream");

        executionPlanRuntimeQ2p2.addCallback("q2outputStream", new StreamCallback() {
            long count = 1;
            long currentTime = 0;
            long latency = 0;

            @Override
            public void receive(Event[] events) {
//                EventPrinter.print(events);
                count = count + events.length;
//                System.out.println("query2 output event count" + count);

                currentTime = System.currentTimeMillis();


                /*for (Event evt : events) {
                    Object[] data = evt.getData();
                    long eventOriginationTime = (Long) data[42];
                    latency = eventOriginationTime == -1l ? -1l : (currentTime - eventOriginationTime);
                    System.out.println(latency);

                }*/

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


        System.out.println("Data loading started.");


        //Load the data from the input data set file. If the "incrementalloading" flag is set to
        //true, the file will be read by the data loader thread in a sequence of time intervals.
        //If the flag is false, the entire data set will be read and buffered in the RAM after
        //this method gets called.
        //loadEventsFromFile(Config.getConfigurationInfo("org.wso2.siddhi.debs2015.dataset"));

        System.out.println("Incremental data loading is performed.");


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

        //executionPlanRuntime.shutdown();

    }


    public void loadData(String fileName, InputHandler inputHandler) {
        int count = 0;
        long startTime = System.currentTimeMillis();

        try {
            FileInputStream f = new FileInputStream(fileName);
            FileChannel ch = f.getChannel();
            MappedByteBuffer mb = ch.map(FileChannel.MapMode.READ_ONLY,
                    0L, ch.size());
            int size = 8000;
            byte comma = ',';
            byte newLine = '\n';

            byte[] barray = new byte[size];

            int lastLine = size;
            while (mb.hasRemaining()) {

                int copySize = size - lastLine;
//                System.out.println(copySize);
                System.arraycopy(barray, lastLine, barray, 0, copySize);
//                System.out.println(new String(barray));
                int dataToFetch = Math.min(mb.remaining(), size - copySize);
                mb.get(barray, copySize, dataToFetch);

                int nGet = copySize + dataToFetch;
                lastLine = 0;
                int last = 0;
                while (true) {

                    if (nGet < last + 106) {
                        break;
                    }

                    String medallion = new String(barray, last, 32, StandardCharsets.UTF_8);
                    last += 33;

                    String hack_license = new String(barray, last, 32, StandardCharsets.UTF_8);
                    last += 33;

                    String pickup_datetime = new String(barray, last, 19, StandardCharsets.UTF_8);
                    last += 20;

                    String dropoff_datetime = new String(barray, last, 19, StandardCharsets.UTF_8);
                    last += 20;

                    String trip_time_in_secs = null;
                    for (int i = last; i < nGet; i++) {
                        if (comma == barray[i]) {
                            trip_time_in_secs = (new String(barray, last, i - last, StandardCharsets.UTF_8));
                            last = i + 1;
                            break;
                        }
                    }
                    if (trip_time_in_secs == null) {
                        break;
                    }

                    String trip_distance = null;
                    for (int i = last; i < nGet; i++) {
                        if (comma == barray[i]) {
                            trip_distance = (new String(barray, last, i - last, StandardCharsets.UTF_8));
                            last = i + 1;
                            break;
                        }
                    }
                    if (trip_distance == null) {
                        break;
                    }

                    String pickup_longitude = null;
                    for (int i = last; i < nGet; i++) {
                        if (comma == barray[i]) {
                            pickup_longitude = new String(barray, last, i - last, StandardCharsets.UTF_8);
                            last = i + 1;
                            break;
                        }
                    }
                    if (pickup_longitude == null) {
                        break;
                    }


                    String pickup_latitude = null;
                    for (int i = last; i < nGet; i++) {
                        if (comma == barray[i]) {
                            pickup_latitude = new String(barray, last, i - last, StandardCharsets.UTF_8);
                            last = i + 1;
                            break;
                        }
                    }
                    if (pickup_latitude == null) {
                        break;
                    }

                    String dropoff_longitude = null;
                    for (int i = last; i < nGet; i++) {
                        if (comma == barray[i]) {
                            dropoff_longitude = new String(barray, last, i - last, StandardCharsets.UTF_8);
                            last = i + 1;
                            break;
                        }
                    }
                    if (dropoff_longitude == null) {
                        break;
                    }

                    String dropoff_latitude = null;
                    for (int i = last; i < nGet; i++) {
                        if (comma == barray[i]) {
                            dropoff_latitude = (new String(barray, last, i - last, StandardCharsets.UTF_8));
                            last = i + 1;
                            break;
                        }
                    }
                    if (dropoff_latitude == null) {
                        break;
                    }

                    boolean passed = false;
                    for (int i = last; i < nGet; i++) {
                        if (comma == barray[i]) {
                            last = i + 1;
                            passed = true;
                            break;
                        }
                    }
                    if (!passed) {
                        break;
                    }

                    String fare_amount = null;
                    for (int i = last; i < nGet; i++) {
                        if (comma == barray[i]) {
                            fare_amount = (new String(barray, last, i - last, StandardCharsets.UTF_8));
                            last = i + 1;
                            break;
                        }
                    }
                    if (fare_amount == null) {
                        break;
                    }

                    passed = false;
                    for (int i = last; i < nGet; i++) {
                        if (comma == barray[i]) {
                            last = i + 1;
                            passed = true;
                            break;
                        }
                    }
                    if (!passed) {
                        break;
                    }

                    passed = false;
                    for (int i = last; i < nGet; i++) {
                        if (comma == barray[i]) {
                            last = i + 1;
                            passed = true;
                            break;
                        }
                    }
                    if (!passed) {
                        break;
                    }

                    String tip_amount = null;
                    for (int i = last; i < nGet; i++) {
                        if (comma == barray[i]) {
                            tip_amount = (new String(barray, last, i - last, StandardCharsets.UTF_8));
                            last = i + 1;
                            break;
                        }
                    }
                    if (tip_amount == null) {
                        break;
                    }

                    passed = false;
                    for (int i = last; i < nGet; i++) {
                        if (newLine == barray[i]) {
                            last = i + 1;
                            passed = true;
                            break;
                        }
                    }
                    if (!passed) {
                        break;
                    }

                    lastLine = last;

                    long currentTIme = System.currentTimeMillis();
                    float pickupLongitude = Float.parseFloat(pickup_longitude);

                    if (-74.916578f > pickupLongitude || -73.120778f < pickupLongitude) {
                        continue;
                    }

                    float pickupLatitude = Float.parseFloat(pickup_latitude);

                    if (40.129715978f > pickupLatitude || 41.477182778f < pickupLatitude) {
                        continue;
                    }


                    float dropoffLongitude = Float.parseFloat(dropoff_longitude);

                    if (-74.916578f > dropoffLongitude || -73.120778f < dropoffLongitude) {
                        continue;
                    }

                    float dropoffLatitude = Float.parseFloat(dropoff_latitude);

                    if (40.129715978f > dropoffLatitude || 41.477182778f < dropoffLatitude) {
                        continue;
                    }
                    float fareAmount = Float.parseFloat(fare_amount);
                    float tipAmount = Float.parseFloat(tip_amount);
                    float totalAmount;

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
                        //e.printStackTrace();
                        //If we find a discrepancy in converting data, then we have to discard that
                        //particular event.
                        continue;
                    }

                    inputHandler.send(eventData);
                    count++;
                }
            }

            System.out.println("Total amount of events read : " + count);
            System.out.println("Total time taken for read : " + (System.currentTimeMillis()-startTime));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (Throwable e) {
            e.printStackTrace();
        }
        System.out.println("Now exiting from data loader thread.");
    }

}

