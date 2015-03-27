package org.wso2.siddhi.debs2015.DesignWithDisruptor;

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
import org.wso2.siddhi.debs2015.DesignWithDisruptor.util.DataPublisher;
import org.wso2.siddhi.debs2015.DesignWithDisruptor.util.SiddhiFactory;
import org.wso2.siddhi.debs2015.util.Config;
import org.wso2.siddhi.debs2015.util.Constants;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;


public class Manager {
    ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("my-thread-%d").build();
    private ExecutorService executorService = Executors.newCachedThreadPool(namedThreadFactory);

    public static void main(String[] args) {
        Manager manager = new Manager();
        manager.run();
    }

    private void run() {
        Disruptor<Event> disruptor = new Disruptor<Event>(new SiddhiFactory(), Constants.DISRUPTOR_BUFFER_SIZE, executorService, ProducerType.SINGLE, new SleepingWaitStrategy());
        final Disruptor<Event> disruptor2 = new Disruptor<Event>(new SiddhiFactory(), Constants.DISRUPTOR_BUFFER_SIZE, executorService, ProducerType.SINGLE, new SleepingWaitStrategy());

        Query2Part1 query2Part1 = new Query2Part1();
        ExecutionPlanRuntime executionPlanRuntimeQ2p1 = query2Part1.addExecutionPlan();
        InputHandler taxiTripsInputHandler = executionPlanRuntimeQ2p1.getInputHandler("taxi_trips");


        Query1Part1 query1Part1 = new Query1Part1();
        ExecutionPlanRuntime executionPlanRuntimeQ1p1 = query1Part1.addExecutionPlan();
        InputHandler q1ProfitStreamInputHandler = executionPlanRuntimeQ1p1.getInputHandler("profitStream");
        disruptor2.handleEventsWith(new DataHandler(q1ProfitStreamInputHandler, false));

        executionPlanRuntimeQ1p1.addCallback("q1outputStream", new StreamCallback() {
            long count = 1;
            long currentTime = 0;
            long latency = 0;

            @Override
            public void receive(Event[] events) {
                //EventPrinter.print(events);
                count = count + events.length;
                System.out.println("query1 output event count" + count);

                currentTime = System.currentTimeMillis();

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
        InputHandler profitStreamInputHandler = executionPlanRuntimeQ2p2.getInputHandler("profitStream");
        disruptor2.handleEventsWith(new DataHandler(profitStreamInputHandler, false));

        executionPlanRuntimeQ2p2.addCallback("q2outputStream", new StreamCallback() {
            long count = 1;
            long currentTime = 0;
            long latency = 0;

            @Override
            public void receive(Event[] events) {
                //EventPrinter.print(events);
                count = count + events.length;
                System.out.println("query2 output event count" + count);

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
            RingBuffer<Event> ringBuffer2 = disruptor2.start();

            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    long sequenceNo = ringBuffer2.next();
                    try {
                        Event existingEvent = ringBuffer2.get(sequenceNo);
                        existingEvent.setTimestamp(System.currentTimeMillis());
                        existingEvent.setIsExpired(event.isExpired());
                        existingEvent.setData(event.getData());
                    } finally {
                        ringBuffer2.publish(sequenceNo);
                    }
                }
            }
        });


        executionPlanRuntimeQ1p1.start();
        executionPlanRuntimeQ2p2.start();
        executionPlanRuntimeQ2p1.start();


        System.out.println("Data loading started.");
        disruptor.handleEventsWith(new DataHandler(taxiTripsInputHandler, true));
        DataPublisher dataPublisher = new DataPublisher(Config.getConfigurationInfo("org.wso2.siddhi.debs2015.dataset"), disruptor.start());


        //start the data loading process
        dataPublisher.start();


        //Just make the main thread sleep infinitely
        //Note that we cannot have an event based mechanism to exit from this infinit loop.
        // It is because even if the data sending thread has completed its task of sending the data to the SiddhiManager, the SiddhiManager object may be conducting the processing of the remaining
        //data. Furthermore, since this is CEP its better have this type of mechanism, rather than terminating once we are done sending the data to the CEP engine.
        while (true) {
            try {
                Thread.sleep(Constants.MAIN_THREAD_SLEEP_TIME);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    public class DataHandler implements EventHandler<Event> {

        private LinkedBlockingQueue<String> aggregateInputList = new LinkedBlockingQueue<String>();
        private InputHandler inputHandler;
        private Date startDateTime;
        long count = 1;
        long timeDifferenceFromStart = 0;
        long timeDifference = 0; //This is the time difference for this time window.
        long currentTime = 0;
        long prevTime = 0;
        //long startTime = System.currentTimeMillis();
        long startTime = 0;
        long cTime = 0;
        //Special note : Originally we need not subtract 1. However, due to some reason if there are n events in the input data set that are
        //pumped to the eventBufferList queue, only (n-1) is read. Therefore, we have -1 here.
        final int EVENT_COUNT = Integer.parseInt(Config.getConfigurationInfo("org.wso2.siddhi.debs2015.dataset.size")) - 1;

        boolean loggingEnabled = true;
        boolean originalEventStream = false;
        boolean firstEvent = true;
        float percentageCompleted = 0;

        public DataHandler(InputHandler inputHandler, boolean originalEventStream) {
            this.inputHandler = inputHandler;
            this.originalEventStream = originalEventStream;
        }

        public void onEvent(Event event, long sequence, boolean endOfBatch) {
            try {
                if (loggingEnabled) {
                    if (firstEvent) {
                        //We print the start and the end times of the experiment even if the performance logging is disabled.
                        startDateTime = new Date();
                        startTime = startDateTime.getTime();
                        SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd.hh:mm:ss-a-zzz");
                        System.out.println("Started experiment at : " + startTime + "--" + ft.format(startDateTime));
                        firstEvent = false;
                    }

                }

                if (originalEventStream) {    //only for original stream we are injecting the timestamp
                    cTime = System.currentTimeMillis();
                    event.getData()[Constants.INPUT_INJECTION_TIMESTAMP_FIELD] = cTime; //This corresponds to the iij_timestamp
                }

                inputHandler.send(event);

                if (loggingEnabled) {
                    if (count % Constants.STATUS_REPORTING_WINDOW_INPUT == 0) {
                        percentageCompleted = ((float) count / (EVENT_COUNT));
                        currentTime = System.currentTimeMillis();
                        timeDifferenceFromStart = (currentTime - startTime);
                        timeDifference = currentTime - prevTime;
                        //<time from start(ms)><time from start(s)><aggregate throughput (events/s)><throughput in this time window (events/s)><percentage completed (%)>
                        aggregateInputList.add(timeDifferenceFromStart + "," + Math.round(timeDifferenceFromStart / 1000) + "," + Math.round(count * 1000.0 / timeDifferenceFromStart) + "," + Math.round(Constants.STATUS_REPORTING_WINDOW_INPUT * 1000.0 / timeDifference) + "," + percentageCompleted);
                        prevTime = currentTime;
                    }
                    count++;

                    if (count > EVENT_COUNT) {
                        percentageCompleted = ((float) count / EVENT_COUNT);
                        currentTime = System.currentTimeMillis();
                        timeDifferenceFromStart = (currentTime - startTime);
                        timeDifference = currentTime - prevTime;
                        //<time from start(ms)><time from start(s)><aggregate throughput (events/s)><throughput in this time window (events/s)><percentage completed (%)>
                        aggregateInputList.add(timeDifferenceFromStart + "," + Math.round(timeDifferenceFromStart / 1000) + "," + Math.round(count * 1000.0 / timeDifferenceFromStart) + "," + Math.round(Constants.STATUS_REPORTING_WINDOW_INPUT * 1000.0 / timeDifference) + "," + percentageCompleted);

                        //At this moment we are done with sending all the events from the queue. Now we are about to complete the experiment.
                        Date dNow = new Date();
                        SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd.hh:mm:ss-a-zzz");
                        System.out.println("Ended experiment at : " + dNow.getTime() + "--" + ft.format(dNow));
                        System.out.println("Event count : " + count);
                        timeDifferenceFromStart = dNow.getTime() - startDateTime.getTime();
                        System.out.println("Total run time : " + timeDifferenceFromStart);
                        System.out.println("Average input data rate (events/s): " + Math.round((count * 1000.0) / timeDifferenceFromStart));
                        System.out.flush();

                    }
                }

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

}

