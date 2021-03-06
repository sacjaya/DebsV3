/**
 * 
 */
package org.wso2.siddhi.debs2015.input;

import com.google.common.base.Splitter;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author Miyuru Dayarathna
 *
 */
public class DataLoderThread extends Thread {
	private String fileName;
	private static Splitter splitter = Splitter.on(',');
	private LinkedBlockingQueue<Object[]> eventBufferList;
	private BufferedReader br;
	private int count;
	
	public DataLoderThread(String fileName, LinkedBlockingQueue<Object[]> eventBuffer){
        super("Data Loader");
        this.fileName = fileName;
		this.eventBufferList = eventBuffer;
	}
	
	public void run(){
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


                    	
                        Object[] eventData = null;
                        
                        try{
                        eventData = new Object[]{		  medallion, 
                                                          hack_license , 
                                                          pickup_datetime, 
                                                          dropoff_datetime, 
                                                          Short.parseShort(trip_time_in_secs), 
                                                          Float.parseFloat(trip_distance), //This can be represented by two bytes
                                                          Float.parseFloat(pickup_longitude), 
                                                          Float.parseFloat(pickup_latitude), 
                                                          Float.parseFloat(dropoff_longitude), 
                                                          Float.parseFloat(dropoff_latitude),
                                                          Float.parseFloat(fare_amount), //These currency values can be coded to two bytes
                                                          //Float.parseFloat(surcharge),
                                                          //Float.parseFloat(mta_tax),
                                                          Float.parseFloat(tip_amount),
                                                          //Float.parseFloat(tolls_amount),
                                                          //Float.parseFloat(total_amount),
                                                          0l}; //We need to attach the time when we are injecting an event to the query network. For that we have to set a separate field which will be populated when we are injecting an event to the input stream. 
                        }catch(NumberFormatException e){
                        	//e.printStackTrace();
                        	//If we find a discrepancy in converting data, then we have to discard that
                        	//particular event.
                        	line = br.readLine();
                        	continue;
                        }

                        //We keep on accumulating data on to the event queue.
                        //This will get blocked if the space required is not available.
                        eventBufferList.put(eventData);
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
	
//	public static Float customParseFloat(String input){
//		
//	}
}
