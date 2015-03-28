/**
 *
 */
package org.wso2.siddhi.debs2015.input;

import com.google.common.base.Splitter;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author Miyuru Dayarathna
 */
public class DataLoderThread1 extends Thread {
    private String fileName;
    private static Splitter splitter = Splitter.on(',');
    private LinkedBlockingQueue<Object> eventBufferList;
    private BufferedReader br;
    private int count;

    public DataLoderThread1(String fileName, LinkedBlockingQueue<Object> eventBuffer) {
        this.fileName = fileName;
        this.eventBufferList = eventBuffer;
    }

    public void run() {
        try {

            long startTime = System.currentTimeMillis();


            FileInputStream f = new FileInputStream(fileName);
//            BufferedInputStream bf = new BufferedInputStream(f);

            FileChannel ch = f.getChannel();
            MappedByteBuffer mb = ch.map(FileChannel.MapMode.READ_ONLY,
                    0L, ch.size());

            int size = 2000;
            byte comma = ',';
            byte newLine = '\n';

            byte[] barray = new byte[size];

            int lastLine = size;

//            int nGet =0;
//            while (bf.()) {
            while (mb.hasRemaining()) {

                int copySize = size - lastLine;
//                System.out.println(copySize);
                System.arraycopy(barray, lastLine, barray, 0, copySize);

//                System.out.println(new String(barray));
                int dataToFetch = Math.min(mb.remaining(), size - copySize);
                mb.get(barray, copySize, dataToFetch);

                int nGet = copySize + dataToFetch;
//                StringBuilder stringBuilder=new StringBuilder(new CharArray()barray)
//                System.out.println(new String(barray));
                lastLine = 0;
                int last = 0;
//                System.out.println("lastLine " + lastLine);


                while (true) {
                    if (nGet < last + 106) {
                        break;
                    }
//                    Object[] eventData = new Object[13];

//                    medallion,
//                            hack_license,
//                            pickup_datetime,
//                            dropoff_datetime,
                    String medallion = new String(barray, last, 32, StandardCharsets.UTF_8);
                    last += 33;
//                    if (nGet < last + 32) {
//                        break;
//                    }
                    String hack_license  = new String(barray, last, 32, StandardCharsets.UTF_8);
                    last += 33;
//                    if (nGet < last + 19) {
//                        break;
//                    }
                    String pickup_datetime = new String(barray, last, 19, StandardCharsets.UTF_8);
                    last += 20;
//                    if (nGet < last + 19) {
//                        break;
//                    }
                    String dropoff_datetime = new String(barray, last, 19, StandardCharsets.UTF_8);
                    last += 20;

//                    int commaIndex = 4;
//                    int dataIndex = 4;
//
//                    for (int i = last; i < nGet; i++) {
//                        if (comma == barray[i]) {
//                            if (commaIndex == 4) {
//                                eventData[dataIndex] = new Short(new String(barray, last, i - last, StandardCharsets.UTF_8));
//                                dataIndex++;
//                                commaIndex++;
//                            } else if (commaIndex == 10 || commaIndex == 12 || commaIndex == 13 || commaIndex > 14) {
//                                commaIndex++;
//                            } else {
//                                eventData[dataIndex] = new Float(new String(barray, last, i - last, StandardCharsets.UTF_8));
//                                dataIndex++;
//                                commaIndex++;
//                            }
////                            trip_time_in_secs = new Short(new String(barray, last, i - last, StandardCharsets.UTF_8));
//                            last = i + 1;
////                            break;
//                        } else if (newLine == barray[i]) {
////                            if(dataIndex==4){
////                                eventData[dataIndex]= new Short(new String(barray, last, i - last, StandardCharsets.UTF_8));
////                                dataIndex++;
////                            }  else if(dataIndex==10||dataIndex==12||dataIndex==13){
////                                eventData[dataIndex] = new Float(new String(barray, last, i - last, StandardCharsets.UTF_8));
////                            } else if(){
////
////                            }
////                            trip_time_in_secs = new Short(new String(barray, last, i - last, StandardCharsets.UTF_8));
//                            last = i + 1;
//                            break;
//                        }
//                    }
//                    if (eventData[11] == null) {
//                        break;
//                    }

                    Short trip_time_in_secs = null;
                    for (int i = last; i < nGet; i++) {
                        if (comma == barray[i]) {
                            trip_time_in_secs = new Short(new String(barray, last, i - last, StandardCharsets.UTF_8));
                            last = i + 1;
                            break;
                        }
                    }
                    if (trip_time_in_secs == null) {
                        break;
                    }

                    Float trip_distance = null;
                    for (int i = last; i < nGet; i++) {
                        if (comma == barray[i]) {
                            trip_distance = new Float(new String(barray, last, i - last, StandardCharsets.UTF_8));
                            last = i + 1;
                            break;
                        }
                    }
                    if (trip_distance == null) {
                        break;
                    }

                    Float pickup_longitude = null;
                    for (int i = last; i < nGet; i++) {
                        if (comma == barray[i]) {
                            pickup_longitude = new Float(new String(barray, last, i - last, StandardCharsets.UTF_8));
                            last = i + 1;
                            break;
                        }
                    }
                    if (pickup_longitude == null) {
                        break;
                    }


                    Float pickup_latitude = null;
                    for (int i = last; i < nGet; i++) {
                        if (comma == barray[i]) {
                            pickup_latitude = new Float(new String(barray, last, i - last, StandardCharsets.UTF_8));
                            last = i + 1;
                            break;
                        }
                    }
                    if (pickup_latitude == null) {
                        break;
                    }

                    Float dropoff_longitude = null;
                    for (int i = last; i < nGet; i++) {
                        if (comma == barray[i]) {
                            dropoff_longitude = new Float(new String(barray, last, i - last, StandardCharsets.UTF_8));
                            last = i + 1;
                            break;
                        }
                    }
                    if (dropoff_longitude == null) {
                        break;
                    }

                    Float dropoff_latitude = null;
                    for (int i = last; i < nGet; i++) {
                        if (comma == barray[i]) {
                            dropoff_latitude = new Float(new String(barray, last, i - last, StandardCharsets.UTF_8));
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

                    Float fare_amount = null;
                    for (int i = last; i < nGet; i++) {
                        if (comma == barray[i]) {
                            fare_amount = new Float(new String(barray, last, i - last, StandardCharsets.UTF_8));
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

                    Float tip_amount = null;
                    for (int i = last; i < nGet; i++) {
                        if (comma == barray[i]) {
                            tip_amount = new Float(new String(barray, last, i - last, StandardCharsets.UTF_8));
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

                    Object[] eventData = new Object[]{medallion,
                            hack_license,
                            pickup_datetime,
                            dropoff_datetime,
                            trip_time_in_secs,
                            trip_distance, //This can be represented by two bytes
                            pickup_longitude,
                            pickup_latitude,
                            dropoff_longitude,
                            dropoff_latitude,
                            //Boolean.parseBoolean(payment_type),
                            fare_amount, //These currency values can be coded to two bytes
                            //Float.parseFloat(surcharge),
                            //Float.parseFloat(mta_tax),
                            tip_amount,
                            //Float.parseFloat(tolls_amount),
                            //Float.parseFloat(total_amount),
                            0l}; //We need to attach the time when we are injecting an event to the query network. For that we have to set a separate field which will be populated when we are injecting an event to the input stream.
//
//                    eventData[12] = 0l;
                    lastLine = last;
//                    eventBufferList.put(eventData);
////                        line = br.readLine();
                    count++;
//                    System.out.println("lastLine " + lastLine);
//                    System.out.println(lastLine + " " + last//
                    System.out.println(Arrays.deepToString(eventData));
                }
//                String pickup_longitude = scanner.next();
//                String pickup_latitude = scanner.next();
//                String dropoff_longitude = scanner.next();
//                String dropoff_latitude = scanner.next();


//                ByteBuffer byteBuffer = ByteBuffer.wrap(barray);
//                byteBuffer.get(last);

//                String medallion= loadData()
//                String medallion =new String (byteBuffer.get(new byte[16]).array());
//                byteBuffer.getChar();
//                last += 33;
//                String hack_license = new String (byteBuffer.get(new byte[32]).array());
//                byteBuffer.getChar();
//                String pickup_datetime = new String (byteBuffer.get(new byte[19]).array());
//                byteBuffer.getChar();
//                String dropoff_datetime =new String (byteBuffer.get(new byte[19]).array());
//                byteBuffer.getChar();

//                Short trip_time_in_secs=byteBuffer.getShort();
//                byteBuffer.getChar();
//                for (int i = last; i < nGet; i++) {
//                    if (comma == barray[i]) {
//                        ByteBuffer byteBuffer = ByteBuffer.wrap(barray, last, i - last);
//
//                        trip_time_in_secs = byteBuffer.get();
//                    }
//                }


//                String trip_distance = scanner.next();
//                String pickup_longitude = scanner.next();
//                String pickup_latitude = scanner.next();
//                String dropoff_longitude = scanner.next();
//                String dropoff_latitude = scanner.next();
//                //String payment_type = dataStrIterator.next();
//                scanner.next();
//                String fare_amount = scanner.next();
//                //String surcharge = dataStrIterator.next();
//                //String mta_tax = dataStrIterator.next();
//                scanner.next();
//                scanner.next();
//                String tip_amount = scanner.next();
//                //String tolls_amount = dataStrIterator.next();
//                //String total_amount = dataStrIterator.next();
//                scanner.next();
//                scanner.next();
////
//
//                for (; i < nGet; i++)
//                    if (comma == barray[i]) {
//                        new String(barray, last, i, StandardCharsets.UTF_8);
//                        ByteBuffer bbuf = ByteBuffer.wrap(fixedMessageData, 0, fixedMessageData.length);
//
//                    } else if ()

            }

//            FileInputStream fin = new FileInputStream(new File(fileName));
//            BufferedReader bf= new BufferedReader(new FileReader(new File(fileName)));
////            java.util.Scanner scanner = new java.util.Scanner(fin,"UTF-8").useDelimiter("\\A");
//            Scanner scanner = new Scanner(new File(fileName),"UTF-8").useDelimiter("[,\\n]");
////            String theString = scanner.hasNext() ? scanner.next() : "";
//
//
//            while (scanner.hasNext()) {
//                    	//We make an assumption here that we do not get empty strings due to missing values that may present in the input data set.
////                        Iterator<String> dataStrIterator = splitter.split(line).iterator();
//                        String medallion = scanner.next();
//                        String hack_license = scanner.next();
//                        String pickup_datetime = scanner.next();
//                        String dropoff_datetime = scanner.next();
//                        String trip_time_in_secs = scanner.next();
//                        String trip_distance = scanner.next();
//                        String pickup_longitude = scanner.next();
//                        String pickup_latitude = scanner.next();
//                        String dropoff_longitude = scanner.next();
//                        String dropoff_latitude = scanner.next();
//                        //String payment_type = dataStrIterator.next();
//                        scanner.next();
//                        String fare_amount = scanner.next();
//                        //String surcharge = dataStrIterator.next();
//                        //String mta_tax = dataStrIterator.next();
//                        scanner.next();
//                        scanner.next();
//                        String tip_amount = scanner.next();
//                        //String tolls_amount = dataStrIterator.next();
//                        //String total_amount = dataStrIterator.next();
//                        scanner.next();
//                        scanner.next();
//
//                        Object[] eventData = null;
//
//                        try{
//                        eventData = new Object[]{		  medallion,
//                                                          hack_license ,
//                                                          pickup_datetime,
//                                                          dropoff_datetime,
//                                                          Short.parseShort(trip_time_in_secs),
//                                                          Float.parseFloat(trip_distance), //This can be represented by two bytes
//                                                          Float.parseFloat(pickup_longitude),
//                                                          Float.parseFloat(pickup_latitude),
//                                                          Float.parseFloat(dropoff_longitude),
//                                                          Float.parseFloat(dropoff_latitude),
//                                                          //Boolean.parseBoolean(payment_type),
//                                                          Float.parseFloat(fare_amount), //These currency values can be coded to two bytes
//                                                          //Float.parseFloat(surcharge),
//                                                          //Float.parseFloat(mta_tax),
//                                                          Float.parseFloat(tip_amount),
//                                                          //Float.parseFloat(tolls_amount),
//                                                          //Float.parseFloat(total_amount),
//                                                          0l}; //We need to attach the time when we are injecting an event to the query network. For that we have to set a separate field which will be populated when we are injecting an event to the input stream.
//                        }catch(NumberFormatException e){
//                        	//e.printStackTrace();
//                        	//If we find a discrepancy in converting data, then we have to discard that
//                        	//particular event.
////                        	line = br.readLine();
//                        	continue;
//                        }
//
//                        //We keep on accumulating data on to the event queue.
//                        //This will get blocked if the space required is not available.
//                        eventBufferList.put(eventData);
////                        line = br.readLine();
//                        count++;
//            }
//
////            System.out.println(theString);
//            scanner.close();


//            int split=',';
//            br = new BufferedReader(new FileReader(fileName), 1000);
//            String line = br.readLine();
//            while (line != null) {
            //We make an assumption here that we do not get empty strings due to missing values that may present in the input data set.
//               String[] words= line.split(",");
//                StringTokenizer stringTokenizer= new StringTokenizer(line,",")
//                        Iterator<String> dataStrIterator = splitter.split(line).iterator();

//                        String medallion =                 line.substring(0,split); ;
//                        String hack_license =                 line.substring(0,split); ;
//                        String pickup_datetime =                 line.substring(0,split); ;
//                        String dropoff_datetime =                 line.substring(0,split); ;
//                        String trip_time_in_secs =                 line.substring(0,split); ;
//                        String trip_distance =                 line.substring(0,split); ;
//                        String pickup_longitude =                 line.substring(0,split); ;
//                        String pickup_latitude =                 line.substring(0,split); ;
//                        String dropoff_longitude =                 line.substring(0,split); ;
//                        String dropoff_latitude =                 line.substring(0,split); ;
//                        //String payment_type =                 line.substring(0,split); ;
//                                        line.substring(0,split); ;
//                        String fare_amount =                 line.substring(0,split); ;
//                        //String surcharge =                 line.substring(0,split); ;
//                        //String mta_tax =                 line.substring(0,split); ;
//                                        line.substring(0,split); ;
//                                        line.substring(0,split); ;
//                        String tip_amount =                 line.substring(0,split); ;
//                        //String tolls_amount =                 line.substring(0,split); ;
//                        //String total_amount =                 line.substring(0,split); ;
//
//                        Object[] eventData = null;
//
//                        try{
//                        eventData = new Object[]{		  medallion,
//                                                          hack_license ,
//                                                          pickup_datetime,
//                                                          dropoff_datetime,
//                                                          Short.parseShort(trip_time_in_secs),
//                                                          Float.parseFloat(trip_distance), //This can be represented by two bytes
//                                                          Float.parseFloat(pickup_longitude),
//                                                          Float.parseFloat(pickup_latitude),
//                                                          Float.parseFloat(dropoff_longitude),
//                                                          Float.parseFloat(dropoff_latitude),
//                                                          //Boolean.parseBoolean(payment_type),
//                                                          Float.parseFloat(fare_amount), //These currency values can be coded to two bytes
//                                                          //Float.parseFloat(surcharge),
//                                                          //Float.parseFloat(mta_tax),
//                                                          Float.parseFloat(tip_amount),
//                                                          //Float.parseFloat(tolls_amount),
//                                                          //Float.parseFloat(total_amount),
//                                                          0l}; //We need to attach the time when we are injecting an event to the query network. For that we have to set a separate field which will be populated when we are injecting an event to the input stream.
//                        }catch(NumberFormatException e){
//                        	//e.printStackTrace();
//                        	//If we find a discrepancy in converting data, then we have to discard that
//                        	//particular event.
//                        	line = br.readLine();
//                        	continue;
//                        }

            //We keep on accumulating data on to the event queue.
            //This will get blocked if the space required is not available.
//                        eventBufferList.put(eventData);
//                        line = br.readLine();
//                        count++;
//            }
            long endTime = System.currentTimeMillis();
            System.out.println("Total amount of events read : " + count + " in: " + (endTime - startTime));
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

    private static byte[] loadData(BufferedInputStream in, byte[] dataArray) throws IOException {

        int start = 0;
        while (true) {
            int readCount = in.read(dataArray, start, dataArray.length - start);
            if (readCount != -1) {
                start += readCount;
                if (start == dataArray.length) {
                    return dataArray;
                }
            } else {
                throw new EOFException("Connection closed from remote end.");
            }
        }
    }
}
