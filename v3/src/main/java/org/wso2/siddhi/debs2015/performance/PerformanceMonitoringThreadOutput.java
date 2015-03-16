/**
 * 
 */
package org.wso2.siddhi.debs2015.performance;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.LinkedBlockingQueue;

import javax.swing.Timer;

import org.wso2.siddhi.debs2015.util.Config;
import org.wso2.siddhi.debs2015.util.Constants;

import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author Miyuru Dayarathna
 *
 */
public class PerformanceMonitoringThreadOutput extends Thread {
	private FileWriter fw = null;
	private BufferedWriter bw = null;
	private LinkedBlockingQueue<String> aggregateOutputList;
	private Timer timer;
	private int recordCount;
	private int TIMER_TICKING_INTERVAL = 2;//The timer's tick interval in seconds.
	
	public PerformanceMonitoringThreadOutput(LinkedBlockingQueue<String> aggregateOutputList){
		this.aggregateOutputList = aggregateOutputList;
	}
	
	public int getRecordCount(){
		return recordCount;
	}
	
	public void run(){
		String record = null;
		Date dNow = new Date();
		SimpleDateFormat ft = new SimpleDateFormat ("yyyy-MM-dd.hh:mm:ss-a-zzz");
		String timeStamp = ft.format(dNow);
		String statisticsDir = Config.getConfigurationInfo("org.wso2.siddhi.debs2015.experiment.logdir");
		try {
            fw = new FileWriter(new File(statisticsDir + "/output-stats-" + timeStamp + ".csv").getAbsoluteFile());
			bw = new BufferedWriter(fw);
			bw.write("time from start(ms),time from start(s), overall latency (ms/event), latency in this time window (ms/event), overall throughput(events/s), throughput in this time window (events/s)\r\n");
        } catch (IOException e1) {
            e1.printStackTrace();
        }
		
		//We use a timer to see whether we are done with processing the GC data set. This is quite 
		//odd in the context of regular CEP applications. But for making our experiments be more
		//responsive we add such code here.		
//	    final ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
//	    service.scheduleWithFixedDelay(new Watch(this), 0, TIMER_TICKING_INTERVAL, TimeUnit.SECONDS);
		
		while(true){
			try {
                record = aggregateOutputList.take();
            } catch (InterruptedException e1) {
                //If this thread was interrupted while waiting for a record, it implies that its time to exit.
            	break;
            }
			
			//recordCount++;
			
			if(record != null){
				try {
                    bw.write(record + "\r\n");
                    bw.flush();
                } catch (IOException e) {
                    e.printStackTrace();
                }
			}
		}
	}

	public void exitFromRecording() {
		this.interrupt();
    }
}
