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

import org.wso2.siddhi.debs2015.util.Config;
import org.wso2.siddhi.debs2015.util.Constants;

/**
 * @author Miyuru Dayarathna
 *
 */
public class PerformanceMonitoringThreadInput extends Thread {
	FileWriter fw = null;
	BufferedWriter bw = null;
	private LinkedBlockingQueue<String> aggregateInputList;
	
	public PerformanceMonitoringThreadInput(LinkedBlockingQueue<String> aggregateInputList){
		this.aggregateInputList = aggregateInputList;
	}
	
	public void run(){
		String record = null;
		Date dNow = new Date();
		SimpleDateFormat ft = new SimpleDateFormat ("yyyy-MM-dd.hh:mm:ss-a-zzz");
		String timeStamp = ft.format(dNow);
		String statisticsDir = Config.getConfigurationInfo("org.wso2.siddhi.debs2015.experiment.logdir");
		try {
            fw = new FileWriter(new File(statisticsDir + "/input-stats-" + timeStamp + ".csv").getAbsoluteFile());
			bw = new BufferedWriter(fw);
			bw.write("time from start(ms),time from start(s),aggregate throughput (events/s),throughput in this time window (events/s),percentage completed (%)\r\n");
        } catch (IOException e1) {
            e1.printStackTrace();
        }
		
		while(true){
			try {
                record = aggregateInputList.take();
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
			
			if(record != null){
				try {
                    bw.write(record + "\r\n");
                    bw.flush();
                } catch (IOException e) {
                    e.printStackTrace();
                }
			}
			
//			//We sleep for half a second to avoid the CPU core being over utilized.
//			//Since this is just a monitoring thread, there should not be much harm for over all performance in doing so.
//			try {
//                Thread.currentThread().sleep(Constants.MONITORING_THREAD_SLEEP_TIME);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
		}
	}
}
