package org.wso2.siddhi.debs2015.handlerChaining.processors;

import org.wso2.siddhi.debs2015.handlerChaining.DebsEvent;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

public class ExternalTimeWindowCustomProcessor {
    private long timeToKeep;
    LinkedBlockingQueue<DebsEvent> expiredEventQueue = new LinkedBlockingQueue<DebsEvent>();

    public ExternalTimeWindowCustomProcessor(int timeToKeep) {
        this.timeToKeep = timeToKeep;
    }

    public List<DebsEvent> process(DebsEvent debsEvent) {
        List<DebsEvent> returnList = new ArrayList<DebsEvent>();

        long currentTime =  debsEvent.getDropoff_datetime();

        DebsEvent clonedEvent = null;
            if (!debsEvent.isCurrent()) {
                clonedEvent = debsEvent.clone();
                clonedEvent.setCurrent(false);
                clonedEvent.setTimeStamp(currentTime + timeToKeep);
            }

        Iterator<DebsEvent> iterator = expiredEventQueue.iterator();

        while (iterator.hasNext()){
            DebsEvent expiredEvent =iterator.next();
            long timeDiff = expiredEvent.getTimeStamp() - currentTime;
            if (timeDiff <= 0) {
                iterator.remove();
                returnList.add(expiredEvent);
            } else {
                break;
            }
        }


            if (!debsEvent.isCurrent()) {
                this.expiredEventQueue.add(clonedEvent);
            }   else {
                returnList.add(debsEvent);
            }

        return returnList;
    }

}
