package org.wso2.siddhi.debs2015.DesignWithDisruptor.util;

import org.wso2.siddhi.core.event.Event;

/**
 * Created by sachini on 3/26/15.
 */
public class SiddhiFactory implements com.lmax.disruptor.EventFactory<Event> {

    public Event newInstance() {
        return new Event();
    }

}
