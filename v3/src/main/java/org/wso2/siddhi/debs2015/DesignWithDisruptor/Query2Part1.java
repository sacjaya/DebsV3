package org.wso2.siddhi.debs2015.DesignWithDisruptor;

import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.debs2015.extensions.cellId.CellIdFunctionForQuery2;
import org.wso2.siddhi.debs2015.extensions.countEmptyTaxi.EmptyTaxiStreamProcessor;
import org.wso2.siddhi.debs2015.extensions.maxK.MaxKTimeTransformerForQuery2;
import org.wso2.siddhi.debs2015.extensions.median.BucketingBasedMedianAggregator;
import org.wso2.siddhi.debs2015.extensions.timeStamp.TimeStampFunction;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */


public class Query2Part1 {

    public ExecutionPlanRuntime addExecutionPlan() {

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiContext siddhiContext = siddhiManager.getSiddhiContext();
        Map<String, Class> extensions = new HashMap<String, Class>();

        extensions.put("debs:cellId", CellIdFunctionForQuery2.class);
        extensions.put("debs:getTimestamp", TimeStampFunction.class);
        extensions.put("debs:median", BucketingBasedMedianAggregator.class);
        extensions.put("MaxK:getMaxK", MaxKTimeTransformerForQuery2.class);
        extensions.put("debs:emptyTaxi", EmptyTaxiStreamProcessor.class);

        siddhiContext.setSiddhiExtensions(extensions);

        //07290D3599E7A0D62097A346EFCC1FB5,E7750A37CAB07D0DFF0AF7E3573AC141,2013-01-01 00:00:00,2013-01-01 00:02:00,120,0.44,-73.956528,40.716976,-73.962440,40.715008,CSH,3.50,0.50,0.50,0.00,0.0

        //an md5sum of the identifier of the taxi - vehicle bound
        //an md5sum of the identifier for the taxi license
        //time when the passenger(s) were picked up
        //time when the passenger(s) were dropped off
        //duration of the trip (in seconds)
        //trip distance in miles
        //longitude coordinate of the pickup location
        //latitude coordinate of the pickup location
        //longitude coordinate of the drop-off location
        //latitude coordinate of the drop-off location
        //the payment method - credit card or cash
        //fare amount in dollars
        //surcharge in dollars
        //tax in dollars
        //tip in dollars
        //bridge and tunnel tolls in dollars
        //total paid amount in dollars
        // This is an additional field used to indicate the time when the event has been injected to the query network.

        String taxiTripStream = "define stream taxi_trips ( medallion string , hack_license string ,  pickup_datetime_org string, dropoff_datetime_org string , trip_time_in_secs int, " +
                "trip_distance float, pickup_longitude float,  pickup_latitude float,  dropoff_longitude float,  dropoff_latitude float, fare_plus_ip_amount float, iij_timestamp long); ";

        //The profit that originates from an area is computed by calculating the median fare + tip for trips that started in the area and ended within the last 15 minutes


        //Output stream is of the format : cell_based_taxi_trips(startCellNo, endCellNo, pickup_datetime, dropoff_datetime, fare_amount, tip_amount, medallion, iij_timestamp)
        String query1 = " @info(name = 'query1') " +
                "from taxi_trips " +
                "select debs:cellId(pickup_longitude,pickup_latitude) as startCellNo, debs:cellId(dropoff_longitude,dropoff_latitude) as endCellNo , " +
                "debs:getTimestamp(pickup_datetime_org) as pickup_datetime , debs:getTimestamp(dropoff_datetime_org) as dropoff_datetime, fare_plus_ip_amount," +
                " medallion, pickup_datetime_org, dropoff_datetime_org,  iij_timestamp " +
                " insert into cell_based_taxi_trips ;";

        //get profit
        //Output stream is of the format : profitStream(profit, startCellNo, pickup_datetime, dropoff_datetime, iij_timestamp)
        //window of 15 min calculate profit  groupby   startCellNo  -> profit, startCellNo, pickup_datetime, dropoff_datetime, iij_timestamp
        String query2 = "@info(name = 'query2') " +
                "from cell_based_taxi_trips#window.externalTime(dropoff_datetime , 15 min)  " +
                "select debs:median(fare_plus_ip_amount) as profit, startCellNo, endCellNo, pickup_datetime, dropoff_datetime, " +
                "medallion, pickup_datetime_org, dropoff_datetime_org,  iij_timestamp  " +
                "group by startCellNo " +
                "insert all events  into profitStream ;";


        return siddhiManager.createExecutionPlanRuntime("@Plan:name('Query2-median')"+taxiTripStream + query1 + query2 );


    }

}




