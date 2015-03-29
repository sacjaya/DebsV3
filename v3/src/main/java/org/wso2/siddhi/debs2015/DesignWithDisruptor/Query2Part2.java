package org.wso2.siddhi.debs2015.DesignWithDisruptor;

import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.debs2015.extensions.countEmptyTaxi.EmptyTaxiStreamProcessor;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */


public class Query2Part2 {


    public ExecutionPlanRuntime addExecutionPlan() {


        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiContext siddhiContext = siddhiManager.getSiddhiContext();
        Map<String, Class> extensions = new HashMap<String, Class>();

        extensions.put("debs:emptyTaxi", EmptyTaxiStreamProcessor.class);

        siddhiContext.setSiddhiExtensions(extensions);

        //7.5, 284.319, 285.322, 1357287720000, 1357287840000, 8E3E1ED0244393813CB20C8DDE4001BA, 2013-01-04 08:22:00, 2013-01-04 08:24:00, 1427383481957


        String taxiTripStream = " define stream profitStream ( profit float, startCellNo int, endCellNo int , pickup_datetime long ,  dropoff_datetime long ,  medallion string ," +
                "pickup_datetime_org string, dropoff_datetime_org string , iij_timestamp long ); ";


        String emptyTaxiTable = "@IndexBy('cellNo')  " +
                "define table emptyTaxiCountTable (cellNo int , emptyTaxiCount long); ";


        String query1 = " " +
                "from profitStream#debs:emptyTaxi(endCellNo, medallion, dropoff_datetime )  " +
                "select  cellNo , emptyTaxiCount " +
                " insert into taxiCountUpdateStream ;";


        String query2 = "@info(name = 'query2') " +
                "from taxiCountUpdateStream " +
                "select cellNo , sum(emptyTaxiCount) as emptyTaxiCount   " +
                "group by cellNo " +
                "insert into emptyTaxiCountTable ;";


        String query3 = "@info(name = 'query3') " +
                "from profitStream#window.length(0) join emptyTaxiCountTable " +
                "on profitStream.startCellNo == emptyTaxiCountTable.cellNo " +
                "select profitStream.startCellNo as cellNo , profitStream.profit as profit , emptyTaxiCountTable.emptyTaxiCount  as emptyTaxiCount ,  profitStream.pickup_datetime, profitStream.dropoff_datetime, profitStream.iij_timestamp as iij_timestamp " +
                "having  emptyTaxiCountTable.emptyTaxiCount != 0 " +
                "insert into profitRawData ;";

        return siddhiManager.createExecutionPlanRuntime("@Plan:name('Query2-Join')"+taxiTripStream + emptyTaxiTable + query1  +query2+ query3);

    }

}




