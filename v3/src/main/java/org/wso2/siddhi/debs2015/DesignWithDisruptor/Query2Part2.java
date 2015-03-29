package org.wso2.siddhi.debs2015.DesignWithDisruptor;

import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.debs2015.extensions.countEmptyTaxi.EmptyTaxiStreamProcessor;
import org.wso2.siddhi.debs2015.extensions.maxK.MaxKStreamProcessorQuery2;

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
        extensions.put("MaxK:getMaxK2", MaxKStreamProcessorQuery2.class);

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
                "   insert into profitRawData;";


        String query4 = "@info(name = 'query4') " +
                "from profitRawData[emptyTaxiCount != 0] " +
                "select cellNo, profit, emptyTaxiCount,  pickup_datetime, dropoff_datetime, profit/emptyTaxiCount as profit_per_taxi, iij_timestamp " +
                "insert into finalProfitStream;";


        String query5 = "@info(name = 'query5') " +
                "from finalProfitStream#MaxK:getMaxK2(profit_per_taxi, profit, emptyTaxiCount, cellNo,10, iij_timestamp) " +
                "select  pickup_datetime, dropoff_datetime, " +
                "profitable_cell_id_1, empty_taxies_in_cell_id_1, median_profit_in_cell_id_1, profitability_of_cell_1," +
                "profitable_cell_id_2, empty_taxies_in_cell_id_2, median_profit_in_cell_id_2, profitability_of_cell_2," +
                "profitable_cell_id_3, empty_taxies_in_cell_id_3, median_profit_in_cell_id_3, profitability_of_cell_3," +
                "profitable_cell_id_4, empty_taxies_in_cell_id_4, median_profit_in_cell_id_4, profitability_of_cell_4," +
                "profitable_cell_id_5, empty_taxies_in_cell_id_5, median_profit_in_cell_id_5, profitability_of_cell_5," +
                "profitable_cell_id_6, empty_taxies_in_cell_id_6, median_profit_in_cell_id_6, profitability_of_cell_6," +
                "profitable_cell_id_7, empty_taxies_in_cell_id_7, median_profit_in_cell_id_7, profitability_of_cell_7," +
                "profitable_cell_id_8, empty_taxies_in_cell_id_8, median_profit_in_cell_id_8, profitability_of_cell_8," +
                "profitable_cell_id_9, empty_taxies_in_cell_id_9, median_profit_in_cell_id_9, profitability_of_cell_9," +
                "profitable_cell_id_10, empty_taxies_in_cell_id_10, median_profit_in_cell_id_10, profitability_of_cell_10, iij_timestamp " +
                "insert into q2outputStream;";


        return siddhiManager.createExecutionPlanRuntime("@Plan:name('Query2-Join')"+taxiTripStream + emptyTaxiTable + query1 + query2 + query3 + query4 + query5);


    }

}




