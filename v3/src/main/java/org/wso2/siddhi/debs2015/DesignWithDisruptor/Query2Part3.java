/*
 * Copyright (c) 2005 - 2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.wso2.siddhi.debs2015.DesignWithDisruptor;

import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.debs2015.extensions.maxK.MaxKStreamProcessorQuery2;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */


public class Query2Part3 {


    public ExecutionPlanRuntime addExecutionPlan() {


        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiContext siddhiContext = siddhiManager.getSiddhiContext();
        Map<String, Class> extensions = new HashMap<String, Class>();

        extensions.put("MaxK:getMaxK2", MaxKStreamProcessorQuery2.class);

        siddhiContext.setSiddhiExtensions(extensions);

        //7.5, 284.319, 285.322, 1357287720000, 1357287840000, 8E3E1ED0244393813CB20C8DDE4001BA, 2013-01-04 08:22:00, 2013-01-04 08:24:00, 1427383481957


        String profitRawData = " define stream profitRawData (cellNo int, profit float, emptyTaxiCount int, profitability float, pickup_datetime_org string ,  dropoff_datetime_org string ," +
                " iij_timestamp long ); ";


        String query1 = "@info(name = 'query1') " +
                "from profitRawData#MaxK:getMaxK2(profitability, profit, emptyTaxiCount, cellNo,10, iij_timestamp) " +
                "select  pickup_datetime_org, dropoff_datetime_org, " +
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


        return siddhiManager.createExecutionPlanRuntime("@Plan:name('Query2-MaxK')"+profitRawData +  query1);


    }

}




