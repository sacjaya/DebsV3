/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org)
 * All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.siddhi.debs2015.DesignWithDisruptor;

import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.debs2015.extensions.countEmptyTaxi.EmptyTaxiStreamProcessor;
import org.wso2.siddhi.debs2015.extensions.maxK.MaxKStreamProcessor;
import org.wso2.siddhi.debs2015.extensions.maxK.MaxKStreamProcessorQuery2;
import org.wso2.siddhi.debs2015.extensions.median.BucketingBasedMedianAggregator;
import org.wso2.siddhi.debs2015.extensions.window.CustomExternalTimeWindowProcessor;

import java.util.HashMap;
import java.util.Map;


public class Query1Part1 {


    public ExecutionPlanRuntime addExecutionPlan() {

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiContext siddhiContext = siddhiManager.getSiddhiContext();
        Map<String, Class> extensions = new HashMap<String, Class>();

        extensions.put("MaxK:getMaxK", MaxKStreamProcessor.class);
        extensions.put("debs:median", BucketingBasedMedianAggregator.class);
        extensions.put("debs:extTime", CustomExternalTimeWindowProcessor.class);
        extensions.put("debs:emptyTaxi", EmptyTaxiStreamProcessor.class);
        extensions.put("MaxK:getMaxK2", MaxKStreamProcessorQuery2.class);

        siddhiContext.setSiddhiExtensions(extensions);


        String inputStream = " define stream profitStream ( profit float, startCellNo string, endCellNo string , pickup_datetime long ,  dropoff_datetime long ,  medallion string ," +
                "pickup_datetime_org string, dropoff_datetime_org string , iij_timestamp long ); ";


        //window of 30 min calculate count groupby   startCellNo, endCellNo  -> startCellNo, endCellNo, pickup_datetime, dropoff_datetime, count
        String query1 = "@info(name = 'query1') " +
                "from profitStream#window.debs:extTime(dropoff_datetime , 15 min) " +
                "select startCellNo , endCellNo,  pickup_datetime_org, dropoff_datetime_org, iij_timestamp " +
                "insert all events  into countStream ;";


        String query2 = "@info(name = 'query2') " +
                "from countStream#MaxK:getMaxK(startCellNo, endCellNo, 10, iij_timestamp) " +
                "select pickup_datetime_org, dropoff_datetime_org, startCell1 ,endCell1, startCell2, endCell2, " +
                "startCell3 ,endCell3, startCell4, endCell4, startCell5, endCell5, startCell6, endCell6," +
                "startCell7 ,endCell7 , startCell8, endCell8, startCell9, endCell9, startCell10, endCell10, iij_timestamp " +
                "insert into q1outputStream";


        return siddhiManager.createExecutionPlanRuntime("@Plan:name('Query1')" + inputStream + query1 + query2);

    }

}