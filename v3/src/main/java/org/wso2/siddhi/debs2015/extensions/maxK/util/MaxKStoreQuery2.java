/*
*  Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package org.wso2.siddhi.debs2015.extensions.maxK.util;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class MaxKStoreQuery2 {
    //Holds the Max K readings
    private Map<String, Double> units = new ConcurrentHashMap<String, Double>();
    private Map<Double, List<CustomObj>> maxValues  = new TreeMap<Double, List<CustomObj>>();
    //No of data to be held in the Map: The value of K

    /**
     * Calculated the current top k values by comparing the values that are
     * already stored in the Map.
     *
     * @return A Map that contains the Max-K values
     * @params value - The pressure reading value for the current event
     * @params date - The timestamp the pressure reading was produced.
     *
     */
    public synchronized Map<Double, List<CustomObj>> getMaxK(CustomObj customObj) {
        String cell = customObj.getCellID();
        Double count = (Double) customObj.getProfit_per_taxi();
        if(count==0){
        	//We have to make sure that the units contains the cell id we are looking for. Otherwise we may get a NullPointerException.
        	if (units.containsKey(cell)) {
                Double previousCount = units.get(cell);
                units.remove(cell);
                maxValues.get(previousCount).remove(customObj);
        	}
        } else {
        	//This code basically updates the count per cell. If there is a new value for the count
        	//which is non-zero, the old value is replaced with the new value.

        	//The maxValues TreeMap holds the list of cells for each count.
        	//"units" is an index of which the key is the cell ID and the value is the count.

            if (units.containsKey(cell)) {
                Double previousCount = units.get(cell);
                units.put(cell, count);
                maxValues.get(previousCount).remove(customObj);
                List<CustomObj> cellsList = maxValues.get(count);
                if (cellsList != null) {
                    cellsList.add(customObj);
                } else {
                    cellsList = new ArrayList<CustomObj>();
                    cellsList.add(customObj);
                    maxValues.put(count, cellsList);
                }
            } else {
                units.put(cell, count);
                List<CustomObj> cellsList = maxValues.get(count);
                if (cellsList != null) {
                    cellsList.add(customObj);
                } else {
                    cellsList = new ArrayList<CustomObj>();
                    cellsList.add(customObj);
                    maxValues.put(count, cellsList);
                }
            }
        }

        // Returns the pressure readings that are sorted in descending order according to the key (pressure value).
        return new TreeMap<Double, List<CustomObj>>(maxValues).descendingMap();

    }


}

