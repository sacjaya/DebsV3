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
    //private Map<Double, LinkedList<CustomObj>> maxValues  = new TreeMap<Double, LinkedList<CustomObj>>();
    
    private Map<Double, ArrayList<CustomObj>> maxValues  = new TreeMap<Double, ArrayList<CustomObj>>();
    
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
    public synchronized LinkedList<CustomObj> getMaxK(CustomObj customObj, int k) {
        String cell = customObj.getCellID();
        Double count = (Double) customObj.getProfit_per_taxi();
        
        if(count==0){
        	//We know by default, if the count==0 for this particular cell, we already have stored this
        	//cell's information before
        	Double previousCount = units.remove(cell);
        	
            if(previousCount != null){
            	units.remove(cell);
            	maxValues.get(previousCount).remove(customObj);
            }
        } else {
        	//This code basically updates the count per cell. If there is a new value for the count
        	//which is non-zero, the old value is replaced with the new value.

        	//The maxValues TreeMap holds the list of cells for each count.
        	//"units" is an index of which the key is the cell ID and the value is the count.

        	Double kkey = units.get(cell);
        	
            if (kkey != null) {
                units.put(cell, count);
                
                maxValues.get(kkey).remove(customObj);
                
                ArrayList<CustomObj> cellsList = maxValues.get(count);
                
                if (cellsList != null) {
                    cellsList.add(customObj);
                } else {
                    cellsList = new ArrayList<CustomObj>();
                    cellsList.add(customObj);
                    maxValues.put(count, cellsList);
                }
            } else {
                units.put(cell, count);
                ArrayList<CustomObj> cellsList = maxValues.get(count);
                if (cellsList != null) {
                    cellsList.add(customObj);
                } else {
                    cellsList = new ArrayList<CustomObj>();
                    cellsList.add(customObj);
                    maxValues.put(count, cellsList);
                }
            }
        }

        LinkedList<CustomObj> result = new LinkedList<CustomObj>();
        Set<Map.Entry<Double, ArrayList<CustomObj>>> entrySet = ((TreeMap)maxValues).entrySet();//The keyset is the number of unique appearances
        Iterator<Map.Entry<Double, ArrayList<CustomObj>>> itr = entrySet.iterator();
        
        Map.Entry<Double, ArrayList<CustomObj>> currentEntry = null;
        int cntr = 0;
        
        while(itr.hasNext()){
        	currentEntry = itr.next();
        	ArrayList<CustomObj> currentCells = currentEntry.getValue();
        	
        	int currentCellSize = currentCells.size();
        	
        	if(currentCells.size() > 0){
        		
        		for (int i = currentCellSize - 1; i >= 0; i--) {
                    result.add(currentCells.get(i));
                    cntr++;

        			if(cntr > k){ //We need to select only the top k most frequent cells only
        				break;
        			}
        		}
        	}
        	
        	//Just to makesure we exit from iterating the TreeMap structure once we found the top-K number of cells.
			if(cntr > k){
				break;
			}
        	
        }       
                
        return result;
    }


}

