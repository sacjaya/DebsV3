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

package org.wso2.siddhi.debs2015.extensions.maxK.util;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class MaxKStoreForStreamProcessor {
    private Map<String, Integer> units = new ConcurrentHashMap<String, Integer>(); //The units Map keeps the trip count for each and every cell. The index is based on the cell ID.
    private Map<Integer, HashSet<String>> maxValues  = new TreeMap<Integer, HashSet<String>>(); //The maxValues Map keeps records for each trip count. The index is based on the trip count.
    //private int eventCounter = 0;
    

    /**
     * Calculated the current top k values by comparing the values that are
     * already stored in the Map.
     *
     * @return A Map that contains the Max-K values
     * @params value - The pressure reading value for the current event
     * @params date - The timestamp the pressure reading was produced.
     *
     */
    public synchronized HashSet<String> getMaxK(String cell, boolean isCurrent, int k) {
//          eventCounter++;
//          
//          if(eventCounter % 200000 == 0){
//          	System.out.println("Number of records in the window : " + maxValues.size());
//          	System.out.println("Number of records in the units HashMap : " + units.size());
//          }

        Integer tripCount = units.get(cell);
        if(tripCount== null){
            tripCount = 0;
        }
        if(isCurrent){
            tripCount++;
        } else {
            tripCount--;
        }
    	
        if(tripCount==0){
        	//We have to make sure that the units contains the cell id we are looking for. Otherwise we may get a NullPointerException.
        	if (units.containsKey(cell)) {
                Integer previousCount = units.get(cell);
                units.remove(cell);
                maxValues.get(previousCount).remove(cell);
        	}
        } else {
        	//Here we have a non-zero trip count.
        	
        	//This code basically updates the count per cell. If there is a new value for the count
        	//which is non-zero, the old value is replaced with the new value.
        	
        	//The maxValues TreeMap holds the list of cells for each count.
        	//"units" is an index of which the key is the cell ID and the value is the count.
        	
        	
        	Integer kkey = units.get(cell);
        	
        	
            if ((kkey!=null)&&(kkey != -1)) {
                Integer previousCount = units.get(cell);
                if(previousCount != tripCount){
                	units.put(cell, tripCount);
                	maxValues.get(previousCount).remove(cell);
                }
                HashSet<String> cellsList = maxValues.get(tripCount);
                if (cellsList != null) {
                    cellsList.add(cell);//Since we use HashSet we need not worry about duplicates here.
                } else {
                    cellsList = new HashSet<String>();
                    cellsList.add(cell);
                    maxValues.put(tripCount, cellsList);
                }
            } else if((kkey==null)||(kkey == -1)) {
                units.put(cell, tripCount);
                HashSet<String> cellsList = maxValues.get(tripCount);
                if (cellsList != null) {
                    cellsList.add(cell);//Since we use HashSet we need not worry about duplicates here.
                    //Since we have a non-null reference here, we need not to put it back to the TreeMap.
                } else {
                    cellsList = new HashSet<String>();
                    cellsList.add(cell);
                    maxValues.put(tripCount, cellsList);
                }
            }
        }

        //By this point we expect to have a TreeMap which has keys corresponding to the number of 
        //trips and values having lists of start:end cells which had that many number of trips.
        
        //E.g.,
        
        //26-->[140.158,145.165,144.164]
        //18-->[146.164]
        //8-->[144.162,147.168,144.165,146.168]
        
        Set<Integer> keySet = maxValues.keySet();//The keyset is the number of unique appearances
        Iterator<Integer> itr = keySet.iterator();
               
        int currentKey = 0;
        int cntr = 0;
        HashSet<String> result = new HashSet<String>();
        
        while(itr.hasNext()){
        	currentKey = itr.next();
        	HashSet<String> currentCells = maxValues.get(currentKey);
        	
        	if(currentCells.size() > 0){
        		Iterator<String> itr2 = currentCells.iterator();
        		
        		while(itr2.hasNext()){
        			result.add(itr2.next());
        			cntr++;
        			
        			if(cntr > k){ //We need to select only the top k most frequent cells only
        				break;
        			}
        		}
        	}
        	
			if(cntr > k){
				break;
			}
        }
        
        // Returns the pressure readings that are sorted in descending order according to the key (pressure value).
        return result;

    }
}