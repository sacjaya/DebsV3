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
    private Map<String, Integer> routeFrequencies = new ConcurrentHashMap<String, Integer>(); //The units Map keeps the trip count for each and every cell. The index is based on the cell ID.
    private Map<Integer, ArrayList<String>> reverseLookup  = new TreeMap<Integer, ArrayList<String>>();
    private long count;

    /**
     * Calculated the current top k values by comparing the values that are
     * already stored in the Map.
     *
     * @return A Map that contains the Max-K values
     * @params value - The pressure reading value for the current event
     * @params date - The timestamp the pressure reading was produced.
     *
     */
    public LinkedList<String> getMaxK(String cell, boolean isCurrent, int k) {
        CustomObjQuery1 customObjQ1 = new CustomObjQuery1(cell,count++);
        Integer tripCount = routeFrequencies.get(cell);
        if(tripCount== null){
            tripCount = 0;
        }
        if(isCurrent){
            tripCount++;
        } else {
            tripCount--;
        }

        if(tripCount==0){
            Integer previousCount= routeFrequencies.remove(cell);

            if (previousCount!=null) {
                reverseLookup.get(previousCount).remove(customObjQ1);
            }
        } else {
            //Here we have a non-zero trip count.

            //This code basically updates the count per cell. If there is a new value for the count
            //which is non-zero, the old value is replaced with the new value.

            //The maxValues TreeMap holds the list of cells for each count.
            //"units" is a reverse index of which the key is the cell ID and the value is the count.


            

//            if ((key!=null)) {
                Integer previousCount = routeFrequencies.get(cell);

                routeFrequencies.put(cell, tripCount);
                reverseLookup.get(previousCount).remove(customObjQ1);
                ArrayList<String> cellsList = reverseLookup.get(tripCount);

                if (cellsList != null) {
                    if(cellsList.size()==10)
                        cellsList.remove(0);
                    cellsList.add(cell);
                } else {
                    cellsList = new ArrayList<String>();
                    cellsList.add(cell);
                    reverseLookup.put(tripCount, cellsList);
                }
//            } else {
//                routeFrequencies.put(cell, tripCount);
//                ArrayList<String> cellsList = reverseLookup.get(tripCount);
//                if (cellsList != null) {
//                    if(cellsList.size()==10)
//                        cellsList.remove(0);
//                    cellsList.add(cell);//Since we use HashSet we need not worry about duplicates here.
//                    //Since we have a non-null reference here, we need not to put it back to the TreeMap.
//                } else {
//                    cellsList = new ArrayList<String>();
//                    cellsList.add(cell);
//                    reverseLookup.put(tripCount, cellsList);
//                }
//            }

        }

        //By this point we expect to have a TreeMap which has keys corresponding to the number of
        //trips and values having lists of start:end cells which had that many number of trips.

        //E.g.,

        //26-->[140.158,145.165,144.164]
        //18-->[146.164]
        //8-->[144.162,147.168,144.165,146.168]

        Set<Map.Entry<Integer, ArrayList<String>>> entrySet = ((TreeMap)reverseLookup).entrySet();
        Iterator<Map.Entry<Integer, ArrayList<String>>> itr = entrySet.iterator();

        Map.Entry<Double, ArrayList<String>> currentEntry = null;
        int cntr = 0;
        LinkedList<String> result = new LinkedList<String>();

        while(itr.hasNext()){
        	ArrayList<String> currentCells = itr.next().getValue();

//            int currentCellSize = currentCells.size();
//            if(currentCellSize > 0){
//                Iterator<String> itr2 = currentCells.iterator();
//
//                while(itr2.hasNext()){
//                    result.add(itr2.next());
//                    cntr++;
//
//                    if(cntr > k){ //We need to select only the top k most frequent cells only
//                        break;
//                    }
//                }
//
//            }
        	
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

            if(cntr > k){
                break;
            }
        }

        return result;

    }
}