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
    private Map<Integer, TreeSet<CustomObjQuery1>> maxValues  = new TreeMap<Integer, TreeSet<CustomObjQuery1>>(new Comparator<Integer>() {

        public int compare(Integer o1, Integer o2) {
            return o2.compareTo(o1);
        }
    });
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
            Integer previousCount= units.remove(cell);

            if (previousCount!=null) {
                maxValues.get(previousCount).remove(customObjQ1);
            }
        } else {
            //Here we have a non-zero trip count.

            //This code basically updates the count per cell. If there is a new value for the count
            //which is non-zero, the old value is replaced with the new value.

            //The maxValues TreeMap holds the list of cells for each count.
            //"units" is an index of which the key is the cell ID and the value is the count.


            Integer key = units.get(cell);


            if ((key!=null)) {
                Integer previousCount = units.get(cell);

                units.put(cell, tripCount);
                maxValues.get(previousCount).remove(customObjQ1);
                TreeSet<CustomObjQuery1> cellsList = maxValues.get(tripCount);

                if (cellsList != null) {
                    if(cellsList.size()==10)
                        cellsList.remove(cellsList.last());
                    cellsList.add(customObjQ1);
                } else {
                    cellsList = new TreeSet<CustomObjQuery1>(new Comparator<CustomObjQuery1>() {

                        public int compare(CustomObjQuery1 o1, CustomObjQuery1 o2) {
                            return (o2.getCount()).compareTo(o1.getCount());
                        }
                    });
                    cellsList.add(customObjQ1);
                    maxValues.put(tripCount, cellsList);
                }
            } else {
                units.put(cell, tripCount);
                TreeSet<CustomObjQuery1> cellsList = maxValues.get(tripCount);
                if (cellsList != null) {
                    if(cellsList.size()==10)
                        cellsList.remove(cellsList.last());
                    cellsList.add(customObjQ1);//Since we use HashSet we need not worry about duplicates here.
                    //Since we have a non-null reference here, we need not to put it back to the TreeMap.
                } else {
                    cellsList = new TreeSet<CustomObjQuery1>(new Comparator<CustomObjQuery1>() {

                        public int compare(CustomObjQuery1 o1, CustomObjQuery1 o2) {
                            return (o2.getCount()).compareTo(o1.getCount());
                        }
                    });
                    cellsList.add(customObjQ1);
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
        LinkedList<String> result = new LinkedList<String>();

        while(itr.hasNext()){
            currentKey = itr.next();
            TreeSet<CustomObjQuery1> currentCells = maxValues.get(currentKey);

            int currentCellSize = currentCells.size();
            if(currentCellSize > 0){
                Iterator<CustomObjQuery1> itr2 = currentCells.iterator();

                while(itr2.hasNext()){
                    result.add(itr2.next().getCellID());
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