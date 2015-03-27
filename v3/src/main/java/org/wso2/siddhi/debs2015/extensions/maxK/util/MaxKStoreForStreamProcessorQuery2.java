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

public class MaxKStoreForStreamProcessorQuery2 {
    private Map<String, Double> routeFrequencies = new HashMap<String, Double>(); //a reverse index of which the key is the cell ID and the value is the profitability.
    private Map<Double, ArrayList<CustomObj>> reverseLookup = new TreeMap<Double, ArrayList<CustomObj>>(
            new Comparator<Double>() {
                public int compare(Double o1, Double o2) {
                    return o2.compareTo(o1);
                }
            }
    );    //The reverseLookup TreeMap holds the list of cells for each count.
    private double lastReturnedLeastProfitability = 0;
//    private Map<Integer, TreeSet<CustomObjQuery1>> reverseLookup = new TreeMap<Integer, TreeSet<CustomObjQuery1>>(new Comparator<Integer>() {
//
//        public int compare(Integer o1, Integer o2) {
//            return o2.compareTo(o1);
//        }
//    });
//    private long count;


    /**
     * Calculated the current top k values by comparing the values that are
     * already stored in the Map.
     *
     * @return A Map that contains the Max-K values
     * @params value - The pressure reading value for the current event
     * @params date - The timestamp the pressure reading was produced.
     */
    public LinkedList<CustomObj> getMaxK(CustomObj customObj, int k) {
        String cell = customObj.getCellID();
        Double currentProfit = (Double) customObj.getProfit_per_taxi();

        Double previousProfit = routeFrequencies.get(customObj.getCellID());

        if (previousProfit == null) {
            previousProfit = 0d;
        } else {
            reverseLookup.get(previousProfit).remove(cell);
        }


        if (currentProfit == 0) {
            routeFrequencies.remove(cell);
        } else {
            //This code basically updates the count per cell. If there is a new value for the count which is non-zero, the old value is replaced with the new value.

            routeFrequencies.put(cell, currentProfit);

            ArrayList<CustomObj> cellsList = reverseLookup.get(currentProfit);

            if (cellsList != null) {
                if (cellsList.size() == 10)
                    cellsList.remove(0);
                cellsList.add(customObj);
            } else {
                cellsList = new ArrayList<CustomObj>();
                cellsList.add(customObj);
                reverseLookup.put(currentProfit, cellsList);
            }


        }


        Set<Map.Entry<Double, ArrayList<CustomObj>>> entrySet = ((TreeMap)reverseLookup).entrySet();
        Iterator<Map.Entry<Double, ArrayList<CustomObj>>> itr = entrySet.iterator();

        int cntr = 0;
        LinkedList<CustomObj> result = new LinkedList<CustomObj>();


        while(itr.hasNext()){
            Map.Entry<Double, ArrayList<CustomObj>> item = itr.next();
            lastReturnedLeastProfitability = item.getKey();
            ArrayList<CustomObj> currentCells = item.getValue();
            int currentCellSize = currentCells.size();

            if(currentCells.size() > 0){

                for (int i = currentCellSize - 1; i >= 0; i--) {
                    result.add(currentCells.get(i));
                    cntr++;

                    if(cntr >= k){ //We need to select only the top k most frequent cells only
                        break;
                    }
                }
            }

            if(cntr >= k){
                break;
            }
        }

        return result;

    }



//    public LinkedList<String> getMaxK(String cell, boolean isCurrent, int k) {
//        CustomObjQuery1 customObjQ1 = new CustomObjQuery1(cell, count++);
//        Integer previousCount = routeFrequencies.get(cell);
//
//        if (previousCount == null) {
//            previousCount = 0;
//        } else {
//            reverseLookup.get(previousCount).remove(customObjQ1);
//        }
//        int newTripCount = previousCount;
//
//        if (isCurrent) {
//            newTripCount++;
//        } else {
//            newTripCount--;
//        }
//
//        if (newTripCount == 0) {
//            routeFrequencies.remove(cell);
//        } else {
//            //This code basically updates the count per cell. If there is a new value for the count which is non-zero, the old value is replaced with the new value.
//
//            routeFrequencies.put(cell, newTripCount);
//
//            TreeSet<CustomObjQuery1> cellsList = reverseLookup.get(newTripCount);
//
//            if (cellsList != null) {
//                if (cellsList.size() == 10)
//                    cellsList.remove(cellsList.last());
//                cellsList.add(customObjQ1);
//            } else {
//                cellsList = new TreeSet<CustomObjQuery1>(new Comparator<CustomObjQuery1>() {
//
//                    public int compare(CustomObjQuery1 o1, CustomObjQuery1 o2) {
//                        return (o2.getCount()).compareTo(o1.getCount());
//                    }
//                });
//                cellsList.add(customObjQ1);
//                reverseLookup.put(newTripCount, cellsList);
//            }
//
//
//        }
//
//        //By this point we expect to have a TreeMap which has keys corresponding to the number of
//        //trips and values having lists of start:end cells which had that many number of trips.
//
//        //E.g.,
//
//        //26-->[140.158,145.165,144.164]
//        //18-->[146.164]
//        //8-->[144.162,147.168,144.165,146.168]
//
////
//        Set<Map.Entry<Integer, TreeSet<CustomObjQuery1>>> entrySet = ((TreeMap)reverseLookup).entrySet();
//        Iterator<Map.Entry<Integer, TreeSet<CustomObjQuery1>>> itr = entrySet.iterator();
//
//
//        int cntr = 0;
//        LinkedList<String> result = new LinkedList<String>();
//
//        while (itr.hasNext()) {
//            TreeSet<CustomObjQuery1> currentCells = itr.next().getValue();
//
//            int currentCellSize = currentCells.size();
//            if (currentCellSize > 0) {
//                Iterator<CustomObjQuery1> itr2 = currentCells.iterator();
//
//                while (itr2.hasNext()) {
//                    result.add(itr2.next().getCellID());
//                    cntr++;
//
//                    if (cntr > k) { //We need to select only the top k most frequent cells only
//                        break;
//                    }
//                }
//
//            }
//
//            if (cntr > k) {
//                break;
//            }
//        }
//
//        // Returns the pressure readings that are sorted in descending order according to the key (pressure value).
//        return result;
//    }
}