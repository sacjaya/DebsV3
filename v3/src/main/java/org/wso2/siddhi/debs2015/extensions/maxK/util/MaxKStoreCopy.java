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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

public class MaxKStoreCopy {
    private Map<String, Integer> units = new ConcurrentHashMap<String, Integer>(); //The units Map keeps the trip count for each and every cell. The index is based on the cell ID.
    private Map<Integer, LinkedList<String>> maxValues  = new TreeMap<Integer, LinkedList<String>>(new Comparator<Integer>() {

        public int compare(Integer o1, Integer o2) {
           return o2.compareTo(o1);
        }
    }); //Here we use a custom comparator so that we can order the elements in the descending order.
    
    int eventCounter = 0;
    
    //The maxValues Map keeps records for each trip count. The index is based on the trip count.
//    Comparator<Integer> comparator = new Comparator<Integer>() {
//
//        public int compare(Integer o1, Integer o2) {
//           return o1.compareTo(o2);
//        }
//    };
    

    /**
     * Calculated the current top k values by comparing the values that are
     * already stored in the Map.
     *
     * @return A Map that contains the Max-K values
     * @params value - The pressure reading value for the current event
     * @params date - The timestamp the pressure reading was produced.
     *
     */
    public synchronized LinkedList<String> getMaxK(String cell, int tripCount, int k) {    	
        if(tripCount==0){
        	//We have to make sure that the units contains the cell id we are looking for. Otherwise we may get a NullPointerException.
        	Integer previousCount= units.remove(cell);
        	
        	if (previousCount!=null) {
        		//The first item we entered the cell list should be removed first. 
        		//Because it will get expired first. So when ever we get an event expiration, we
        		//remove the first item in the list corresponding to that count.
        		maxValues.get(previousCount).removeFirst();
        	}
        } else {
        	//Here we have a non-zero trip count.
        	
        	//This code basically updates the count per cell. If there is a new value for the count
        	//which is non-zero, the old value is replaced with the new value.
        	
        	//The maxValues TreeMap holds the list of cells for each count.
        	//"units" is an index of which the key is the cell ID and the value is the count.
        	
        	Integer kkey = units.get(cell);
        	
            if ((kkey!=null)&&(kkey != -1)) {
            	//We already had a trip count for this particular route previously.
                Integer previousCount = units.get(cell);
                
                if(previousCount != tripCount){
                	units.put(cell, tripCount);
                	//Although we can remove the first element from the list when an event gets 
                	//expired, we cannot do that here, because we are just updating an existing
                	//trip count.
                	maxValues.get(previousCount).remove(cell);
                }
                
                LinkedList<String> cellsList = maxValues.get(tripCount);
                
                if (cellsList != null) {
                	//We do not have to check whether we have a duplicate here. Because by default we remove
                	//elements from the TreeMap before we add a new record.
                    cellsList.add(cell);
                } else {
                    cellsList = new LinkedList<String>();
                    cellsList.add(cell);
                    maxValues.put(tripCount, cellsList);
                }
            } else if((kkey==null)||(kkey == -1)) {
            	//We do not have a trip count for this particular route. We have to add everything
            	//from the beginning. First, we add the route to units Map. Then we add a record
            	//in the maxValues TreeMap.
                units.put(cell, tripCount);
                LinkedList<String> cellsList = maxValues.get(tripCount);
                
                if (cellsList != null) {
                	//Again we need not to worry whether we already had stored the route in the LinkedList.
                	cellsList.add(cell);
                } else {
                    cellsList = new LinkedList<String>();
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
        
        Set<Integer> keySet = ((TreeMap)maxValues).keySet();//The keyset is the number of unique appearances
        Iterator<Integer> itr = keySet.iterator();
        
        
               
        int currentKey = 0;
        int cntr = 0;
        LinkedList<String> result = new LinkedList<String>();
        
        eventCounter++;
        
        if(eventCounter % 10000 == 0){
        	Iterator<Integer> itr3 = keySet.iterator();
        
            while(itr3.hasNext()){
            	currentKey = itr3.next();
            	int routeCount = ((Map<Integer, LinkedList<String>>)maxValues).get(currentKey).size();
            	
            	if(routeCount != 0){
            		System.out.print(currentKey + "," + routeCount + ",");
            	}
            }
            
            System.out.println();
        }
        
        currentKey = 0;
        
        while(itr.hasNext()){
        	currentKey = itr.next();
        	
        	LinkedList<String> currentCells = maxValues.get(currentKey);
        	//System.out.println("currentKey:"+currentKey+"--->currentListSize:"+currentCells.size());
        	/*
        	if(currentCells.size() > 0){
        		Iterator<String> itr2 = currentCells.iterator();
        		        		
        		//We need to reverse the order of the list of items stored in the LinkedHashSet so 
        		//that the last inserted cell will be the first item in the new list.
        		Stack stk = new Stack();
        		
        		while(itr2.hasNext()){
        			stk.push(itr2.next());
        		}
        		
        		//Once we are done with reversing the order, we use the data from the Stack to fill 
        		//the result list until we have got top-K items
        		while(!stk.isEmpty()){
        			result.add((String) stk.pop());
        			cntr++;
        			
        			if(cntr > k){
        				break;
        			}
        		}
        	}
        	*/
        	
        	/*
        	if(currentCells.size() > 0){
        		LinkedList<String> list = new LinkedList<String>(currentCells);
        		
        		Iterator<String> itr2 = list.descendingIterator();
        		//Iterator<String> itr2 = currentCells.iterator();
        		
        		while(itr2.hasNext()){
        			result.add(itr2.next());
        			cntr++;
        			
        			if(cntr > k){ //We need to select only the top k most frequent cells only
        				break;
        			}
        		}
        	}*/
        	
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
        	
        	//Just to makesure we exit from iterating the TreeMap structure once we found the top-K number of cells.
			if(cntr > k){
				break;
			}
        }
        
        //System.out.println("+++++++++++++++++++++++++++++++++++++++++++++");
        
        // Returns the pressure readings that are sorted in descending order according to the key (pressure value).
        return result;

    }
}
