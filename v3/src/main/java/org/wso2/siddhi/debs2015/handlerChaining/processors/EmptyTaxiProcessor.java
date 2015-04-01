package org.wso2.siddhi.debs2015.handlerChaining.processors;

import org.wso2.siddhi.debs2015.handlerChaining.DebsEvent;

import java.util.*;

public class EmptyTaxiProcessor {

    private Map<Integer, Object[]> medallionMap = new HashMap<Integer, Object[]>();
    private Map<Integer, CountProfitCustomObj> cellDataMap = new HashMap<Integer, CountProfitCustomObj>();       //key:Cell, values: (profit , count )
    private LinkedList<Object[]> taxiInfoWindow = new LinkedList<Object[]>();







    public void process(DebsEvent debsEvent) {
         Set<Integer> changedCell = new LinkedHashSet<Integer>();//key:Cell, values: (profit , count )

         int endCell = debsEvent.getEndCellNo();
         int medallion = debsEvent.getMedallion();
         long  dropoffTime = debsEvent.getDropoff_datetime();
         int startCell  = debsEvent.getStartCellNo();
         float profit = debsEvent.getProfit();

          CountProfitCustomObj previousData=  cellDataMap.get(endCell);
          if(previousData != null){
              previousData.count++;
          }  else {
              cellDataMap.put(endCell, new CountProfitCustomObj(0,1));
          }
         changedCell.add(endCell);


        //Adding new Profit
        CountProfitCustomObj previousDataStartValue=  cellDataMap.get(startCell);
        if(previousDataStartValue != null){
            previousDataStartValue.profit = profit;
            changedCell.add(startCell);
        }  else {
            cellDataMap.put(startCell,  new CountProfitCustomObj(profit,0));
        }




        //remove previous trip of the same taxi
        Object[] previousDropoff = medallionMap.put(medallion, new Object[]{endCell,medallion,dropoffTime});   // previousDropOff has endCellNo, medallion, dropoff_datetime
        if(previousDropoff != null){
            int cell = (Integer)previousDropoff[0];
            cellDataMap.get(cell).count--;
                changedCell.add(cell);

        }

        while (true){
            Object[] trip = taxiInfoWindow.peekFirst();        //endCellNo, medallion, dropoff_datetime, startCellNo , profit, pickup_datetime, iij_timestamp
            if(trip != null && dropoffTime -(Long)trip[2] >= 1800000){
                taxiInfoWindow.removeFirst();
                Integer medallionKey =  (Integer)trip[1];
                Object[] medallionMapEntry = medallionMap.get(medallionKey);

                if(medallionMapEntry != null && medallionMapEntry[2] ==trip[2] ){
                    int cell =   (Integer)trip[0];
                    cellDataMap.get(cell).count--;
                    changedCell.add(cell);
                    medallionMap.remove(medallionKey);

                }

            }  else {
                break;
            }

        }
        taxiInfoWindow.add(new Object[]{endCell,medallion,dropoffTime});

        ////cellNo , profit , emptyTaxiCount ,  pickup_datetime , dropoff_datetime , iij_timestamp,
        List<ProfitObj> profitObjs = new ArrayList<ProfitObj>();

        for(Integer cell: changedCell){
            int count = cellDataMap.get(cell).count;
            if(count >.0) {
                float profitOfCell = cellDataMap.get(cell).profit;
                 profitObjs.add(new ProfitObj(cell, profitOfCell/count, profitOfCell,count));
            } else {
                cellDataMap.remove(cell);
            }
        }

        debsEvent.setProfitObjList(profitObjs);

    }


    private  class CountProfitCustomObj{
        private  float profit;
        private int count;

        public CountProfitCustomObj(float profit, int count){
          this.profit = profit;
          this.count = count;
        }

        public float getProfit() {
            return profit;
        }

        public int getCount() {
            return count;
        }
    }
}
