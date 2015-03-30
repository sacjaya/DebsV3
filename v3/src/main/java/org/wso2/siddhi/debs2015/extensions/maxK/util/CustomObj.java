package org.wso2.siddhi.debs2015.extensions.maxK.util;

/**
 * Created by sachini on 3/17/15.
 */
public class CustomObj{
    int cellID;
    float profit_per_taxi;
    Object profit;
    Object emptyTaxiCount;


    public CustomObj(int cellID,float profit_per_taxi,Object profit,Object emptyTaxiCount){
        this.cellID = cellID;
        this.profit_per_taxi = profit_per_taxi;
        this.profit = profit;
        this.emptyTaxiCount = emptyTaxiCount;
    }

    public int getCellID() {
        return cellID;
    }

    public Object getProfit_per_taxi() {
        return profit_per_taxi;
    }

    public Object getProfit() {
        return profit;
    }

    public Object getEmptyTaxiCount() {
        return emptyTaxiCount;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof CustomObj && cellID == ((CustomObj) obj).getCellID();
    }



//    public static void main(String[] args) {
//        List<CustomObj> cc = new ArrayList<CustomObj>();
//        cc.add(new CustomObj("asa", 1l,null, null));
//
//        cc.add(new CustomObj("ccccc", 1l,null, null));
//
//        cc.remove(new CustomObj("asa", 67l,null, null));
//
//        cc.remove("ccccc");
//    }

}