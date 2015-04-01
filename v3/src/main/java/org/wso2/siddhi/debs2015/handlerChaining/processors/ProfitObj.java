package org.wso2.siddhi.debs2015.handlerChaining.processors;


public class ProfitObj {
    int cellID;
    float profit_per_taxi;
    float profit;
    int emptyTaxiCount;


    public ProfitObj(int cellID, float profit_per_taxi, float profit, int emptyTaxiCount){
        this.cellID = cellID;
        this.profit_per_taxi = profit_per_taxi;
        this.profit = profit;
        this.emptyTaxiCount = emptyTaxiCount;
    }

    public int getCellID() {
        return cellID;
    }

    public float getProfit_per_taxi() {
        return profit_per_taxi;
    }

    public float getProfit() {
        return profit;
    }

    public int getEmptyTaxiCount() {
        return emptyTaxiCount;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof ProfitObj && cellID == ((ProfitObj) obj).getCellID();
    }





}
