package org.wso2.siddhi.debs2015.handlerChaining;

import org.wso2.siddhi.debs2015.handlerChaining.processors.ProfitObj;

import java.util.List;


public class DebsEvent {
	//Note that we do not use the fields hack_license, trip_distance, trip_time_in_secs, which are 
	//available on the original data set
    int medallion;
    String pickup_datetime_org;
    String dropoff_datetime_org;
    float pickup_longitude;
    float pickup_latitude;
    float dropoff_longitude;
    float dropoff_latitude;
    float fare_plus_ip_amount;
    long iij_timestamp;

    /*****************************q2p1:query1********************************************/

    int startCellNo;//    debs:cellId(pickup_longitude,pickup_latitude) as startCellNo
    int endCellNo; //debs:cellId(dropoff_longitude,dropoff_latitude) as endCellNo
    long pickup_datetime; //debs:getTimestamp(pickup_datetime_org) as pickup_datetime
    long dropoff_datetime; //debs:getTimestamp(dropoff_datetime_org) as dropoff_datetime


    /*****************************q2p1:query2********************************************/
    boolean current = true;
    long timeStamp;
    List<DebsEvent> listAfterFirstWindow;
    float profit;


    /*****************************q1p1:query1********************************************/
    List<DebsEvent> listAfterSecondWindow;

    /*****************************q1p1:query2********************************************/
    Object[] topK;

    /*****************************q2p2:query1********************************************/
    List<ProfitObj> profitObjList;


    public DebsEvent(){

    }

    public DebsEvent(int medallion, String pickup_datetime_org, String dropoff_datetime_org, float pickup_longitude,
                     float pickup_latitude, float dropoff_longitude, float dropoff_latitude, float fare_plus_ip_amount, long iij_timestamp, int startCellNo, int endCellNo,
                     long pickup_datetime, long dropoff_datetime) {
        this.medallion = medallion;
        this.pickup_datetime_org = pickup_datetime_org;
        this.dropoff_datetime_org = dropoff_datetime_org;
        this.pickup_longitude = pickup_longitude;
        this.pickup_latitude = pickup_latitude;
        this.dropoff_longitude = dropoff_longitude;
        this.dropoff_latitude = dropoff_latitude;
        this.fare_plus_ip_amount = fare_plus_ip_amount;
        this.iij_timestamp = iij_timestamp;
        this.startCellNo = startCellNo;
        this.endCellNo = endCellNo;
        this.pickup_datetime = pickup_datetime;
        this.dropoff_datetime = dropoff_datetime;
    }

    public int getMedallion() {
        return medallion;
    }

    public void setMedallion(int medallion) {
        this.medallion = medallion;
    }

    public String getPickup_datetime_org() {
        return pickup_datetime_org;
    }

    public void setPickup_datetime_org(String pickup_datetime_org) {
        this.pickup_datetime_org = pickup_datetime_org;
    }

    public String getDropoff_datetime_org() {
        return dropoff_datetime_org;
    }

    public void setDropoff_datetime_org(String dropoff_datetime_org) {
        this.dropoff_datetime_org = dropoff_datetime_org;
    }

    public List<DebsEvent> getListAfterFirstWindow() {
        return listAfterFirstWindow;
    }

    public void setListAfterFirstWindow(List<DebsEvent> listAfterFirstWindow) {
        this.listAfterFirstWindow = listAfterFirstWindow;
    }

    public float getPickup_longitude() {
        return pickup_longitude;
    }

    public void setPickup_longitude(float pickup_longitude) {
        this.pickup_longitude = pickup_longitude;
    }

    public float getPickup_latitude() {
        return pickup_latitude;
    }

    public void setPickup_latitude(float pickup_latitude) {
        this.pickup_latitude = pickup_latitude;
    }

    public float getDropoff_longitude() {
        return dropoff_longitude;
    }

    public void setDropoff_longitude(float dropoff_longitude) {
        this.dropoff_longitude = dropoff_longitude;
    }

    public float getDropoff_latitude() {
        return dropoff_latitude;
    }

    public void setDropoff_latitude(float dropoff_latitude) {
        this.dropoff_latitude = dropoff_latitude;
    }

    public float getFare_plus_ip_amount() {
        return fare_plus_ip_amount;
    }

    public void setFare_plus_ip_amount(float fare_plus_ip_amount) {
        this.fare_plus_ip_amount = fare_plus_ip_amount;
    }

    public long getIij_timestamp() {
        return iij_timestamp;
    }

    public void setIij_timestamp(long iij_timestamp) {
        this.iij_timestamp = iij_timestamp;
    }

    public int getStartCellNo() {
        return startCellNo;
    }

    public void setStartCellNo(int startCellNo) {
        this.startCellNo = startCellNo;
    }

    public int getEndCellNo() {
        return endCellNo;
    }

    public void setEndCellNo(int endCellNo) {
        this.endCellNo = endCellNo;
    }

    public long getPickup_datetime() {
        return pickup_datetime;
    }

    public void setPickup_datetime(long pickup_datetime) {
        this.pickup_datetime = pickup_datetime;
    }

    public long getDropoff_datetime() {
        return dropoff_datetime;
    }

    public void setDropoff_datetime(long dropoff_datetime) {
        this.dropoff_datetime = dropoff_datetime;
    }


    public boolean isCurrent() {
        return current;
    }

    public void setCurrent(boolean current) {
        this.current = current;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public float getProfit() {
        return profit;
    }

    public void setProfit(float profit) {
        this.profit = profit;
    }

    public List<DebsEvent> getListAfterSecondWindow() {
        return listAfterSecondWindow;
    }

    public void setListAfterSecondWindow(List<DebsEvent> listAfterSecondWindow) {
        this.listAfterSecondWindow = listAfterSecondWindow;
    }

    public Object[] getTopK() {
        return topK;
    }

    public void setTopK(Object[] topK) {
        this.topK = topK;
    }


    public List<ProfitObj> getProfitObjList() {
        return profitObjList;
    }

    public void setProfitObjList(List<ProfitObj> profitObjList) {
        this.profitObjList = profitObjList;
    }

    public DebsEvent clone(){    	
    	return  new DebsEvent(medallion,pickup_datetime_org,dropoff_datetime_org,pickup_longitude,pickup_latitude,dropoff_longitude,dropoff_latitude,fare_plus_ip_amount,iij_timestamp,startCellNo, endCellNo,pickup_datetime, dropoff_datetime);
    	
    }
}
