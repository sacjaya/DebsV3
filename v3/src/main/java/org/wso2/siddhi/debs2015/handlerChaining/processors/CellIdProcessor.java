package org.wso2.siddhi.debs2015.handlerChaining.processors;

import org.apache.log4j.Logger;

public class CellIdProcessor  {
    private Logger log = Logger.getLogger(CellIdProcessor.class);
    private float  westMostLongitude = -74.916578f; //previous -74.916586f;
    private float  eastMostLongitude = -73.120778f; //previous -73.116f;
    private float  longitudeDifference =eastMostLongitude-westMostLongitude ;
    private float  northMostLatitude = 41.477182778f; //previous 41.477185f;
    private float  southMostLatitude = 40.129715978f; //previous 40.128f;
    private float  latitudeDifference = northMostLatitude-southMostLatitude ;
    private short  gridResolution = 600; //This is the number of cells per side in the square of the simulated area.



    public int execute(float inputLongitude, float inputLatitude) {
    	//--------------------------------------------------          The following is for longitude -------------------------------------
    	short cellIdFirstComponent;

        if(westMostLongitude==inputLongitude){
            cellIdFirstComponent= 1;
        } else {
            cellIdFirstComponent = (short) Math.round((((inputLongitude- westMostLongitude) / longitudeDifference) * gridResolution));
        }

        //--------------------------------------------------          The following is for latitude -------------------------------------

        short cellIdSecondComponent;
        
        if(northMostLatitude == inputLatitude) {
            cellIdSecondComponent = 1;
        } else {
            cellIdSecondComponent = (short) Math.round((((northMostLatitude - inputLatitude) / latitudeDifference) * gridResolution));
        }

        return  (cellIdFirstComponent*1000+cellIdSecondComponent);
    }



}
