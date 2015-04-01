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
            cellIdFirstComponent= gridResolution;
        } else {
            cellIdFirstComponent = (short) ((((eastMostLongitude - inputLongitude) / longitudeDifference) * gridResolution) + 1);
        }

        //--------------------------------------------------          The following is for latitude -------------------------------------

        short cellIdSecondComponent;

        if(southMostLatitude==inputLatitude){
            cellIdSecondComponent= gridResolution;
        } else {
            cellIdSecondComponent = (short) ((((northMostLatitude - inputLatitude) / latitudeDifference) * gridResolution) + 1);
        }

        return cellIdFirstComponent*1000+cellIdSecondComponent;
    }



}
