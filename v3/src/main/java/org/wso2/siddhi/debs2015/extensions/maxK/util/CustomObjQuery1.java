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

/**
 * Created by sachini on 3/17/15.
 */
public class CustomObjQuery1 {
    String cell;
    long count;


    public CustomObjQuery1(String cellID, long count){
        this.cell = cellID;
        this.count = count;
    }

    public String getCellID() {
        return cell;
    }

    public Long getCount() {
        return count;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof CustomObj) {
            return cell.equals(((CustomObj) obj).getCellID());
        }
        return  false;
    }
}