package com.ELTTool;


import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 *This component will take a list from a list APICall or a list SQLPull and extract the iterative list for a list-db API call
 *<br>
 *<br>
 *Job File Requirements
 * <br>
 * <ul>
 * <li>Order - Required - The order this component should run in: 1 (1st), 2 (2nd) ... </li>
 * <li>Component - Required - <i>ListAPIPrep</i></li>
 * <li>Prefix - Required - The number in the Prefix array needed: 1, 2, etc...</li>
 * <li>RunStyle - Required - <i>batch</i>
 * <li>Parameters - Required - one JSON object for each iterative element desired
     * <ul>
     * <li>old - the name that is needed off the original list</li>
     * <li>new - the name for the new value, the API parameter name</li>
     * <li>type - API parameter type: <i>header</i>, <i>query</i>, or <i>path</i></li>
     * </ul>
 * </li>
 * </ul>
 */
public class ListAPIPrep extends Component {


    // this component will go through an JSON Array and strip out the requested parts, rename them, and output a new object to the job
    // this will be used for iteration on an API call so a previous API call that was made
    // can be stripped out to teh next API call can iterate on a distinct list.  Get rid of the junk.



    private static Logger logger = Logger.getLogger(ListAPIPrep.class.getName());

    private JSONArray iterateList;
    private JSONArray outIterateList;
    private JSONArray stripList;

    private static final String oldKW = "old";
    private static final String newKW = "new";









    public void setIterateList(JSONArray iterateList) {
        this.iterateList = new JSONArray();
        this.iterateList = iterateList;
    }

    public JSONArray getIterateList() { return outIterateList; }










    @Override
    public void runComponent() {


        try {


            outIterateList = new JSONArray();

            //---------------------------------------------
            // go through each object in the array

            logger.info(LoggingUtils.getLogEntry("Inbound list size: " + iterateList.length(), jobName, jobId, groupId));

            for (Object o : iterateList) {

                JSONObject curJo = (JSONObject) o;
                JSONArray curJA = new JSONArray();

                // strip and rename the requested values into the new object
                for (Object rl : stripList) {

                    JSONObject newJO = new JSONObject();

                    newJO.put(APICall.nameAPIKW, ((JSONObject) rl).getString(newKW));
                    newJO.put(APICall.typeAPIKW, ((JSONObject) rl).getString("type"));
                    newJO.put(APICall.valueAPIKW, curJo.getString(((JSONObject) rl).getString(oldKW)));

                    curJA.put(newJO);

                }

                // add it to the output JSON Array
                outIterateList.put(curJA);

            }


            support.firePropertyChange(getPropertyChangeMetricId(Metrics.listSize.toString()), -1, outIterateList.length());
            logger.info(LoggingUtils.getLogEntry("Outbound list size: " + outIterateList.length(), jobName, jobId, groupId));
            //System.out.println(outIterateList);

            completeComponent();

        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, jobName, jobId, groupId));
            killComponent();
        }






    }




    @Override
    public boolean loadComponent(JSONObject runParms, String context) {

        try {

            this.context = context;
            stripList = runParms.getJSONArray("Parameters");

            return false;

        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, jobName, jobId, groupId));
            killComponent();
            return false;
        }


    }





    @Override
    public void closeComponent() {

        // null out any list to reduce heap size, I think it should do this automatically anyway
        iterateList = null;

    }





    @Override
    public ArrayList<String> reportDetails() {

        ArrayList<String> a = allCompReportDetails();

        addDetailString("stripCount", stripList.length(), a);

        return a;


    }





}
