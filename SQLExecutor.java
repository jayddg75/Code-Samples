package com.ELTTool;

import org.json.JSONArray;
import org.json.JSONObject;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.logging.Logger;





/**
 *This component will execute SQL against the target DBConnection and runs in both styles
 * <ul>
 * <li><i>batch</i> - execute statements one time</li>
 * <li><i>continuous</i> - execute statements on a fixed interval</li>
 * </ul>
 *<br>
 *<br>
 *Job File Requirements
 * <br>
 * <ul>
     * <li>Order - Required - The order this component should run in: 1 (1st), 2 (2nd) ... </li>
     * <li>Component - Required - <i>SQLExecutor</i></li>
     * <li>Prefix - Required - The number in the Prefix array needed: 1, 2, etc...</li>
     * <li>RunStyle - Required - <i>batch</i> or <i>continuous</i></li>
     * <li>sqlFile - Optional - pass this to load a sql file in the job folder. just the file name</li>
     * <li>Parameters - Required - list of parms
     * <ul>
         * <li>timeBetweenSecs - required for continuous - number of seconds between executions</li>
         * <li>dbConnection - Required - desired DBConnection</li>
         * <li>sql - Required - array of sql to run
            * <ul>
                * <li>type - both - <i>sp</i> for stored procedure or <i>sql</i> for a sql statement</li>
                * <li>call - sp - valid call statement with <i>?</i> for sp parms: call Some_Schema.Test_Load(?)</li>
                * <li>runCheck - sp - whether to check the landing table first for records before sql execution</li>
                * <li>spParmCount - sp - the number of sp parameters</li>
                * <li>spParms - sp
                * <ul>
                    * <li>type - <i>boolean</i>, <i>float</i>, or <i>varchar</i></li>
                    * <li>order - the order the parm is on the sp call itself</li>
                    * <li>usual context for value</li>
                * </ul>
                * </li>
                * <li>order - sql - order of sql execution for the array</li>
                * <li>fileOrder - sql - which order the desired statment shows up in the sql file passed above</li>
            * </ul>
         * </li>
     * </ul>
     * </li>
 * </ul>
 */
public class SQLExecutor extends Component {


    private static Logger logger = Logger.getLogger(SQLExecutor.class.getName());


    private ConnectionManager cm;
    private String backendSchema;
    private Properties fileProps;
    private String sqlFileName;

    private Connection.DBTypes dbConnType;
    private String dbRef;
    private DBConnection dbConn;
    private int statementTimeout;

    private int timeBetweenSecs;

    private JSONArray sqlArray;

    private Scheduler sch;

    private String checkStmtSql = "select count(*) from ${schema}.${table}";










    public SQLExecutor(ConnectionManager cm, String backendSchema, Properties fileProps) {
        this.cm = cm;
        this.backendSchema = backendSchema;
        this.fileProps = fileProps;
    }






    @Override
    public void runComponent() {

        try {



            // start the DB connection
            dbConn = cm.getDBConnection(dbRef, dbConnType, jobName, SQLExecutor.class.getName(), jobId, groupId);
            dbConn.startConnection(statementTimeout, jobId, groupId);


            if (runStyle.equals(JobManager.contRunStyle)) {

                sch = new Scheduler(new ExecuteAll(), timeBetweenSecs * 1000L, jobName, jobId, groupId);
                sch.runScheduler();

            } else {

                ExecuteAll ea = new ExecuteAll();
                ea.run();
                completeComponent();

            }




        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, jobName, jobId, groupId));
            killComponent();
        }

    }








    @Override
    public boolean loadComponent(JSONObject runParms, String context) {


        try {



            this.context = context;
            JSONArray pa = runParms.getJSONArray("Parameters");

            if( runParms.isNull("sqlFile"))
                sqlFileName = null;
            else
                sqlFileName = (String) getJSONValue(runParms, "sqlFile", Component.stringType);


            for (int i = 0; i < pa.length(); i++) {

                JSONObject curParm = pa.getJSONObject(i);
                String parmName = (String) curParm.names().get(0);

                switch (parmName) {

                    case "timeBetweenSecs":
                        timeBetweenSecs = (Integer) getJSONValue(curParm, context, Component.intType, parmName);
                        break;
                    case "dbConnection":
                        dbConnType = Connection.DBTypes.valueOf( (String) getJSONValue(curParm, DBConnection.dbConnTypeKW, Component.stringType, parmName) );
                        statementTimeout = (Integer) getJSONValue(curParm, DBConnection.dbConnStmtTimeoutKW, Component.intType, parmName);
                        dbRef = (String) getJSONValue(curParm, context, Component.stringType, parmName);
                        break;
                    case "sql":
                        sqlArray = curParm.getJSONArray(parmName);
                        break;

                } // end switch

            } // end looping on parameters



            return true;



        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, jobName, jobId, groupId));
            killComponent();
            return false;
        }









    }





    @Override
    public void closeComponent() {

        try {

            if (runStyle.equals(JobManager.contRunStyle))
                sch.stopScheduler(true);

            dbConn.shutdownConnection();


        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, jobName, jobId, groupId));
        }

    }






    /**
     * This inner class executes the sql statements
     */
    class ExecuteAll implements Runnable {


        @Override
        public void run() {

            logger.info(LoggingUtils.getLogEntry("Executing statements", jobName, jobId, groupId));

            try {

                String[] stmts = null;
                if (sqlFileName != null)
                    stmts = LoadSQLFile.loadSQLFile(fileProps, sqlFileName, true, jobName, jobId, groupId);



                for (Object o : sqlArray) {

                    JSONObject jo = (JSONObject) o;
                    String sqlType = (String) getJSONValue(jo, "type", Component.stringType);


                    switch (sqlType) {

                        case "sp":

                            String spCall = (String) getJSONValue(jo, "call", Component.stringType);
                            int spParmCount = (Integer) getJSONValue(jo, "spParmCount", Component.intType);
                            Boolean spRunCheck = (Boolean) getJSONValue(jo, "runCheck", Component.booleanType);

                            dbConn.setPreparedStatement(spCall);

                            logger.info(LoggingUtils.getLogEntry("SPCall: " + spCall, jobName, jobId, groupId));
                            //---------------------------------------------------------
                            // loop through all the parameters and set the SP parms

                            if (spParmCount > 0) {

                                JSONArray spParms =  jo.getJSONArray("spParms");
                                int parmCount = 0;


                                for (Object p : spParms) {

                                    parmCount++;
                                    JSONObject curSPParm = (JSONObject) p;

                                    String parmType = (String) getJSONValue(curSPParm, "type", Component.stringType);
                                    int parmOrder = (Integer) getJSONValue(curSPParm, "order", Component.intType);
                                    Object parmValue = curSPParm.get(context);

                                    logger.info(LoggingUtils.getLogEntry("SPParm: " + parmOrder + " | type: " + parmType + " | value: " + parmValue, jobName, jobId, groupId));

                                    dbConn.setPrepParm(parmType, parmOrder, parmValue);


                                }


                                if (parmCount != spParmCount)
                                    throw new Exception("The number of parameters passed in doesn't match the stated amount, double check your stuff!");

                            }



                            //---------------------------------------------------------------
                            // check the landing table if needed

                            boolean runFlag = true;

                            if(spRunCheck) {

                                String checkExecStmt = checkStmtSql.replace(Replacement.tableKW, prefix + "_Landing")
                                        .replace(Replacement.schemaKW, backendSchema);


                                support.firePropertyChange(getPropertyChangeMetricId(Metrics.spCheck.toString()), 0, 1);
                                ResultSet rs = dbConn.executeQuery(checkExecStmt, true);
                                rs.next();
                                int tableHotCount = rs.getInt(1);


                                // if there are no records on the check table, then don't run the SP
                                if (tableHotCount == 0) {
                                    runFlag = false;
                                }

                            }



                            //------------------------------------------------------------------
                            //------------------------------------------------------------------
                            // run the sp


                            if (runFlag) {

                                logger.info(LoggingUtils.getLogEntry("Executing SP", jobName, jobId, groupId));
                                support.firePropertyChange(getPropertyChangeMetricId(Metrics.spCall.toString()), 0, 1);
                                dbConn.executePrepStmt();

                            }



                            break;


                        case "sql":

                            int fileOrder = ((Integer) getJSONValue(jo, "fileOrder", "i")) - 1;
                            support.firePropertyChange(getPropertyChangeMetricId(Metrics.sqlStmt.toString()), 0, 1);
                            dbConn.executeStatement(Replacement.schemaAndPrefix(backendSchema, prefix, stmts[fileOrder]), false);

                            break;


                    } // end switch

                } // end looping through SQL array




            } catch(SQLException s) {


                // if the job is continuous, then just log the error.  For batch kill the job
                if (runStyle.equals(JobManager.contRunStyle)) {

                    logger.severe(LoggingUtils.getErrorEntry(s, jobName, jobId, groupId));
                    NotificationBox.displayNotification("There was a problem executing: " + s.getMessage());

                } else {

                    logger.severe(LoggingUtils.getErrorEntry(s, jobName, jobId, groupId));
                    killComponent();

                }


            } catch (Exception e) {

                // anything else, log it and kill the job
                logger.severe(LoggingUtils.getErrorEntry(e, jobName, jobId, groupId));
                killComponent();
            }


        }


    }







    @Override
    public ArrayList<String> reportDetails() {


        ArrayList<String> a = allCompReportDetails();

        addDetailString("sqlFileName", sqlFileName, a);
        addDetailString("dbRef", dbRef, a);
        addDetailString("statementTimeout", String.valueOf(statementTimeout), a);
        addDetailString("sqlArrayCount", String.valueOf(sqlArray.length()), a);


        return a;


    }





}
