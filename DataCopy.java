package com.ELTTool;




import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.json.JSONArray;
import org.json.JSONObject;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;


/**
*This will move copies of entire tables from one place to another via an S3 bucket
*<br>
*<br>
*Job File Requirements
* <br>
* <ul>
    * <li>Order - Required:Integer - The order this component should run in: 1 (1st), 2 (2nd) ... </li>
    * <li>Component - Required:String - <i>APICall</i></li>
    * <li>Prefix - Required:Integer - The number in the Prefix array needed: 1, 2, etc...</li>
    * <li>RunStyle - Required:String - <i>batch</i></li>
    * <li>Parameters - Required:JSON Array - list of parms described below
    * <ul>
        * <li>sourceDBConnection - Required:String - the connection for the source</li>
        * <li>targetDBConnection - Required:String - the connection for the target</li>
        * <li>s3Ref - Required:String - the reference name defined in the connection file for the s3 connection</li>
        * <li>bucket - Required:String - the name of the s3 bucket</li>
        * <li>tableList - Required:JSON Array - the array of tables to move
        * <ul>
            * <li>sourceSchema - Required:String - the source schema name</li>
            * <li>sourceTable - Required:String - the source table name to copy the data from</li>
            * <li>targetSchema - Required:String - the schema in the target connection to create the table in</li>
        * </ul>
        * </li>
    * </ul>
    * </li>
* </ul>
*/
public class DataCopy extends Component {




    private static Logger logger = Logger.getLogger(DataCopy.class.getName());



    private ConnectionManager cm;
    private Properties fileProps;

    private S3 s3;

    private String s3Ref;
    private String bucket;
    private JSONArray tableList;
    private int threadCount;

    private DBConnection sourceDBConn;
    private String sourceDBRef;
    private Connection.DBTypes sourceDBType;
    private int sourceStatementTimeout;

    private DBConnection targetDBConn;
    private String targetDBRef;
    private Connection.DBTypes targetDBType;
    private int targetStatementTimeout;

    private String stmtFile = "DataCopy.txt";
    private String[] stmts;

    private ArrayBlockingQueue<JSONObject> queue;
    private AtomicInteger recCount;

    private ProgressBar pb;








    public DataCopy(ConnectionManager cm, Properties fileProps) {
        this.cm = cm;
        this.fileProps = fileProps;
    }








    @Override
    public void runComponent() {



        try {




            sourceDBConn = cm.getDBConnection(sourceDBRef, sourceDBType, jobName, this.getClass().getSimpleName(), jobId, groupId);
            sourceDBConn.startConnection(sourceStatementTimeout, jobId, groupId);
            String db = ((Snowflake) sourceDBConn).database;

            targetDBConn = cm.getDBConnection(targetDBRef, targetDBType, jobName, this.getClass().getSimpleName(), jobId, groupId);
            targetDBConn.startConnection(targetStatementTimeout, jobId, groupId);

            s3 = cm.getS3Connection(s3Ref, jobName);
            s3.startConnection(0, jobId, groupId);

            stmts = LoadSQLFile.loadSQLFile(fileProps, stmtFile, false, jobName, jobId, groupId);







            //------------------------------------------------------------------------------
            //------------------------------------------------------------------------------
            //------------------------------------------------------------------------------
            // get all the DDLs from the source and create them on the target
            // make sure any dependencies are handled in the order in the array


            ProgressBar pbd = new ProgressBar(0, tableList.length(), "Getting DDLs", true);

            int ddlCounter = 0;
            for (Object o : tableList) {

                JSONObject jo = (JSONObject) o;
                String schema = jo.getString("sourceSchema");
                String tableName = jo.getString("sourceTable");
                String targetSchema = jo.getString("targetSchema");

                ddlCounter++;
                pbd.updateProgressBar(ddlCounter, "Current: " + tableName);

                ResultSet rs = sourceDBConn.executeQuery( Replacement.schemaAndTableName(schema, tableName, stmts[0]), false );
                rs.next();
                String rawDDL = rs.getString(1).toLowerCase();  // put in lower case

                String fullyQualified = (db + "." + schema + ".").toLowerCase();

                // fact tables will carry fully qualified foreign key references, so strip those out
                String stripDDL = rawDDL.replace(fullyQualified, "");


                //-------------------------------------------------------------
                // execute the stripped ddl on the target database and schema

                String dropStmt = Replacement.schemaAndTableName(targetSchema, tableName, stmts[2]);
                String useStmt = Replacement.schema(targetSchema, stmts[1]);

                targetDBConn.executeStatement(useStmt, false);
                targetDBConn.executeStatement(dropStmt, false);
                targetDBConn.executeStatement(stripDDL, false);



            }

            support.firePropertyChange(getPropertyChangeMetricId(Metrics.ddlCount.toString()), -1, ddlCounter);

            pbd.closeProgressBar();


            sourceDBConn.shutdownConnection();
            targetDBConn.shutdownConnection();



            //------------------------------------------------------------------------------
            //------------------------------------------------------------------------------
            //------------------------------------------------------------------------------
            // now pull all the data over through the s3 bucket
            // run the number of threads requested



            // put all the objects in a blocking queue
            queue = new ArrayBlockingQueue(tableList.length());

            for(Object o : tableList) {
                JSONObject jo = (JSONObject) o;

                //System.out.println(curObj.toString());

                while ( !queue.offer(jo) ) {
                    // loop while waiting for the string to get inserted into the queue
                }

            }



            recCount = new AtomicInteger(0);
            pb = new ProgressBar(0, tableList.length(), "Moving Table Data", true);


            // start running the threads ...
            ArrayList<Future<?>> futures = new ArrayList<>();
            ExecutorService es = Executors.newFixedThreadPool(threadCount);

            for (int i = 0; i < threadCount; i++)
                futures.add( es.submit(new getData()) );



            // wait for all threads to be done
            boolean allThreadsDone = false;
            while (!allThreadsDone) {

                // wait one second
                Thread.sleep(1000);

                int doneCount = 0;
                for(Future<?> f : futures)
                    if (f.isDone())
                        doneCount++;

                if (doneCount == threadCount)
                    allThreadsDone = true;


            }


            pb.closeProgressBar();


            completeComponent();





        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, jobName, jobId, groupId));
            killComponent();
        }





    }







    public void removeFiles(String tableName) {

        //----------------------------------------------------------------
        // delete any files already in the s3 bucket for the table

        ListObjectsV2Result lor = s3.getS3().listObjectsV2(bucket, tableName);
        int returnCount = lor.getObjectSummaries().size();

        logger.info("Files to be deleted in s3 for table: " + tableName + " | fileCount: " + returnCount );

        if (returnCount > 0)
            for (S3ObjectSummary s : lor.getObjectSummaries())
                s3.getS3().deleteObject(bucket, s.getKey());


    }






    @Override
    public boolean loadComponent(JSONObject runParms, String context) {



        try{

            this.context = context;
            JSONArray pa = runParms.getJSONArray("Parameters");


            for (int i = 0; i < pa.length(); i++) {

                JSONObject curParm = pa.getJSONObject(i);
                String parmName = (String) curParm.names().get(0);


                switch (parmName) {

                    case "sourceDBConnection":
                        sourceDBType = Connection.DBTypes.valueOf( (String) getJSONValue(curParm, DBConnection.dbConnTypeKW, Component.stringType, parmName));
                        sourceStatementTimeout = (Integer) getJSONValue(curParm, DBConnection.dbConnStmtTimeoutKW, Component.intType, parmName);
                        sourceDBRef = (String) getJSONValue(curParm, context, Component.stringType, parmName);
                        break;
                    case "targetDBConnection":
                        targetDBType = Connection.DBTypes.valueOf( (String) getJSONValue(curParm, DBConnection.dbConnTypeKW, Component.stringType, parmName));
                        targetStatementTimeout = (Integer) getJSONValue(curParm, DBConnection.dbConnStmtTimeoutKW, Component.intType, parmName);
                        targetDBRef = (String) getJSONValue(curParm, context, Component.stringType, parmName);
                        break;
                    case "s3Ref":
                        s3Ref = (String) getJSONValue(curParm, context, Component.stringType, parmName);
                        break;
                    case "bucket":
                        bucket = (String) getJSONValue(curParm, context, Component.stringType, parmName);
                        break;
                    case "tableList":
                        tableList = curParm.getJSONArray(parmName);
                        break;
                    case "threadCount":
                        threadCount = (Integer) getJSONValue(curParm, context, Component.intType, parmName);
                        break;



                }

            }





            return false;


        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, jobName, jobId, groupId));
            return false;
        }



    }








    class getData implements Runnable {


        @Override
        public void run() {


            try {


                //------------------------------------------
                //start up a new connection for each thread so queryIds and session Ids don't interfere with each other

                DBConnection sourceThDBConn = cm.getDBConnection(sourceDBRef, sourceDBType, jobName, this.getClass().getSimpleName(), jobId, groupId);
                sourceThDBConn.startConnection(sourceStatementTimeout, jobId, groupId);

                DBConnection targetThDBConn = cm.getDBConnection(targetDBRef, targetDBType, jobName, this.getClass().getSimpleName(), jobId, groupId);
                targetThDBConn.startConnection(targetStatementTimeout, jobId, groupId);


                Object curObj = null;

                while ( (curObj = queue.poll()) != null ) {

                    JSONObject jo = (JSONObject) curObj;
                    String sourceSchema = jo.getString("sourceSchema");
                    String tableName = jo.getString("sourceTable");
                    String targetSchema = jo.getString("targetSchema");


                    recCount.getAndIncrement();
                    pb.updateProgressBar(recCount.get(), "Current: " + tableName + " (Removing files)");

                    removeFiles(tableName);


                    //----------------------------------------------------------------
                    // pull the data from the source


                    String copyToS3 = Replacement.awsIdAndKey(s3.getAccessKeyId(), s3.getSecretKey(), Replacement.schemaAndTableName(sourceSchema, tableName, stmts[3]));
                    String copyFromS3 = Replacement.awsIdAndKey(s3.getAccessKeyId(), s3.getSecretKey(), Replacement.schemaAndTableName(targetSchema, tableName, stmts[4]));

                    pb.updateProgressBar(recCount.get(), "Current: " + tableName + " (Source --> S3)");

                    sourceThDBConn.executeStatement(copyToS3, false);

                    pb.updateProgressBar(recCount.get(), "Current: " + tableName + " (S3 --> Target)");

                    targetThDBConn.executeStatement(copyFromS3, false);

                    String queryId = ((Snowflake) targetThDBConn).getQueryId();
                    String sessionId = ((Snowflake) targetThDBConn).getSessionId();

                    String copyRecCountStmt = Replacement.queryAndSession(queryId, sessionId, stmts[5]);
                    ResultSet rs = targetThDBConn.executeQuery(copyRecCountStmt, false);
                    rs.next();
                    int copyRecCount = rs.getInt(1);

                    logger.info("TableName: " + tableName + " | RecordCount: " + copyRecCount);
                    support.firePropertyChange(getPropertyChangeMetricId(Metrics.recordCount.toString()), -1, copyRecCount);

                    pb.updateProgressBar(recCount.get(), "Current: " + tableName + " (Removing files)");
                    removeFiles(tableName);


                }




            } catch (Exception e) {
                logger.severe(LoggingUtils.getErrorEntry(e, jobName, jobId, groupId));
                killComponent();
            }


        }






    }






    @Override
    public void closeComponent() {

        try {

            // shutdown the s3 client
            s3.shutdownConnection();

        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, jobName, jobId, groupId));
            killComponent();
        }

    }







    @Override
    public ArrayList<String> reportDetails() {


        ArrayList<String> a = allCompReportDetails();

        addDetailString("sourceDBRef", sourceDBRef, a);
        addDetailString("targetDBRef", targetDBRef, a);
        addDetailString("s3Ref", s3Ref, a);
        addDetailString("bucket", bucket, a);
        addDetailString("threadCount", threadCount, a);


        int tableNum = 0;
        for (Object o : tableList) {

            tableNum++;
            JSONObject jo = (JSONObject) o;
            String sourceSchema = jo.getString("sourceSchema").trim();
            String tableName =  jo.getString("sourceTable").trim();
            String targetSchema = jo.getString("targetSchema").trim();

            String s = tableNum + ". " + String.format("%-30s", tableName) + "sourceSchema: " + String.format("%-30s", sourceSchema) + "targetSchema: " + targetSchema;

            a.add(s);

        }


        return a;


    }







}
