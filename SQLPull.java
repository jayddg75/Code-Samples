package com.ELTTool;

import org.json.JSONArray;
import org.json.JSONObject;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;




/**
 *This component pull a query against a source database and convert it to a standard JSON record.  This runs in one of two modes ...
 * <ul>
 * <li><i>list</i> - pull a query off the database and pass off to the job for a subsequent component to pickup</li>
 * <li><i>db</i> - pull a query off the database, create a standard record, and put into files</li>
 * </ul>
 *<br>
 *<br>
 *Job File Requirements
 * <br>
 * <ul>
     * <li>Order - Required - The order this component should run in: 1 (1st), 2 (2nd) ... </li>
     * <li>Component - Required - <i>SQLPull</i></li>
     * <li>Prefix - Required - The number in the Prefix array needed: 1, 2, etc...</li>
     * <li>RunStyle - Required - <i>batch</i></li>
     * <li>mode - Required - <i>list</i> or <i>db</i></li>
     * <li>Parameters - Required - list of parms
     * <ul>
         * <li>dbConnection - Required -desired DBConnection</li>
         * <li>sqlFile - Required - name of the sql file in the job folder</li>
     * </ul>
     * </li>
 * </ul>
 */
public class SQLPull extends Component {



    private static Logger logger = Logger.getLogger(SQLPull.class.getName());


    private ConnectionManager cm;
    private Properties fileProps;

    private String selectStmt;
    private String sqlFile;
    private int fileOrder;

    private DBConnection sourceDBConn;
    private Connection.DBTypes sourceDBType;
    private String sourceDBRef;
    private int sourceStatementTimeout;

    private int fileRecLimit;
    private boolean rowDate;

    private BufferedWriter bw;

    private String mode;
    public static final String listModeKW = "list";
    public static final String dbModeKW = "db";

    private JSONArray iterateList;

    private ResultSet rs;
    private ResultSetMetaData rsm;

    private boolean loadFlag;







    public SQLPull(ConnectionManager cm, Properties fileProps) {
        this.cm = cm;
        this.fileProps = fileProps;
    }






    public JSONArray getIterateList() { return iterateList; }

    public String getMode() {
        return mode;
    }




    @Override
    public void runComponent() {


        try {

            loadFlag = true;
            bw = null;

            // get the data from the database

            sourceDBConn = cm.getDBConnection(sourceDBRef, sourceDBType, jobName, SQLPull.class.getName(), jobId, groupId);
            sourceDBConn.startConnection(sourceStatementTimeout, jobId, groupId);

            //System.out.println(selectStmt);
            rs = sourceDBConn.executeQuery(selectStmt, false);
            rsm = rs.getMetaData();



            if (mode.equals(dbModeKW))
                runFileLoad();
            else
                runListLoad();


            completeComponent();



        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, jobName, jobId, groupId));
            killComponent();
        }


    }






    private void runListLoad() {



        try {



            iterateList = new JSONArray();


            // check to see if there are any results

            boolean hasRecord;
            hasRecord = rs.isBeforeFirst();
            int recordCount = 0;

            // if there are pull the first row
            if (hasRecord)
                rs.next();



            while (hasRecord && loadFlag) {


                //logger.info(rs.getString(1));
                iterateList.put( new JSONArray(rs.getString(1)) );
                recordCount++;
                hasRecord = rs.next();

            }

            logger.info(LoggingUtils.getLogEntry("Records pulled: " + recordCount, jobName, jobId, groupId));
            support.firePropertyChange(getPropertyChangeMetricId(Metrics.recordCount.toString()), -1, recordCount);



            // if no records were produced kill the job
            if (recordCount == 0)
                throw new Exception("SQLPull pulled no records");



        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, jobName, jobId, groupId));
            killComponent();
        }




    }







    private void runFileLoad() {


        try {



            //--------------------------------------------------------------
            // set the file names and paths

            int fileCounterLimit = 100;
            int fileCounter = 1;
            int totalFileCount = 0;
            int totalRecords = 0;


            LoadFolders.checkLoadFolders(fileProps, prefix);
            String workFolder = fileProps.getProperty(Main.fpLoadFolderKW) + "\\" + prefix + "\\" + JobManager.componentWorkFolderNameKW;


            compPassList = new ArrayList<>();

            int curRecCounter = 0;
            boolean hasRecord;
            boolean writeFile = false;

            JSONArray arrayOut;
            arrayOut = new JSONArray();



            // check to see if there are any results
            hasRecord = rs.isBeforeFirst();


            // if there are pull the first row
            if (hasRecord)
                rs.next();
            else
                throw new Exception("SQLPull pulled no records");






            //----------------------------------------------------------------------------------
            //----------------------------------------------------------------------------------
            //----------------------------------------------------------------------------------
            // loop through the result set


            while ( ( hasRecord && loadFlag) || curRecCounter > 0) {

                //System.out.println("hasRecord: " + hasRecord + " | curRecCounter: " + curRecCounter + " | writeFile: " + writeFile + " | totalRecords: " + totalRecords );

                if(!hasRecord && !writeFile) {
                    writeFile = true;
                }

                // don't increment if writing out a file
                if(!writeFile) {
                    totalRecords++;
                    curRecCounter++;
                }



                if (curRecCounter <= fileRecLimit && !writeFile) {


                    arrayOut.put(so.standardRecord("D", prefix, makeObject(rs, rsm).toString(), rowDate));


                    // if there are already the max number of records don't pull the next one
                    if (curRecCounter == fileRecLimit) {
                        writeFile = true;
                    } else {
                        hasRecord = rs.next();
                    }




                } else {

                    totalFileCount++;

                    // write out the file and leave it in the work folder
                    String fileName = so.standardFileName(prefix, "D");
                    String curFilepath = workFolder + "\\" + fileName;
                    bw = new BufferedWriter(new FileWriter(curFilepath));
                    bw.write(arrayOut.toString());
                    bw.flush();
                    bw.close();


                    logger.info(LoggingUtils.getLogEntry("FileName: " + fileName + " | Record count: " + curRecCounter + " | Total records: " + totalRecords + " | Files produced :" + totalFileCount, jobName, jobId, groupId));

                    arrayOut = new JSONArray();

                    compPassList.add(new File(curFilepath));

                    //set the counter back to zero
                    curRecCounter = 0;
                    fileCounter++;


                    if (fileCounter > fileCounterLimit) { fileCounter = 0; }


                    // get ready to start the next file
                    writeFile = false;
                    hasRecord = rs.next();


                } // end work for current record


            } // end looping on record existence

            logger.info(LoggingUtils.getLogEntry("Total Records: " + totalRecords, jobName, jobId, groupId));
            logger.info(LoggingUtils.getLogEntry("Total File Count: " + totalFileCount, jobName, jobId, groupId));

            support.firePropertyChange(getPropertyChangeMetricId(Metrics.recordCount.toString()), -1, totalRecords);
            support.firePropertyChange(getPropertyChangeMetricId(Metrics.fileCount.toString()), -1, totalFileCount);




        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, jobName, jobId, groupId));
            killComponent();
        }


    }






    private JSONObject makeObject(ResultSet rs, ResultSetMetaData rsm) {

        try {

            JSONObject dataObj = new JSONObject();

            for(int i = 1; i <= rsm.getColumnCount(); i++) {

                String colName = rsm.getColumnName(i);

                switch (rsm.getColumnTypeName(i).toLowerCase()) {

                    case "int":
                    case "tinyint":
                    case "bit":
                    case "smallint":
                        int v = rs.getInt(i);
                        if (rs.wasNull())
                            dataObj.put(colName, JSONObject.NULL);
                        else
                            dataObj.put(colName, v);
                        break;
                    case "bigint":
                        long l= rs.getLong(i);
                        if (rs.wasNull())
                            dataObj.put(colName, JSONObject.NULL);
                        else
                            dataObj.put(colName, l);
                        break;
                    case "varchar":
                    case "char":
                    case "text":
                    case "nvarchar":
                    case "nchar":
                    case "ntext":
                    case "datetime":
                    case "date":
                    case "datetimeoffset":
                    case "smalldatetime":
                    case "time":
                        dataObj.put(colName, rs.getString(i));
                        break;
                    case "decimal":
                    case "numeric":
                    case "real":
                        double d = rs.getDouble(i);
                        if (rs.wasNull())
                            dataObj.put(colName, JSONObject.NULL);
                        else
                            dataObj.put(colName, d);
                        break;
                    case "float":
                        float f = rs.getFloat(i);
                        if (rs.wasNull())
                            dataObj.put(colName, JSONObject.NULL);
                        else
                            dataObj.put(colName, f);
                        break;


                } // end switch

            } // end looping on columns


            return dataObj;



        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, jobName, jobId, groupId));
            killComponent();
            return null;
        }




    }





    @Override
    public boolean loadComponent(JSONObject runParms, String context) {


        try {


            this.context = context;
            JSONArray pa = runParms.getJSONArray("Parameters");
            mode = (String) getJSONValue(runParms, "mode", Component.stringType);


            for (int i = 0; i < pa.length(); i++) {

                JSONObject curParm = pa.getJSONObject(i);
                String parmName = (String) curParm.names().get(0);


                switch (parmName) {

                    case "sourceDBConnection":
                        sourceDBType = Connection.DBTypes.valueOf( (String) getJSONValue(curParm, DBConnection.dbConnTypeKW, Component.stringType, parmName) );
                        sourceStatementTimeout = (Integer) getJSONValue(curParm, DBConnection.dbConnStmtTimeoutKW, Component.intType, parmName);
                        sourceDBRef = (String) getJSONValue(curParm, context, Component.stringType, parmName);
                        break;
                    case "sqlFile":
                        sqlFile = (String) getJSONValue(curParm, context, Component.stringType, parmName);
                        fileOrder = (Integer) getJSONValue(curParm, "fileOrder", Component.intType, parmName);
                        break;
                    case "fileRecLimit":
                        fileRecLimit = (Integer) getJSONValue(curParm, context, Component.intType, parmName);
                        break;
                    case "rowDate":
                        rowDate = (Boolean) getJSONValue(curParm, context, Component.booleanType, parmName);
                        break;



                }

            }





            //---------------------------------------------
            // get the sql to run ...


            String[] sqlStmts = LoadSQLFile.loadSQLFile(fileProps, sqlFile, true, jobName, jobId, groupId);
            selectStmt = sqlStmts[fileOrder - 1];

            return false;


        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, jobName, jobId, groupId));
            killComponent();
            return false;
        }


    }







    @Override
    public void closeComponent() {

        try {

            loadFlag = false;

            sourceDBConn.shutdownConnection();

            if (bw != null) {
                bw.close();
            }


        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, jobName, jobId, groupId));
        }

    }







    @Override
    public ArrayList<String> reportDetails() {

        ArrayList<String> a = allCompReportDetails();

        addDetailString("mode", mode, a);
        addDetailString("sqlFile", sqlFile, a);
        addDetailString("fileOrder", fileOrder, a);
        addDetailString("sourceStatementTimeout", sourceStatementTimeout, a);
        addDetailString("fileRecLimit", fileRecLimit, a);
        addDetailString("rowDate", rowDate, a);

        for (String s : selectStmt.split(System.lineSeparator()))
            a.add("     " + s);


        return a;




    }





}
