package com.ELTTool;


import org.json.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.atomic.*;
import java.util.logging.Logger;



/**
 *This component will grab files and load them into the associated landing table and runs in two styles
 * <ul>
 *     <li><i>batch</i> - pick up a list of files in the work folder from the job</li>
 *     <li><i>continuous</i> - look for files in the load folder</li>
 * </ul>
 *<br>
 *<br>
 *Job File Requirements
 * <br>
 * <ul>
     * <li>Order - Required - The order this component should run in: 1 (1st), 2 (2nd) ... </li>
     * <li>Component - Required - <i>FileToLanding</i></li>
     * <li>Prefix - Required - The number in the Prefix array needed: 1, 2, etc...</li>
     * <li>RunStyle - Required - <i>batch</i> or <i>continuous</i></li>
     * <li>Parameters - Required - list of parms described below
     * <ul>
         * <li>dbConnection - the DBConnection desired to use for file table inserts and landing loads</li>
         * <li>fileCheckSeconds - Required for continuous - how ofter to look for files in the load folder</li>
         * <li>workFileTimeLimitSecs - Future Use - how long to wait before a file in the work folder is considered problematic and needs attention</li>
     * </ul>
     * </li>
 * </ul>
 */
public class FileToLanding extends Component {


    private static Logger logger = Logger.getLogger(FileToLanding.class.getName());




    private Properties fileProps;
    private File topWorkingFolder;
    private File loadFromFolder;
    private File workFolder;


    private String sqlFile = "FileToLanding.txt";
    private String putStmt;
    private String copyIntoStmt;

    private String backendSchema;

    private int fileCheckSeconds;
    private int workFileTimeLimitSecs;

    private Connection.DBTypes dbConnType;
    private String dbRef;
    private int statementTimeout;

    private ConnectionManager cm;
    private DBConnection dbConn;
    private DBHelper dh;


    private AtomicLong totalFileCount;
    private AtomicLong totalCopyIntoCount;

    private AtomicBoolean loadingFiles;

    private Scheduler putSched;
    private Scheduler copySched;









    public FileToLanding(ConnectionManager cm, DBHelper dh, Properties fileProps, String backendSchema) {

        try {

            this.cm = cm;
            loadingFiles = new AtomicBoolean(false);
            this.fileProps = fileProps;
            this.dh = dh;
            this.backendSchema = backendSchema;

            this.totalFileCount = new AtomicLong(0);
            this.totalCopyIntoCount = new AtomicLong(0);


        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, jobName, jobId, groupId));
            killComponent();
        }

    }










    public void runComponent() {

        try {


            topWorkingFolder = new File(fileProps.getProperty(Main.fpLoadFolderKW) + "\\" + prefix);
            loadFromFolder = new File(topWorkingFolder.getPath() + "\\" + JobManager.componentLoadFolderNameKW);
            workFolder = new File(topWorkingFolder.getPath() + "\\" + JobManager.componentWorkFolderNameKW);

            // start the DB connection
            dbConn = cm.getDBConnection(dbRef, dbConnType, jobName, FileToLanding.class.getName(), jobId, groupId);
            dbConn.startConnection(statementTimeout, jobId, groupId);



            if (runStyle.equals(JobManager.contRunStyle)) {

                logger.info(LoggingUtils.getLogEntry("Running continuous", jobName, jobId, groupId));

                Long checkMillis = fileCheckSeconds * 1000L;


                // start up monitoring job and leave it running
                // offset the copy to half the interval so they run in between each other

                putSched = new Scheduler(new putCheck(), checkMillis, jobName, jobId, groupId);
                copySched = new Scheduler(new copyRun(), checkMillis, checkMillis / 2L,  jobName, jobId, groupId);
                putSched.runScheduler();
                copySched.runScheduler();


            } else {

                logger.info(LoggingUtils.getLogEntry("Starting batch run", jobName, jobId, groupId));

                File[] batchFileList = new File[compPassList.size()];

                for(int i = 0; i < compPassList.size(); i++) {
                    batchFileList[i] = compPassList.get(i);
                }


                putTableStaging(batchFileList);
                runCopyValidate();

                completeComponent();

            }




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










    public boolean loadComponent(JSONObject runParms, String context) {


        try {


            this.context = context;
            JSONArray pa = runParms.getJSONArray("Parameters");


            for (int i = 0; i < pa.length(); i++) {

                JSONObject curParm = pa.getJSONObject(i);
                String parmName = (String) curParm.names().get(0);


                switch (parmName) {

                    case "fileCheckSeconds":
                        fileCheckSeconds = (Integer) getJSONValue(curParm, context, Component.intType, parmName);
                        break;
                    case "dbConnection":
                        dbConnType = Connection.DBTypes.valueOf( (String) getJSONValue(curParm, DBConnection.dbConnTypeKW, Component.stringType, parmName) );
                        statementTimeout = (Integer) getJSONValue(curParm, DBConnection.dbConnStmtTimeoutKW, Component.intType, parmName);
                        dbRef = (String) getJSONValue(curParm, context, Component.stringType, parmName);
                        break;
                    case "workFileTimeLimitSecs":
                        workFileTimeLimitSecs = (Integer) getJSONValue(curParm, context, Component.intType, parmName);
                        break;

                }

            }





            // load the SQL File

            String[] sqlStmts = LoadSQLFile.loadSQLFile(fileProps, sqlFile, false, jobName, jobId, groupId);

            for (int i = 0; i < sqlStmts.length; i++) {


                String stmt = sqlStmts[i]
                        .replace(Replacement.prefixKW, prefix)
                        .replace(Replacement.schemaKW, backendSchema);


                switch (i) {

                    case 0:
                        putStmt = stmt;
                        break;
                    case 1:
                        copyIntoStmt = stmt;
                        break;

                }
            }

            return true;

        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, jobName, jobId, groupId));
            killComponent();
            return false;
        }


    }





    public void closeComponent() {

        try {

            logger.info(LoggingUtils.getLogEntry("Closing component: " + FileToLanding.class.getName(), jobName, jobId, groupId));
            dbConn.shutdownConnection();

            if(runStyle.equals(JobManager.contRunStyle)) {
                support.firePropertyChange(getPropertyChangeMetricId(Metrics.totalFileCount.toString()), -1, totalFileCount.get());
                support.firePropertyChange(getPropertyChangeMetricId(Metrics.totalPutCount.toString()), -1, totalCopyIntoCount.get());
            }


            if (runStyle.equals(JobManager.contRunStyle)) {

                copySched.stopScheduler(true);
                putSched.stopScheduler(true);

            }


        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, jobName, jobId, groupId));
        }

    }







    private void putTableStaging(File[] fileList) {


        try {

            logger.info(LoggingUtils.getLogEntry("Putting " + fileList.length + " files into Snowflake: ", jobName, jobId, groupId));

            loadingFiles.set(true);


            // loop through the files it saw that were ready to load
            for (int i = 0; i < fileList.length; i++) {


                String fileName = fileList[i].getName();
                String filePath = fileList[i].getPath();

                // count the records in the file

                BufferedReader br = new BufferedReader(new FileReader(filePath));
                StringBuilder sb = new StringBuilder();
                String line;

                while ( (line = br.readLine()) != null ){
                    sb.append(line).append(System.lineSeparator());
                }

                br.close();

                int recordCount = new JSONArray(sb.toString()).length();


                // execute the put statement
                String finalPut = putStmt.replace("${filePath}", fileList[i].getPath());

                dh.fileTableInsert(backendSchema, prefix, fileName, recordCount, jobName);
                dbConn.executeStatement(finalPut, false);

                // delete the file

                while ( !fileList[i].delete() ){
                    // don't continue until the file is deleted
                }

            }

            totalFileCount.getAndAdd(fileList.length);
            support.firePropertyChange(getPropertyChangeMetricId(Metrics.fileCount.toString()), -1, fileList.length);

            loadingFiles.set(false);



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





    /**
     *This inner class will run at intervals and check to see if anything needs to be put into a table stage
     */
    class putCheck implements Runnable {



        @Override
        public void run() {


            try {

                if (!copySched.isRunning())
                    throw new Exception("The copy run check executor has stopped!");

                File[] fileList = loadFromFolder.listFiles();

                //logger.warning("fileList size: " + fileList.length + " | loadingFiles: " + loadingFiles.get());

                if (fileList.length > 0 && !loadingFiles.get()) {

                    logger.info(LoggingUtils.getLogEntry("File to Stage: " + fileList.length, jobName, jobId, groupId));
                    putTableStaging(fileList);

                }



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




    /**
     *This inner class will check for any staged data in a table stage and if it finds any, run a copy into stmt
     */
    class copyRun implements Runnable {


        @Override
        public void run() {

            try {

                if(!putSched.isRunning())
                    throw new Exception("The load check executor has stopped!");

                runCopyValidate();



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





    private void runCopyValidate() {

        try {


            String listFilesStmt = dh.listFiles(backendSchema, prefix);
            ResultSet rs = dbConn.executeQuery(listFilesStmt, true);


            boolean runFlag = false;

            while(rs.next())
                if (!runFlag)
                    runFlag = true;


            if (runFlag) {

                logger.info(LoggingUtils.getLogEntry("Copying data from staging to landing table", jobName, jobId, groupId));
                totalCopyIntoCount.getAndIncrement();
                support.firePropertyChange(getPropertyChangeMetricId(Metrics.copyInto.toString()), -1, 1);
                dbConn.executeStatement(copyIntoStmt, true);
            }

            rs.close();



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









    @Override
    public ArrayList<String> reportDetails() {


        ArrayList<String> a = allCompReportDetails();

        addDetailString("fileCheckSeconds", fileCheckSeconds, a);
        addDetailString("workFileTimeLimitSecs", workFileTimeLimitSecs, a);
        addDetailString("dbRef", dbRef, a);
        addDetailString("statementTimeout", statementTimeout, a);

        return a;


    }







}
