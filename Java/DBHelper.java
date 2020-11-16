package com.ELTTool;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;



/**
 *This class is used by various components to help ease standard db stuff, like checking for tables and fileTable inserts
 */
public class DBHelper {





    private static Logger logger = Logger.getLogger(DBHelper.class.getName());

    private ConnectionManager cm;

    private String backendSchema;

    private DBConnection dbConn;

    private final String sqlFile = "DBHelper.txt";


    private String tableCheck;
    private String landingDDL;
    private String fileDDL;
    private String auditDDL;
    private String errorDDL;
    private String fileInsert;
    private String metricsInsert;
    private String metricsDDL;
    private String listFiles;
    private String jobSummary;



    private static final String landingKW = "Landing";
    private static final String fileKW = "File";
    private static final String auditKW = "Audit";
    private static final String errorLogKW = "Error_Log";
    private static final String metricCollectorKW = "Metrics_Collector";






    public DBHelper(Properties fileProps, String backendSchema, String backendDBRef, Connection.DBTypes backendDBType, ConnectionManager cm) throws Exception {

        this.backendSchema = backendSchema;
        this.cm = cm;

        dbConn = cm.getDBConnection(backendDBRef, backendDBType, DBHelper.class.getSimpleName(), DBHelper.class.getSimpleName(), null, null);
        dbConn.startConnection(120, null, null);

        String[] sqlStmts = LoadSQLFile.loadSQLFile(fileProps, sqlFile, false, DBHelper.class.getSimpleName(), null, null);


        if (sqlStmts.length != 10 ) {
            throw new Error("Sql statement count is not right for DBHelper");
        }


        for (int i = 0; i < sqlStmts.length; i++) {

            switch (i) {

                case 0:
                    landingDDL = sqlStmts[i];
                    break;
                case 1:
                    fileDDL = sqlStmts[i];
                    break;
                case 2:
                    auditDDL = sqlStmts[i];
                    break;
                case 3:
                    errorDDL = sqlStmts[i];
                    break;
                case 4:
                    tableCheck = sqlStmts[i];
                    break;
                case 5:
                    fileInsert = sqlStmts[i];
                    break;
                case 6:
                    metricsInsert = sqlStmts[i];
                    break;
                case 7:
                    metricsDDL = sqlStmts[i];
                    break;
                case 8:
                    listFiles = sqlStmts[i];
                    break;
                case 9:
                    jobSummary = sqlStmts[i];
                    break;

            }

        }


    }





    // this should get run on any component that produces records to ensure all th tables are in place
    public void tableCheckRun(String prefix, String schema, boolean baseTables) throws SQLException {


        String[][] stmts = null;

        if(baseTables) {

            stmts = new String[2][2];

            stmts [0][0] = errorLogKW;
            stmts [0][1] = errorDDL;

            stmts [1][0] = metricCollectorKW;
            stmts [1][1] = metricsDDL;

        } else {

            stmts = new String[3][2];

            stmts [0][0] = landingKW;
            stmts [0][1] = landingDDL;

            stmts [1][0] = fileKW;
            stmts [1][1] = fileDDL;

            stmts [2][0] = auditKW;
            stmts [2][1] = auditDDL;



        }





        for(int i = 0; i< stmts.length; i++){


            String ddlOut = Replacement.schemaAndPrefix(schema, prefix, stmts[i][1]);

            String tableName = null;

            if (baseTables)
                tableName = stmts[i][0];
            else
                tableName = prefix + "_" + stmts[i][0];


            String tableCheckStmt = Replacement.schemaAndPrefix(schema, prefix, tableCheck)
                    .replace(Replacement.tableNameKW, tableName);


            //System.out.println(tableCheckStmt);

            //-----------------------------------------------
            // check and execute if needed ...

            ResultSet rs = dbConn.executeQuery(tableCheckStmt, false);
            rs.next();
            int tableCount = rs.getInt(1);


            if ( tableCount == 0 ){
                dbConn.executeStatement(ddlOut, false);
            }





        } // end check to see if need to run checks


    }








    public void fileTableInsert(String schema, String prefix, String fileName, int recCount, String jobName) throws SQLException {

        String fileInsertStmt =  fileInsert.replace(Replacement.schemaKW, schema)
                        .replace(Replacement.prefixKW, prefix)
                        .replace(Replacement.fileNameKW, fileName)
                        .replace(Replacement.recCountKW, String.valueOf(recCount));


        dbConn.executeStatement(fileInsertStmt, true);


    }






    public String metricsInsertPrep(String schema) {

        return Replacement.schemaAndPrefix(schema, "", metricsInsert);

    }






    public String listFiles(String schema, String prefix) {

        return Replacement.schemaAndPrefix(schema, prefix, listFiles);

    }







    public ResultSet getJobSummary(Long jobId) throws SQLException {

        String execStmt = Replacement.schemaAndPrefix(backendSchema, "", jobSummary)
                            .replace("${jobId}", String.valueOf(jobId))
                                .replace("${status}", Metrics.status.toString());

        return dbConn.executeQuery(execStmt, false);

    }




}
