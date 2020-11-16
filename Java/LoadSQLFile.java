package com.ELTTool;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.Logger;



/**
 *The is a helper class that get the sql file and returns the sql statements in a String[]
 */
public class LoadSQLFile {



    private static Logger logger = Logger.getLogger(LoadSQLFile.class.getName());





    public static String[] loadSQLFile(Properties fileProps, String sqlFile, boolean jobSQL, String jobName, Long jobId, Long groupId) {


        try {

            String sqlPath;

            // load the sql file
            if (jobSQL)
                sqlPath = fileProps.getProperty(Main.fpJobFolderKW) + "\\" + sqlFile;
            else
                sqlPath = fileProps.getProperty(Main.fpSqlFolderKW) + "\\" + sqlFile;




            logger.info(LoggingUtils.getLogEntry("SQL File: " + sqlPath, jobName, jobId, groupId));

            //System.out.println(sqlPath);

            BufferedReader br = new BufferedReader(new FileReader(sqlPath));
            StringBuilder sb = new StringBuilder();
            String line;

            while ( (line = br.readLine()) != null )
                sb.append(line + System.lineSeparator());


            String sqlSeparator = "<::-----sqlStmt-----::>";

            return sb.toString().split(sqlSeparator);



        } catch (IOException e) {
            logger.severe(LoggingUtils.getErrorEntry(e, jobName, jobId, groupId));
            return null;
        }


    }



}
