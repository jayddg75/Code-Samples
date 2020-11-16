package com.ELTTool;

import sun.reflect.Reflection;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 *The is a backbone class that standardizes log entries in various ways
 */
public class LoggingUtils {


    private static Logger logger = Logger.getLogger(LoggingUtils.class.getName());







    public static String getErrorEntry(Exception e, String jobName, Long jobId, Long groupId) {

        try {

            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);

            return  formatLogHeader(jobName, jobId, groupId) + " | stackTrace: "  + sw.toString();

        } catch (Exception ex) {
            logger.severe(ex.getMessage());
            return null;
        }

    }







    public static String getLogEntry(String message, String jobName, Long jobId, Long groupId) {

        try {

            return formatLogHeader(jobName, jobId, groupId) + " | message: "  + message.replace("\\|", ":");

        } catch (Exception ex) {
            logger.severe(ex.getMessage());
            return null;
        }

    }



    private static String formatLogHeader(String jobName, Long jobId, Long groupId) {

        String jobIdOut = jobId != null ? String.valueOf(jobId) : "";
        String groupOut = groupId != null ? String.valueOf(groupId) : "";
        String jobNameOut = jobName != null ? jobName : "";

        return "jobName: " + jobNameOut + " | jobId: " + jobIdOut + " | groupId: " + groupOut;


    }


}
