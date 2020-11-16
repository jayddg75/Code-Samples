package com.ELTTool;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;



/**
 *Used to shutdown executors in a proper fashion and used in various places
 */
public class ShutdownExecutor {



    private static Logger logger = Logger.getLogger(ShutdownExecutor.class.getName());





    public static void shutdownSES(ScheduledExecutorService ses, String jobName, Long jobId, Long groupId) {

        try {

            logger.info(LoggingUtils.getLogEntry("Checking to see if SES needs to shut down", jobName, jobId, groupId));

            if ( !(ses == null) ) {

                if (!ses.isShutdown()) {

                    logger.info(LoggingUtils.getLogEntry("SES is up and running ... trying to shut it down", jobName, jobId, groupId));

                    ses.shutdown();

                    // wait 10 seconds for all the tasks to complete
                    if (!ses.awaitTermination(3, TimeUnit.SECONDS))
                        ses.shutdownNow();

                    if (!ses.awaitTermination(2, TimeUnit.SECONDS))
                        logger.info(LoggingUtils.getLogEntry("Tasks didn't terminate", jobName, jobId, groupId));


                }


            }



        } catch (InterruptedException e) {

            logger.info(LoggingUtils.getLogEntry("SES: Interrupting the thread", jobName, jobId, groupId));
            ses.shutdownNow();
            Thread.currentThread().interrupt();

        }

    }





    public static void shutdownSES(ExecutorService ses, String jobName, Long jobId, Long groupId) {

        try {

            logger.info(LoggingUtils.getLogEntry("Checking to see if SES needs to shut down", jobName, jobId, groupId));

            if ( !(ses == null) ) {

                if (!ses.isShutdown()) {

                    logger.info(LoggingUtils.getLogEntry("SES is up and running ... trying to shut it down", jobName, jobId, groupId));

                    ses.shutdown();

                    // wait 10 seconds for all the tasks to complete
                    if (!ses.awaitTermination(2, TimeUnit.SECONDS))
                        ses.shutdownNow();

                    if (!ses.awaitTermination(2, TimeUnit.SECONDS))
                        logger.info(LoggingUtils.getLogEntry("Tasks didn't terminate", jobName, jobId, groupId));


                }


            }



        } catch (InterruptedException e) {

            logger.info(LoggingUtils.getLogEntry("SES: Interrupting the thread", jobName, jobId, groupId));
            ses.shutdownNow();
            Thread.currentThread().interrupt();

        }

    }




}
