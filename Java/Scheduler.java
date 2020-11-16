package com.ELTTool;



import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;


public class Scheduler {



    private static Logger logger = Logger.getLogger(Scheduler.class.getName());

    private String jobName;
    private Long jobId;
    private Long groupId;

    private AtomicBoolean run;
    private Runnable sched;
    private Long intervalMillis;
    private Long initialDelayMillis;

    private ExecutorService esMain;
    private Future<?> futureMain;

    private boolean fixedScheduleRun;

    private AtomicBoolean running;







    //------------------------------------------------------------------------------------------------------
    // constructors



    public Scheduler(Runnable sched, Long intervalMillis, String jobName, Long jobId, Long groupId) {

        this.sched = sched;
        this.intervalMillis = intervalMillis;
        this.jobName = jobName;
        this.jobId = jobId;
        this.groupId = groupId;

        run = new AtomicBoolean(false);
        fixedScheduleRun = true;
        running = new AtomicBoolean();
        initialDelayMillis = 0L;

    }




    public Scheduler(Runnable sched, Long intervalMillis, Long initialDelayMillis, String jobName, Long jobId, Long groupId) {

        this.sched = sched;
        this.intervalMillis = intervalMillis;
        this.jobName = jobName;
        this.jobId = jobId;
        this.groupId = groupId;
        this.initialDelayMillis = initialDelayMillis;

        run = new AtomicBoolean(false);
        fixedScheduleRun = true;
        running = new AtomicBoolean();

    }




    public Scheduler(Runnable sched, String jobName, Long jobId, Long groupId) {

        this.sched = sched;
        this.jobName = jobName;
        this.jobId = jobId;
        this.groupId = groupId;

        fixedScheduleRun = false;
        running = new AtomicBoolean();

    }





    public boolean isRunning() { return running.get(); }









    //------------------------------------------------------------------------------------------------------
    // main methods


    // this starts the scheduler
    public void runScheduler() throws Exception {

        running.set(true);
        esMain = Executors.newSingleThreadExecutor();

        if(fixedScheduleRun) {

            futureMain = esMain.submit(new FixedScheduledRun());

        } else {

            futureMain = esMain.submit(new OnceRun());
            stopScheduler(false);

        }



    }









    public class FixedScheduledRun implements Runnable {



        @Override
        public void run() {


            try {




                run.set(true);
                ExecutorService esTask = Executors.newSingleThreadExecutor();
                Future<?> futureTask = null;


                // wait for the inital delay if asked for
                Thread.sleep(initialDelayMillis);


                while(run.get()) {


                    //logger.info(LoggingUtils.getLogEntry("Starting scheduled execution task", jobName, jobId, groupId));
                    Long startTime = new Date().getTime();



                    //------------------------------------------------------------
                    // start the runnable
                    futureTask = esTask.submit(sched);



                    // if the runnable is not done, skip the next execution and wait the interval to check again
                    // this just skips the next run to subsequent task don't build up.
                    while (!futureTask.isDone())
                        Thread.sleep(1000);



                    Long endTime = new Date().getTime();
                    //logger.info(LoggingUtils.getLogEntry("Finished scheduled execution task", jobName, jobId, groupId));



                    //-------------------------------------------------------------
                    // check what to do
                    // if it finished before, then wait the time till the next interval
                    // else

                    Long diffTime = new Date().getTime() - startTime;
                    if (diffTime <= intervalMillis) {


                        Long timeLeft = intervalMillis - diffTime;
                        //logger.info("Wait till next run: " + timeLeft);
                        Thread.sleep(timeLeft);

                    } else {

                        // get how long it ran ...
                        Long runTime = endTime - startTime;
                        Long nextRun = runTime % intervalMillis;
                        //logger.info("Wait till next run: " + nextRun);

                        Thread.sleep(nextRun);

                    }


                }

                stopSchedulerInternal(esTask, futureTask, false);


            } catch (Exception e) {

                // just rethrow the error here so the job is forced to deal with the exception and decide how to handle it
                throw new RuntimeException(e);

            }


        }


    }






    public class OnceRun implements Runnable {



        @Override
        public void run() {


            try {


                run.set(true);
                ExecutorService esTask = Executors.newSingleThreadExecutor();


                logger.info(LoggingUtils.getLogEntry("Starting scheduled execution task", jobName, jobId, groupId));
                Long startTime = new Date().getTime();


                //------------------------------------------------------------
                // start the runnable
                Future<?> futureTask = esTask.submit(sched);


                Long endTime = new Date().getTime();
                logger.info(LoggingUtils.getLogEntry("Finished scheduled execution task", jobName, jobId, groupId));

                stopSchedulerInternal(esTask, futureTask, false);


            } catch (Exception e) {

                // just rethrow the error here so the job is forced to deal with the exception and decide how to handle it
                throw new RuntimeException(e);

            }


        }


    }






    private void stopSchedulerInternal(ExecutorService es, Future<?> future, boolean hardShutdown) throws Exception {

        run.set(false);

        if(!future.isDone())
            future.cancel(hardShutdown);

        ShutdownExecutor.shutdownSES(es, jobName, jobId, groupId);

        running.set(false);


    }




    public void stopScheduler(boolean hardShutdown) throws Exception {

        run.set(false);

        if(!futureMain.isDone())
            futureMain.cancel(hardShutdown);

        ShutdownExecutor.shutdownSES(esMain, jobName, jobId, groupId);

        running.set(false);


    }









}
