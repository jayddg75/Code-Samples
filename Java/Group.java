package com.ELTTool;


import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;




/**
 *Groups are meant to run a set of jobs and are for convenience, example run all core dimension loads. Use the wait flag on the jobs to run things in parallel and only hold things up for dependencies.<br>
 *<br>
 *Group file spec ...<br>
 *<br>
 *name - required - name for the group<br>
 *members - required - array of json objects<br>
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;name - required - job name<br>
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;order - required - the run order, must match the array order and starts on 1<br>
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;wait - required - boolean (true or false); whether to wait for the job to complete before starting the next one<br>
 *<br>
 *<br>
 *Example ...<br>
 *<br>
 *"name": "CoreDims",<br>
 *"jobs": [<br>
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{<br>
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"name": "DRIV_D_B",<br>
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"order": 1,<br>
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"wait": false<br>
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;},<br>
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{<br>
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"name": "PTRM_D_B",<br>
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"order": 2,<br>
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"wait": false<br>
 *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;},<br>
 *]<br>
 *}
 *
 *
 */
public class Group implements Runnable {



    private static Logger logger = Logger.getLogger(Group.class.getName());

    private String groupName;
    private String context;
    private AtomicInteger status;

    private ArrayList<Member> memberList;
    private JobManager jm;


    private AtomicInteger runningJobOrder;
    private AtomicBoolean killGroup;
    private AtomicBoolean jobComplete;
    private AtomicBoolean allJobsComplete;

    private Scheduler sched;

    private AtomicLong groupId;
    private AtomicLong runTime;









    public Group(ArrayList<Member> memberList, JobManager jm, String groupName) {

        this.memberList = memberList;
        this.jm = jm;
        this.groupName = groupName;
        killGroup = new AtomicBoolean(false);
        jobComplete = new AtomicBoolean(false);
        runningJobOrder = new AtomicInteger(-1);
        allJobsComplete = new AtomicBoolean(false);
        status = new AtomicInteger(Status.Stopped.getStatusCode());

    }





    public String getGroupName() {
        return groupName;
    }

    public ArrayList<Member> getMemberList() {
        return memberList;
    }

    public String getContext() {
        return context;
    }

    public void setContext(String context) {
        this.context = context;
    }

    public int getStatus() { return status.get(); }







    @Override
    public void run() {


        try {


            groupId = new AtomicLong(new Date().getTime());

            // set it to running ...
            status.set(Status.Running.getStatusCode());



            logger.warning(LoggingUtils.getLogEntry("***********************************************************", groupName, null, groupId.get()));
            logger.warning(LoggingUtils.getLogEntry("***********************************************************", groupName, null, groupId.get()));
            logger.warning(LoggingUtils.getLogEntry("***********************************************************", groupName, null, groupId.get()));
            logger.warning(LoggingUtils.getLogEntry("Starting group: " + groupName, groupName, null, groupId.get()));
            logger.warning(LoggingUtils.getLogEntry("***********************************************************", groupName, null, groupId.get()));
            logger.warning(LoggingUtils.getLogEntry("***********************************************************", groupName, null, groupId.get()));
            logger.warning(LoggingUtils.getLogEntry("***********************************************************", groupName, null, groupId.get()));


            killGroup.set(false);
            jobComplete.set(false);
            runningJobOrder.set(-1);
            allJobsComplete.set(false);

            int jobCompletedCount = 0;

            // start the checker to see if the group needs to be killed
            sched = new Scheduler(new CheckGroupRun(), 2000L, 2000L, null, null, groupId.get());
            sched.runScheduler();

            //-----------------------------------------------------------------------------
            // set all the group information on the jobs about to be run for identification

            for(Member m : memberList)
                jm.setGroupRun(m.getJobName(), groupId.get(), groupName, m.getOrder());





            //-----------------------------------------------------------------------------
            // run the members

            for (Member m : memberList)
                if (!killGroup.get()) {


                    //-----------------------------------------------
                    // pre-run setup

                    if (m.getWait())
                        jobComplete.set(false);
                    else
                        jobComplete.set(true);


                    runningJobOrder.set(m.getOrder());




                    //----------------------------------------------
                    // run the job


                    jm.startJob(m.getJobName(), true);


                    //----------------------------------------------
                    // wait for the job to complete if passed in that way

                    while (!jobComplete.get())
                        Thread.sleep(2000);


                }

                stopGroup();


        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, null, null, groupId.get()));
            killGroup();
        }


    }









    public void stopGroup() {


        try {


            logger.warning(LoggingUtils.getLogEntry("***********************************************************", groupName, null, groupId.get()));
            logger.warning(LoggingUtils.getLogEntry("***********************************************************", groupName, null, groupId.get()));
            logger.warning(LoggingUtils.getLogEntry("***********************************************************", groupName, null, groupId.get()));
            logger.warning(LoggingUtils.getLogEntry("Stopping group: " + groupName, groupName, null, groupId.get()));
            logger.warning(LoggingUtils.getLogEntry("***********************************************************", groupName, null, groupId.get()));
            logger.warning(LoggingUtils.getLogEntry("***********************************************************", groupName, null, groupId.get()));
            logger.warning(LoggingUtils.getLogEntry("***********************************************************", groupName, null, groupId.get()));

            sched.stopScheduler(true);

            status.set(Status.Stopped.getStatusCode());


        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, null, null, groupId.get()));
        }

    }







    public void killGroup(){

        logger.warning(LoggingUtils.getLogEntry("Killing group: " + groupName, null, null, groupId.get()));

        status.set(Status.Error.getStatusCode());
        killGroup.set(true);


        // go through and stop all the jobs for the group
        for(Member m : memberList)
            jm.stopJob(m.getJobName());

        stopGroup();

    }





    class CheckGroupRun implements Runnable {


        @Override
        public void run() {



            try {



                //-----------------------------------------
                // check for the current job status

                int curJobStatus = -99;
                int batchCount = 0;
                int batchCompletedCount = 0;


                for (Member m : memberList)
                    if (m.getOrder() == runningJobOrder.get()) {

                        curJobStatus =  jm.getJobStatus(m.getJobName());


                        if (!killGroup.get() && curJobStatus == Status.Error.getStatusCode()) {


                            // if the job reports an error kill the group if not already doing so
                            killGroup.set(true);
                            killGroup();


                        } else {


                            // go through each component in the current job and if all the
                            // batch ones report that they have completed then mark the job as completed
                            // that way any continuous components will continue to run

                            if (!jobComplete.get()) {

                                batchCount = 0;
                                batchCompletedCount = 0;

                                for (Component c : jm.getJob(m.getJobName()).getSteps()) {

                                    if(c.getRunStyle().equals(JobManager.batchRunStyle)) {

                                        batchCount++;
                                        if (c.getStatus() == Status.Completed.getStatusCode())
                                            batchCompletedCount++;

                                    }



                                }


                                // if all batch components are complete set job complete
                                if (batchCompletedCount == batchCount)
                                    jobComplete.set(true);

                            }


                        }



                    } // end going through current member



            } catch (Exception e) {
                logger.severe(LoggingUtils.getErrorEntry(e, null, null, groupId.get()));
                killGroup();
            }




        } // end run




    }





}