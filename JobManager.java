package com.ELTTool;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Properties;
import java.util.logging.Logger;
import javax.swing.JComboBox;
import javax.swing.tree.DefaultMutableTreeNode;
import org.json.*;


/**
 *The job manager loads, reloads, runs, and stops jobs and considered a pivotal class operationally
 */
public class JobManager {




    private static Logger logger = Logger.getLogger(JobManager.class.getName());


    private ArrayList<Job> jobs;
    private ConnectionManager cm;
    private Properties fileProps;
    private DBHelper dh;
    private MetricsCollector mc;
    private GroupManager gm;

    private String context;

    private String backendSchema;
    private String backendDBRef;
    private Connection.DBTypes backendDBType;
    private String mongoRef;

    private boolean runTableChecks;
    private boolean individualStartComps;


    public static final String contRunStyle = "continuous";
    public static final String batchRunStyle = "batch";

    public static final String stopKW = "Stop";
    public static final String startKW = "Start";

    public static final String componentWorkFolderNameKW = "Work";
    public static final String componentLoadFolderNameKW = "Load";

    private JComboBox<String> groupCB;



    //----------------------------------------------------
    // constructor, getters, setters ...

    public JobManager(Properties fileProps, ConnectionManager cm, DBHelper dh, MetricsCollector mc, String backendSchema,
                      String backendDBRef, Connection.DBTypes backendDBType, boolean runTableChecks, boolean individualStartComps,
                      JComboBox<String> groupCB, String context, String mongoRef) {

            this.fileProps = fileProps;
            jobs = new ArrayList<>();
            this.cm = cm;
            this.dh = dh;
            this.mc = mc;
            this.backendSchema = backendSchema;
            this.backendDBRef = backendDBRef;
            this.backendDBType = backendDBType;
            this.runTableChecks = runTableChecks;
            this.individualStartComps = individualStartComps;
            this.groupCB = groupCB;
            this.context = context;
            this.mongoRef = mongoRef;

    }











    public void setGroupManager(GroupManager gm) {
        this.gm = gm;
    }




    //----------------------------------------------------
    // this will load a job from a file

    public void loadJob(File f, Boolean supressMessage) {



        try {



            String jobName = f.getName().substring(0, f.getName().length() - 5);

            logger.info("Loading job: "+ jobName);

            boolean loadJob = true;




            //-------------------------------------------------------------------
            //-------------------------------------------------------------------
            // check to see if the job was already loaded


            boolean checkFlag = false;
            Job sameJob = null;

            for (Job j :  jobs) {

                if (j.getJobName().equals(jobName)) {

                  checkFlag = true;
                  sameJob = j;

                }

            }



            if (checkFlag) {

                int r = GetInput.GetYesNo("Choose", "Do you want to replace the existing job?");

                if (r == 0) {

                    // stop the job if it is running ...
                    if (sameJob.getStatus() > Status.Stopped.getStatusCode()){
                        sameJob.stopJob();
                    }

                    // remove the job from the jobs array and null it out
                    jobs.remove(sameJob);
                    sameJob = null;

                } else {
                    loadJob = false;
                }

            }




            //-------------------------------------------------------------------
            //-------------------------------------------------------------------
            // if the job needs to load, then load it

            if (loadJob) {


                //-------------------------------------------------------
                // read the file and convert to JSON object

                BufferedReader br = new BufferedReader(new FileReader(f));
                StringBuilder sb = new StringBuilder();

                String line;
                while ( (line = br.readLine()) != null ) {
                    sb.append(line);
                }


                br.close();

                JSONObject js = new JSONObject(sb.toString());

                JSONArray prefix = js.getJSONArray("Prefix");
                String category = js.getString("Category");
                String description = js.getString("Description");


                String checkMessage = null;
                boolean checkVal = false;

                if (prefix.length() == 0) {
                    checkMessage = "prefix";
                    checkVal = true;
                } else if (category.isEmpty()) {
                    checkMessage = "category";
                    checkVal = true;
                }


                if (checkVal) {
                    throw new Exception(checkMessage + " was not passed in for the job");
                }


                Job j = new Job(cm, fileProps, prefix, category, jobName, js, dh, mc,
                                    backendSchema, backendDBRef, backendDBType, runTableChecks,
                                        individualStartComps, description, context, mongoRef);
                jobs.add(j);
                j.initializeComponents();

                logger.info("Loaded job: "+ jobName + " | prefix: " + prefix + " | category: " + category);

                if (!supressMessage) {
                    NotificationBox.displayNotification("Successfully added job");
                }


            }





        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, null, null, null));
        }


    }








    public Long getCurrentJobId(String jobName) {

        Long curId = null;
        for(Job j : jobs)
            if(j.getJobName().equals(jobName))
                curId = j.getJobId();

        return curId;


    }




    //----------------------------------------------------
    // this will start a job on a new thread

    public void startJob(String jobName, boolean silentMode) {


        try {


            for (Job j : jobs) {

                //System.out.println(j.getJobName());

                if (j.getJobName().equals(jobName)) {

                    logger.info("Job Status: " + j.getStatus());

                    // only start stopped jobs
                    if (j.getStatus() == Status.Stopped.getStatusCode()) {


                        // start the job
                        j.setPreJob(silentMode, mc);

                        Runnable r = j;
                        Thread th = new Thread(r);
                        th.setName(jobName);
                        th.start();


                    } else {
                        NotificationBox.displayNotification("Job is already running: " + jobName);
                    }

                }
            }

            logger.fine("Started job: " + jobName);

        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, null, null, null));
        }


    }






    public void stopJob(String jobName) {

        try {


            logger.info("Stopping job: " + jobName);

            for (Job j : jobs)
                if (j.getJobName().equals(jobName))
                    if(j.getStatus() > Status.Stopped.getStatusCode())  // if the job is running or started
                        j.stopJob();


        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, null, null, null));
        }


    }



    //----------------------------------------------------
    // this will list all the jobs and attributes

    public Object[][] listJobs(String jobStatusFilter, String groupNameFilter) {


        try {



            ArrayList<Job> filterList = new ArrayList<>();
            boolean allStatus = jobStatusFilter.equals(Status.All.toString());
            boolean allGroups = groupNameFilter.equals(Status.All.toString());


            if ( allStatus && allGroups ) {

                filterList = jobs;

            } else {


                // apply the filter(s) asked for

                if ( allStatus && !allGroups ) {


                    // on group named is filtered
                    for (Group g : gm.getGroups())
                        if (g.getGroupName().equals(groupNameFilter))
                            for (Job j : jobs)
                                for (Member m : g.getMemberList())
                                    if (j.getJobName().equals(m.getJobName()))
                                        filterList.add(j);


                } else if ( !allStatus && allGroups ) {

                    // only job status is filtered
                    for (Job j : jobs)
                        if ( Status.getDescFromCode(j.getStatus()).equals(jobStatusFilter) )
                            filterList.add(j);


                } else {


                    // on group named is filtered
                    for (Group g : gm.getGroups())
                        if (g.getGroupName().equals(groupNameFilter))
                            for (Job j : jobs)
                                if (Status.getDescFromCode(j.getStatus()).equals(jobStatusFilter))
                                    for (Member m : g.getMemberList())
                                        if (j.getJobName().equals(m.getJobName()))
                                            filterList.add(j);


                }


            }




            ArrayList<String> groupNames = new ArrayList<>();

            int numOfElements = 6;

            Object[][] data = new Object[filterList.size()][numOfElements];

            for (int i = 0; i < filterList.size(); i++) {

                for (int x = 0; x < numOfElements; x++) {

                    switch (x) {
                        case 0:
                            data[i][x] = filterList.get(i).getJobName();
                            break;
                        case 1:
                            data[i][x] =  Status.getDescFromCode(filterList.get(i).getStatus());
                            break;
                        case 2:
                            data[i][x] = filterList.get(i).getRunTime();
                            break;
                        case 3:
                            Long jobId = filterList.get(i).getJobId();
                            if (jobId == 0)
                                data[i][x] = null;
                            else
                                data[i][x] = jobId;
                            break;
                        case 4:
                            data[i][x] = filterList.get(i).getGroupId();
                            break;
                        case 5:
                            data[i][x] = filterList.get(i).getCategory();
                            break;

                    }

                } // end looping on job attributes

            } // end looping on jobs



            return data;

        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, null, null, null));
            return null;
        }


    }









    public void setGroupRun(String jobName, Long groupId, String groupName, int groupOrder) {


        for (Job j : jobs)
            if (j.getJobName().equals(jobName))
                j.setGroupRun(groupId, groupName, groupOrder);


    }




    //----------------------------------------------------
    // get job details

    public void getJobSummary(String jobName) {

        try {


            for (Job j : jobs)
                if (j.getJobName().equals(jobName)) {

                    // start the job
                    Runnable r = new JobSummary(j);
                    Thread th = new Thread(r);
                    th.run();


                } // end on finding job


        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, null, null, null));
        }



    }





    public void getJobActivity(String jobName) {

        for (Job j : jobs)
            if (j.getJobName().equals(jobName))
                j.getJobActivity();


    }







    public Job getJob(String jobName) {

        Job out = null;

        for(Job j : jobs)
            if(j.getJobName().equals(jobName))
                out = j;

        return out;

    }



    //-----------------------------------------------
    // choose a name of a job to use.  Make this overloaded to handle getting



    public String chooseJob(String type, boolean hasJobId) {


        try {



            logger.fine("Choosing job");


            ArrayList<String> jobList = new ArrayList<>();

            if (type.equals(Status.All.getStatusDesc())) {

                for (Job j : jobs)
                    if(j.getJobId() != 0L && hasJobId)
                        jobList.add(j.getJobName());
                    else
                        jobList.add(j.getJobName());

            } else {

                for (Job j : jobs)
                    if (Status.getCodeFromDesc(type) == j.getStatus())
                        if (j.getJobId() != 0L && hasJobId)
                            jobList.add(j.getJobName());
                        else
                            jobList.add(j.getJobName());

            }

            String selectJob = GetInput.GetChoice("Job", "Choose Job", jobList);

            logger.info("Chose job: " + selectJob);

            return selectJob;

        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, null, null, null));
            return null;
        }

    }







    public String chooseComponent(String jobName, String type) {


        ArrayList<String> comps = null;


        try {

            for (Job j : jobs)
                if (j.getJobName().equals(jobName)) {

                    comps = new ArrayList<>();

                    if(type.equals(Status.All.getStatusDesc())) {

                        for (Component c : j.getSteps())
                            comps.add(c.getComponentId());

                    } else {

                        for (Component c : j.getSteps())
                            if (Status.getCodeFromDesc(type) == c.getStatus())
                                comps.add(c.getComponentId());

                    }


                }


            String selectComp = GetInput.GetChoice("Component", "Choose Component", comps);

            logger.info("Choose component: " + selectComp);

            return selectComp;

        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, null, null, null));
            return null;
        }









    }







    public void startStopComponent(String jobName, String componentId, String action) {


        for (Job j: jobs) {

            if (j.getJobName().equals(jobName)) {

                Job curJob = j;

                for(Component c : curJob.getSteps() ) {


                    if(c.getComponentId().equals(componentId)) {

                        ArrayList<String> comp = new ArrayList<>();
                        comp.add(componentId);

                        if(action.equals(stopKW)) {
                            j.stopComponents(comp);
                        } else if (action.equals(startKW)){
                            j.startComponents(comp);

                        }


                    } // end component

                } // end looping on components

            } // end job

        } // end looping on jobs






    }






    public void buildJobNodes(DefaultMutableTreeNode top, String topJobNodeName) {

        DefaultMutableTreeNode jobsTop = new DefaultMutableTreeNode(topJobNodeName);
        top.add(jobsTop);


        for (Job j : jobs) {

            DefaultMutableTreeNode job = new DefaultMutableTreeNode(j.getJobName());
            jobsTop.add(job);

            for (Component c : j.getSteps()) {

                DefaultMutableTreeNode comp = new DefaultMutableTreeNode(c.getClass().getSimpleName());
                job.add(comp);

                comp.add(new DefaultMutableTreeNode("Order: " + c.getOrder()));
                comp.add(new DefaultMutableTreeNode("Prefix: " + c.getPrefix()));
                comp.add(new DefaultMutableTreeNode("RunStyle: " + c.getRunStyle()));



            }


        }


    }




    public void loadAllJobs() {

        try {

            logger.info("Loading all jobs");

            File jobsLocation = new File(fileProps.getProperty(Main.fpJobFolderKW));
            FilenameFilter filter = (File dir, String name) -> name.toLowerCase().endsWith("json");
            File[] files = jobsLocation.listFiles(filter);

            for (File f : files ) {
                loadJob(f, true);
            }

            logger.info("Loaded all jobs");

        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, null, null, null));
        }

    }





    public boolean validateJobName(String jobName) {

        boolean matchFlag = false;
        for(Job j : jobs)
            if(j.getJobName().equals(jobName))
                matchFlag = true;


        return matchFlag;

    }




    public Integer getJobStatus(String jobName) {

        Integer jobStatus = null;

        for (Job j : jobs)
            if (j.getJobName().equals(jobName))
                jobStatus = j.getStatus();


        return jobStatus;

    }





}
