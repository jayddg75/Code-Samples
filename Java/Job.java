package com.ELTTool;


import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.io.File;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
import org.json.*;


/**
 * A job is the runnable collection of one or more components and is defined in JSON file in the jobs folder and can have one or more sql files related to it
 * <br><br>
 * Job file:<br>
 * A job file is a JSON object with .json extension with the following parts
 * <ul>
 * <li>Category - A textual description of the category of the job: dimension, fact, adhoc, etc... used for grouping purposes</li>
 * <li>Description - A textual description of the purpose of the job to make job interpretation easier</li>
 * <li>Prefix - a JSON array that contains the list of prefixes used on the job.  This helps with job consistency and cuts down on coding errors</li>
 * <li>Steps - a JSON array that composes the majority of the file and contains the ordered list of components to run.  See the components for a breakdown of the required object</li>
 * </ul>
 * <br><br>
 * Typical job file will look like this ... <br><br>
 * {<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"Category": "Dimension",<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"Description": "This load the Code Dimension from the file",<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"Prefix": ["CODE"],<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"Steps": [<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"Order": 1,<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"Component": "SQLExecutor",<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"Prefix": 1,<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"RunStyle": "batch",<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"sqlFile": "CODE_L_Trunc.sql",<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"Parameters": [<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"dbConnection": {<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"type": "Snowflake",<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"statementTimeout": 600,<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"dev": "dev",<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"qa": "dev",<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"stage": null,<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"prod": null<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;}<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;},<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"sql": [<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"type": "sql",<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"order": 1,<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"fileOrder": 1<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;}<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;]<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;}<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;]<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;}<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;]<br>
 * }<br>
 */
public class Job implements Runnable {


    private static Logger logger = Logger.getLogger(Job.class.getName());


    private String jobName;
    private String category;
    private JSONArray prefix;
    private String description;

    private Long groupId;
    private String groupName;
    private int groupOrder;

    private AtomicInteger status;
    private String context;


    private ArrayList<Component> steps;
    private JSONObject jobSettings;
    private ConnectionManager cm;
    private Properties fileProps;
    private DBHelper dh;
    private MetricsCollector mc;

    private Scheduler sched;

    private String backendSchema;
    private String backendDBRef;
    private Connection.DBTypes backendDBType;
    private String mongoRef;

    private boolean runTableChecks;

    private ArrayList<File> compfileList;
    private JSONArray iterateList;

    private boolean fullRun;

    private boolean silentMode;
    private boolean individualStartComps;

    private AtomicLong jobId;
    private AtomicLong runTime;


    private PropertyChangeSupport support;





    public Job(ConnectionManager cm, Properties fileProps, JSONArray prefix, String category, String name,
               JSONObject jobSettings, DBHelper dh, MetricsCollector mc, String backendSchema, String backendDBRef,
               Connection.DBTypes backendDBType, boolean runTableChecks, boolean individualStartComps, String description,
               String context, String mongoRef) {

        try {

            steps = new ArrayList<>();
            compfileList = new ArrayList<>();

            status = new AtomicInteger(0);

            this.cm = cm;
            this.fileProps = fileProps;
            this.prefix = prefix;
            this.category = category;
            this.jobName = name;
            this.jobSettings = jobSettings;
            this.dh = dh;
            this.mc = mc;
            this.backendSchema = backendSchema;
            this.backendDBRef = backendDBRef;
            this.backendDBType = backendDBType;
            this.runTableChecks = runTableChecks;
            this.individualStartComps = individualStartComps;
            this.description = description;
            this.context = context;
            this.mongoRef = mongoRef;

            jobId = new AtomicLong(0);
            runTime = new AtomicLong(0);


        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, Job.class.getSimpleName(), jobId.get(), groupId));
        }

    }


    //-----------------------------------------------------
    // getters and setters for name and category

    public String getJobName() {
        return jobName;
    }
    public String getCategory() {
        return category;
    }
    public String getContext() { return context; }
    public int getStatus() {
        return status.get();
    }
    public JSONArray getPrefix() { return prefix; }
    public String getDescription() { return description; }

    public Long getJobId() { return jobId.get(); }

    public String getBackendSchema() { return backendSchema; }
    public String getBackendDBRef() { return backendDBRef; }

    public boolean getRunTableChecks() { return runTableChecks; }

    public ArrayList<Component> getSteps() { return steps; }

    public String getGroupName() { return groupName; }
    public int getGroupOrder() { return groupOrder; }
    public Long getGroupId() { return groupId; }

    public String getRunTime() {

        return TimeConvert.LongToTime(runTime.get());

    }






    //-----------------------------------------------------
    // functional methods






    private void changeJobStatus(int newStatus) {

        support.firePropertyChange(getPropertyChangeMetricId(Metrics.status.toString()), status.get(),  (long) newStatus );
        status.set(newStatus);

    }


    public void setPreJob(boolean silentMode, PropertyChangeListener pcl) {

        this.silentMode = silentMode;

        support = new PropertyChangeSupport(this);
        support.addPropertyChangeListener(pcl);

    }


    public void setGroupRun(Long groupId, String groupName, int groupOrder) {
        this.groupId = groupId;
        this.groupName = groupName;
        this.groupOrder = groupOrder;
    }





    public void initializeComponents() {


        try {




            JSONArray steps = jobSettings.getJSONArray("Steps");

            for (int i = 0; i < steps.length(); i++) {

                JSONObject curStep = steps.getJSONObject(i);
                int order = curStep.getInt("Order");
                int prefixIndex = curStep.getInt("Prefix");
                String runStyle = curStep.getString("RunStyle");


                if (order != i + 1) {
                    throw new Exception("Component was trying to be loaded out of order");
                }


                logger.info(LoggingUtils.getLogEntry("Initializing: " + curStep.getString("Component"), jobName, jobId.get(), groupId));

                Component c = null;

                switch (curStep.getString("Component")) {

                    case "RabbitClient":
                        c = new RabbitClient(fileProps);
                        break;
                    case "FileToLanding":
                        c = new FileToLanding(cm, dh, fileProps, backendSchema);
                        break;
                    case "SQLExecutor":
                        c = new SQLExecutor(cm, backendSchema, fileProps);
                        break;
                    case "SQLPull":
                        c = new SQLPull(cm, fileProps);
                        break;
                    case "APICall":
                        c = new APICall(fileProps);
                        break;
                    case "FileConverter":
                        c = new FileConverter(fileProps);
                        break;
                    case "ListAPIPrep":
                        c = new ListAPIPrep();
                        break;
                    case "DataCopy":
                        c = new DataCopy(cm, fileProps);
                        break;
                    default:
                        throw new Exception("Unknown component: " + curStep.getString("Component"));


                } // end switch


                c.setStandard( (String) prefix.get(prefixIndex - 1), jobName, runStyle, order);
                this.steps.add(c);




            }


            //------------------------------------------------------------------------
            // set the statuses on each component

            for (Component c : this.steps) {
                c.setToInitialStatus();
            }




            //----------------------------------------------------------
            //----------------------------------------------------------
            //----------------------------------------------------------
            //----------------------------------------------------------
            // load all the components with the requested context

            loadContextVariables();



        } catch( Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, jobName, jobId.get(), groupId));
        }


    }












    public void loadContextVariables() {

        try {


            JSONArray steps = jobSettings.getJSONArray("Steps");

            for (int i = 0; i < steps.length(); i++) {

                JSONObject curStep = steps.getJSONObject(i);

                if (curStep.getInt("Order") != i + 1 ) {
                    throw new Error("Component was trying to be loaded out of order");
                }

                this.steps.get(i).loadComponent(curStep, context);

                logger.info(LoggingUtils.getLogEntry("Loading: " + curStep.getString("Component"), jobName, jobId.get(), groupId));


            }


        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, jobName, jobId.get(), groupId));
        }



    }










    //----------------------------------------------------
    // stop the job by closing each component in order

    public void stopJob () {

        try {


            // if the job is getting shut down due to an error, send a notification
            if (status.get() == Status.Error.getStatusCode())
                NotificationBox.displayRunningNotification(jobName + " has an error and is getting stopped.  Check the log files for more detail. ");


            logger.warning(LoggingUtils.getLogEntry("-----------------------------------------------------------", jobName, jobId.get(), groupId));
            logger.warning(LoggingUtils.getLogEntry("-----------------------------------------------------------", jobName, jobId.get(), groupId));
            logger.warning(LoggingUtils.getLogEntry("Closing job: " + jobName, jobName, jobId.get(), groupId));
            logger.warning(LoggingUtils.getLogEntry("-----------------------------------------------------------", jobName, jobId.get(), groupId));
            logger.warning(LoggingUtils.getLogEntry("-----------------------------------------------------------", jobName, jobId.get(), groupId));



            // go through each component and stop any that are running
            ArrayList<String> comps = new ArrayList<>();
            for (Component c : steps)
                    comps.add(c.getComponentId());


            logger.info(LoggingUtils.getLogEntry("Shutting down components: " + comps.size(), jobName, jobId.get(), groupId));


            stopComponents(comps);

            if(!silentMode)
                getJobActivity();


            // set it to a stopped status
            changeJobStatus(Status.Stopped.getStatusCode());


            sched.stopScheduler(true);






        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, jobName, jobId.get(), groupId));
        }

    }







    //----------------------------------------------------
    // implement runnable for the thread
    // go and start each component in order

    @Override
    public void run() {

        try {



            //------------------------------------------------------------
            // start and setup for a fresh run


            // mark the start of the job
            jobId.set(new Date().getTime());
            runTime.set(0);


            // set to a starting status
            changeJobStatus(Status.Starting.getStatusCode());



            // reset the components
            for (Component c : steps)
                c.resetComponent();



            // reset the job lists
            compfileList = new ArrayList<>();
            iterateList = new JSONArray();



            // start the job details
            if (!silentMode) {

                Runnable r = new JobSummary(this);
                Thread th = new Thread(r);
                th.run();

            }


            logger.warning(LoggingUtils.getLogEntry("------------------------------------------------------------", jobName, jobId.get(), groupId));
            logger.warning(LoggingUtils.getLogEntry("------------------------------------------------------------", jobName, jobId.get(), groupId));
            logger.warning(LoggingUtils.getLogEntry("Starting job: " + jobName + " | context: " + context, jobName, jobId.get(), groupId));
            logger.warning(LoggingUtils.getLogEntry("------------------------------------------------------------", jobName, jobId.get(), groupId));
            logger.warning(LoggingUtils.getLogEntry("------------------------------------------------------------", jobName, jobId.get(), groupId));





            // if running table checks go through each prefix in the array
            if (runTableChecks) {

                logger.info(LoggingUtils.getLogEntry("Running job table checks", jobName, jobId.get(), groupId));

                DBConnection util = cm.getDBConnection(backendDBRef, backendDBType, jobName, jobName, jobId.get(), groupId);
                util.startConnection(180, jobId.get(), groupId);

                for (Object o : prefix) {
                    String p = (String) o;
                    dh.tableCheckRun(p, backendSchema, false);
                }

            }





            ArrayList<String> comps = new ArrayList<>();

            int indChoice = -99;
            if (!individualStartComps) {

                indChoice = 1;

            } else {

                indChoice =  GetInput.GetYesNoAlter("Choose", "Do you want to start one component or the entire job?", "Individual", "All" );

                if (indChoice == -1) {
                    NotificationBox.displayNotification("No choice made");
                    stopJob();
                }

            }






            if (indChoice == 0 ) {

                // if chosen to only run individual components

                ArrayList<String> checkComps = new ArrayList<>();
                for (Component c : steps) {
                    checkComps.add( c.order + "-" + c.getClass().getSimpleName());
                }


                String compChoice = GetInput.GetChoice("Choose", "Component", checkComps);

                // make sure a choice was made
                if (compChoice == null ) {
                    NotificationBox.displayNotification("No choice made");
                    stopJob();
                }


                comps.add(compChoice);

                fullRun = false;

            } else {

                // add them all to the list
                for (Component c : steps) {
                    comps.add(c.getComponentId());
                }

                fullRun = true;

            }

            logger.info(LoggingUtils.getLogEntry("batchFullRun: " + fullRun, jobName, jobId.get(), groupId));
            logger.info(LoggingUtils.getLogEntry("Starting component count:" + comps.size(), jobName, jobId.get(), groupId));
            logger.info(LoggingUtils.getLogEntry("Starting job SES", jobName, jobId.get(), groupId));

            // start the checker to see if the job needs to be killed

            sched = new Scheduler(new checkJob(), 500L, 500L, jobName, jobId.get(), groupId);
            sched.runScheduler();



            logger.info(LoggingUtils.getLogEntry("Starting components", jobName, jobId.get(), groupId));
            // pass of the list to start them up ...
            startComponents(comps);


        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, jobName, jobId.get(), groupId));
        }


    }






    public void startComponents(ArrayList<String> comps) {


        try{


            //---------------------------------------------------------------------------------------
            //---------------------------------------------------------------------------------------
            //---------------------------------------------------------------------------------------
            // run the job by running the components in order






                // go through each component name loaded in the run
                for (String curComponentId : comps) {

                    logger.info(LoggingUtils.getLogEntry("Requested component: " + curComponentId, jobName, jobId.get(), groupId));

                    String[] sp = curComponentId.split("-");
                    String component = sp[1];


                    // go through each loaded component
                    for (Component c : steps) {

                            String loopComponentId = c.getComponentId();

                            // start it if it is the right component
                            if ( curComponentId.equals(loopComponentId) && status.get() > Status.Stopped.getStatusCode()) {


                                logger.warning(LoggingUtils.getLogEntry("--------------------------------------------------------------------------------------------", jobName, jobId.get(), groupId));
                                logger.warning(LoggingUtils.getLogEntry("Starting Step: " + c.order + " | Component: " + c.getClass().getSimpleName() + " |  Style: " + c.getRunStyle() + " | Prefix: " + c.getPrefix(), jobName, jobId.get(), groupId));
                                logger.warning(LoggingUtils.getLogEntry("--------------------------------------------------------------------------------------------", jobName, jobId.get(), groupId));



                                // pre run setup stuff: variables, propertyChangeListener, starting status ...
                                c.preRunSetup(mc, jobId.get(), groupId, groupName);




                                //-------------------------------------------------------------
                                // any pre work that is needed before a component is run
                                // 1. FileToLanding (batch mode) - pass in the list of files to put
                                // 2. API Call (list-db mode) - pass in the JSON array for iteration
                                //       Also pass in the file list for FileToLanding for



                                if ( component.equals(FileToLanding.class.getSimpleName()) && c.getRunStyle().equals(JobManager.batchRunStyle) ) {

                                    c.setCompPassList(compfileList);

                                } else if ( component.equals(APICall.class.getSimpleName()) ) {


                                    APICall curComp = (APICall) c;

                                    // if this is a list-db run, then pass in the apiIterateList
                                    if (curComp.getMode().equals(APICall.listDBModeKeyword))
                                        curComp.setIterateList(iterateList);


                                } else if ( component.equals(ListAPIPrep.class.getSimpleName()) ) {

                                    // if this is a ListStrip then set the iterate list to be processes
                                    ListAPIPrep ls = (ListAPIPrep) c;
                                    ls.setIterateList(iterateList);

                                }





                                //-------------------------------------------------------------
                                // run the component

                                c.setStatus(Status.Running.getStatusCode());
                                c.runComponent();




                                //-------------------------------------------------------------
                                // any post work that is needed after a component has run
                                // 1. SQL Pull - pass out the list of files for FileToLanding to pick up
                                // 2. API Call - same as SQL Pull for list-db and db modes.
                                //          For list mode pass out the JSON array for iteration by next APICall component


                                if ( component.equals(SQLPull.class.getSimpleName()) ) {


                                    SQLPull spc = (SQLPull) c;

                                    if ( spc.getMode().equals(SQLPull.listModeKW)) {

                                        iterateList = new JSONArray();
                                        iterateList = spc.getIterateList();

                                    } else {

                                        compfileList = new ArrayList<>();
                                        compfileList = spc.getCompPassList();

                                    }

                                } else if (component.equals(APICall.class.getSimpleName())) {

                                    APICall curComp = (APICall) c;

                                    // if this is a list API Call run, set the apiIterateList
                                    if ( curComp.getMode().equals(APICall.listModeKeyword) ) {

                                        iterateList = new JSONArray();
                                        iterateList = curComp.getIterateList();

                                    } else {

                                        // else pass out the list of files for FileToLanding to pickup
                                        compfileList = new ArrayList<>();
                                        compfileList = curComp.getFileList();

                                    }


                                } else if (component.equals(FileConverter.class.getSimpleName())) {

                                    compfileList = new ArrayList<>();
                                    compfileList = ((FileConverter) c).getFileList();

                                }  else if ( component.equals(ListAPIPrep.class.getSimpleName())) {

                                    // if this is a ListStrip then pass the processed list back out to the job
                                    // for the next component to pick up
                                    ListAPIPrep ls = (ListAPIPrep) c;
                                    iterateList = new JSONArray();
                                    iterateList = ls.getIterateList();

                                }


                            } // end running component


                    } // end looping on components


                } // end going through the list of string(s) passed in to run




        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, jobName, jobId.get(), groupId));
            stopJob();
        }

    }







    public void stopComponents(ArrayList<String> comps) {


        try {

            for(String curComponentId : comps) {

                for(Component c: steps) {

                    if (c.getComponentId().equals(curComponentId)) {

                        // reset the kill flag
                        logger.info(LoggingUtils.getLogEntry("Checking status of: " + c.getComponentId() + " | status: " + c.getStatus(), jobName, jobId.get(), groupId));
                        c.killJob.set(false);

                         if (c.getStatus() != Status.Stopped.getStatusCode() && c.getStatus() != Status.Completed.getStatusCode() ) {

                             logger.warning(LoggingUtils.getLogEntry("--------------------------------------------------------------------------------------------", jobName, jobId.get(), groupId));
                             logger.warning(LoggingUtils.getLogEntry("Stopping Step: " + c.order + " | Component: " + c.getClass().getSimpleName() + " |  Style: " + c.getRunStyle() + " | Prefix: " + c.getPrefix(), jobName, jobId.get(), groupId));
                             logger.warning(LoggingUtils.getLogEntry("--------------------------------------------------------------------------------------------", jobName, jobId.get(), groupId));

                             c.stopComponent();

                         }



                    }

                }

            }



        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, jobName, jobId.get(), groupId));
        }




    }




    public Object[][] getJobSummary() {

        try {

            Object[][] out = new Object[steps.size()][7];

            for (int i = 0; i < steps.size(); i++ ) {

                out[i][0] = steps.get(i).order;
                out[i][1] = steps.get(i).getClass().getSimpleName();
                out[i][2] = steps.get(i).getPrefix();
                out[i][3] = Status.getDescFromCode(steps.get(i).getStatus());
                out[i][4] = steps.get(i).runStyle;
                out[i][5] = steps.get(i).killJob.get();
                out[i][6] = steps.get(i).getRunTime();


            } // end going through steps

            return out;

        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, jobName, jobId.get(), groupId));
            return null;
        }




    }



    public void getJobActivity() {

        Runnable r = new Activity(dh, jobId.get(), jobName, groupId, cm, mongoRef);
        Thread th = new Thread(r);
        th.setName(jobName);
        th.start();

    }





    public String getPropertyChangeMetricId(String metricLabel) {

        return jobName + "|" + jobId + "|" + groupName + "|" + groupId + "|" + null + "|" + metricLabel + "|" + null + "|" + null;

    }




    public String getJobDetails() {



        StringBuilder sb = new StringBuilder();

        addDetailString("jobName", jobName, sb);
        addDetailString("jobId", jobId == null ? null : jobId, sb);
        addDetailString("category", category, sb);
        addDetailString("prefixCount", prefix.length(), sb);
        addDetailString("description", description, sb);
        addDetailString("groupId", groupId == null ? null : groupId, sb);
        addDetailString("groupName", groupName == null ? null : groupName, sb);
        addDetailString("groupOrder", groupOrder== -1 ? null : groupOrder, sb);
        addDetailString("context", context, sb);
        addDetailString("stepCount", steps.size(), sb);
        addDetailString("backendSchema", backendSchema, sb);
        addDetailString("backendDBRef", backendDBRef, sb);
        addDetailString("runTableChecks", runTableChecks, sb);



        for (Component c : steps) {

            sb.append(System.lineSeparator());
            sb.append("-------------------------------------------").append(System.lineSeparator());
            sb.append(System.lineSeparator());

            for (String s : c.reportDetails())
                sb.append(s).append(System.lineSeparator());


        }


        return sb.toString();


    }




    public void addDetailString(String name, Object desc, StringBuilder sb) {
        sb.append(name).append(" : ").append(desc).append(System.lineSeparator());
    }





    class checkJob implements Runnable {


        @Override
        public void run() {



            //calculate the run time in millis
            runTime.set( (new Date().getTime()) - jobId.get() );


            // check to see if the job needs to get killed
            if (status.get() != Status.Error.getStatusCode())
                for (Component c : steps)
                    if (c.killJob.get()) {
                        logger.severe(LoggingUtils.getLogEntry("Killing job", jobName, jobId.get(), groupId));
                        changeJobStatus(Status.Error.getStatusCode());
                        stopJob();
                    }




            //-------------------------------------------------------
            // set the job status

            int componentCount = 0;
            int batchCount = 0;
            int continuousCount = 0;

            int batchCompletedCount = 0;
            int batchNotCompletedCount = 0;
            int continuousRunningCount = 0;
            int runningCount = 0;
            int stoppedCount = 0;
            int errorCount = 0;
            int startingCount = 0;



            for(Component c : steps) {

                componentCount++;
                int compStatus = c.getStatus();

                boolean batchFlag = false;
                if (c.getRunStyle().equals(JobManager.batchRunStyle)) {
                    batchFlag = true;
                    batchCount++;
                } else {
                    continuousCount++;
                }


                if (compStatus == Status.Completed.getStatusCode())
                    batchCompletedCount++;
                else if (compStatus == Status.Error.getStatusCode())
                    errorCount++;
                else if (compStatus == Status.Running.getStatusCode()) {

                    runningCount++;
                    if(batchFlag)
                        batchNotCompletedCount++;
                    else
                        continuousRunningCount++;

                } else if (compStatus == Status.Stopped.getStatusCode()) {

                    stoppedCount++;
                    if (batchFlag)
                        batchNotCompletedCount++;

                } else if (compStatus == Status.Starting.getStatusCode())
                    startingCount++;


            } // end looping through components


            //---------------------------------------------
            // if the batch components have all completed and there are no continuous ones, then mark the job stopped
            // if all the batch components have completed and there is at least one continuous one running, then
            // the jobs stays in a running status.


            //logger.info("fullRun: " + fullRun + " | batchCompletedCount: " + batchCompletedCount + " | componentCount: " + componentCount + " | status: " + status.get());

            if(status.get() == Status.Error.getStatusCode()) {

                // if there is an error and all components have stopped or have completed then mark it stopped else keep it in error
                if (stoppedCount + batchCompletedCount == componentCount)
                    changeJobStatus(Status.Stopped.getStatusCode());

            } else if (status.get() == Status.Starting.getStatusCode()) {

                // if the job is in a starting status then check to see if anything is running
                if (runningCount > 0)
                    changeJobStatus(Status.Running.getStatusCode());


            } else if (status.get() == Status.Running.getStatusCode()) {

                // if the job is in a running status then check to see if it should be marked as stopped
                if (fullRun && batchCompletedCount == componentCount)
                    stopJob();
                else if ( !fullRun && runningCount == 0)
                    stopJob();

            }

            //logger.info("Job Status: " + status.get());

        } // end run


    }














}
