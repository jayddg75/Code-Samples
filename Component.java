package com.ELTTool;


import org.json.*;

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

/**
 *This is the super class for all components
 */
public abstract class Component {

    public AtomicBoolean killJob;
    public ArrayList<File> compPassList;
    public String runStyle;
    public String prefix;

    public Long groupId;
    public String groupName;

    public AtomicInteger status;


    public String jobName;
    public int order;

    public String context;

    public static final String stringType = "s";
    public static final String intType = "i";
    public static final String booleanType = "b";

    public PropertyChangeSupport support;

    public StandardObjects so;

    private static Logger logger = Logger.getLogger(Component.class.getName());

    public AtomicLong startTime;
    public AtomicLong stopTime;

    public Long jobId;







    public ArrayList<File> getCompPassList() { return compPassList; }

    public void setCompPassList(ArrayList<File> compPassList) {

        this.compPassList = null; // null it out to make sure a fresh one is put everytime
        this.compPassList = compPassList;

    }

    public String getRunStyle() { return  runStyle; }

    public String getPrefix() { return prefix; }

    public int getStatus() { return status.get(); }

    public int getOrder() { return  order; }

    public void setStatus(int newStatus) {

        support.firePropertyChange(getPropertyChangeMetricId(Metrics.status.toString()), this.status.get(), (long) newStatus);
        this.status.set(newStatus);

    }

    public String getComponentId() { return order + "-" + this.getClass().getSimpleName(); }






    public abstract void runComponent();

    public abstract boolean loadComponent(JSONObject runParms, String context);

    public abstract void closeComponent();

    public abstract ArrayList<String> reportDetails();



    // use this to kill a component if an error occurs.  Once the status is set to -1, the job will start to stop by closing all components
    public void killComponent() {

        // don't try to kill the component again if it is already trying to
        if (status.get() != Status.Error.getStatusCode()) {

            logger.warning(LoggingUtils.getLogEntry("Killing component: " + Component.super.getClass().getSimpleName(), jobName, jobId, groupId));
            setStatus(Status.Error.getStatusCode());
            killJob.set(true);
            stopComponent();

        }

    }



    // use this to stop a component, this will make sure process if followed
    public void stopComponent() {

        logger.warning(LoggingUtils.getLogEntry("Stopping component: " + Component.super.getClass().getSimpleName(), jobName, jobId, groupId));
        closeComponent();
        stopTime.set(new Date().getTime());
        setStatus(Status.Stopped.getStatusCode());

    }


    public void resetComponent() {

        logger.info(LoggingUtils.getLogEntry("Reseting component for fresh run: " + Component.super.getClass().getSimpleName(), jobName, jobId, groupId));
        status.set(Status.Stopped.getStatusCode());
        killJob.set(false);

    }


    public void completeComponent() {
        logger.warning(LoggingUtils.getLogEntry("Completing component: " + Component.super.getClass().getSimpleName(), jobName, jobId, groupId));
        closeComponent();
        stopTime.set(new Date().getTime());
        setStatus(Status.Completed.getStatusCode());
    }


    public void setToInitialStatus() {
        status = new AtomicInteger(Status.Stopped.getStatusCode());
        killJob = new AtomicBoolean(false);
    }


    public void preRunSetup(PropertyChangeListener pcl, Long jobId, Long groupId, String groupName) {

        this.jobId = jobId;
        this.groupId = groupId;
        this.groupName = groupName;

        support = new PropertyChangeSupport(this);
        support.addPropertyChangeListener(pcl);

        setStatus(Status.Starting.getStatusCode());
        killJob.set(false);
        startTime = new AtomicLong(new Date().getTime());
        stopTime = new AtomicLong(-1);

    }



    public void setStandard(String prefix, String jobName, String runStyle, int order) {

        this.prefix = prefix;
        this.jobName = jobName;
        this.runStyle = runStyle;
        so = new StandardObjects();
        this.order = order;

    }





    public String getRunTime() {

        Long runTime = 0L;

        if (status.get() != Status.Stopped.getStatusCode()) {

            if (stopTime.get() == -1)
                runTime = (new Date().getTime()) - startTime.get();
            else
                runTime = stopTime.get() - startTime.get();

        }

        return TimeConvert.LongToTime(runTime);


    }






    // check for nulls and throw and error if you find one.  Need to validate that all the parameters were passed in that
    // should have been.  Use this for any parameters that needs to be passed in.
    public Object getJSONValue(JSONObject jo, String keyName, String type, String parmName) {

        // s == string, i == int, b = boolean

        String checkType = type.toLowerCase();

        Object out = null;

        if (jo.getJSONObject(parmName).isNull(keyName)) {

            String message = parmName + " was not set, has a null value, stopping job";
            NotificationBox.displayNotification(message);

        } else {

            switch (checkType) {

                case stringType:
                    out = jo.getJSONObject(parmName).getString(keyName);
                    break;
                case intType:
                    out = jo.getJSONObject(parmName).getInt(keyName);
                    break;
                case booleanType:
                    out = jo.getJSONObject(parmName).getBoolean(keyName);
                    break;

            }

        }


        return out;


    }



    // over load this method so you can just validate on a single key, not a pair lookup

    public Object getJSONValue(JSONObject jo, String keyName, String type) {

        // s == string, i == int, b = boolean

        String checkType = type.toLowerCase();

        Object out = null;

        if (jo.isNull(keyName)) {

            String message = keyName + " was not set, has a null value, stopping job";
            NotificationBox.displayNotification(message);

        } else {

            switch (checkType) {

                case stringType:
                    out = jo.getString(keyName);
                    break;
                case intType:
                    out = jo.getInt(keyName);
                    break;
                case booleanType:
                    out = jo.getBoolean(keyName);
                    break;

            }

        }


        return out;


    }



    public String getPropertyChangeMetricId(String metricLabel) {

        return jobName + "|" + jobId + "|" + groupName + "|" + groupId + "|" + Component.super.getClass().getSimpleName() + "|" + metricLabel + "|" + prefix + "|" + order;

    }



    public ArrayList<String> allCompReportDetails() {

        ArrayList<String> a = new ArrayList<>();

        addDetailString("Component", this.getClass().getSimpleName(), a);
        addDetailString("jobname", jobName, a);
        addDetailString("prefix", prefix, a);
        addDetailString("order", order, a);
        addDetailString("runStyle", runStyle, a);
        addDetailString("groupId", groupId, a);
        addDetailString("groupName", groupName, a);
        addDetailString("status", status.get(), a);
        addDetailString("context", context, a);
        addDetailString("jobId", prefix, a);

        return a;

    }



    public void addDetailString(String name, Object desc, ArrayList<String> al) {
       al.add(name + " : " + desc);
    }



}
