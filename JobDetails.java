package com.ELTTool;

import javax.swing.*;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Logger;


/**
 *This get the job details
 */

public class JobDetails implements Runnable {


    private static Logger logger = Logger.getLogger(JobDetails.class.getName());
    private Job j;


    public JobDetails(Job j) {
        this.j = j;
    }



    @Override
    public void run() {


        try {


            JFrame js = new JFrame();
            js.setTitle("Job details for: " + j.getJobName() + "  -  jobId: " + j.getJobId());
            JTextArea ja = new JTextArea(j.getJobDetails());
            JScrollPane scrollPane = new JScrollPane(ja);
            js.getContentPane().add(scrollPane);

            js.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
            js.setSize(700,600);
            js.setLocationRelativeTo(null);
            js.setVisible(true);


        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, j.getJobName(), j.getJobId(), j.getGroupId()));
        }





    }




}
