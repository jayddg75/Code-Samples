package com.ELTTool;


import javax.swing.*;
import java.awt.*;


/**
 * This is the container for the job summary to provide on place to get it
 */
public class JobSummary implements Runnable {


    private JFrame summaryFrame;
    private Job job;
    private JobSummaryTable jdt;



    public JobSummary(Job j) {
        this.job = j;

    }



    @Override
    public void run() {

        int width = 800;
        int height = 500;

        Color panelBckColor = new Color(100, 120, 120, 150);

        summaryFrame = new JFrame();
        summaryFrame.setSize(width, height);
        summaryFrame.setTitle("Summary for job: " + job.getJobName());


        JPanel summaryPanel = new JPanel();
        summaryPanel.setBackground(panelBckColor);

        jdt = new JobSummaryTable(job);
        JScrollPane scrollPane = new JScrollPane(jdt.getTable());
        summaryPanel.add(scrollPane);

        jdt.setTableSize(width - 30, height - 5);

        summaryFrame.getContentPane().add(summaryPanel);
        summaryFrame.addWindowListener(jdt);

        summaryFrame.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
        summaryFrame.setLocationRelativeTo(null);
        summaryFrame.setVisible(true);

    }
}
