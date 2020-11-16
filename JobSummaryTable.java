package com.ELTTool;


import javax.swing.table.TableColumnModel;
import java.awt.*;
import java.awt.event.WindowEvent;
import java.awt.event.WindowListener;
import java.util.logging.Logger;



/**
 * This is table implementation for the job details table data
 */
public class JobSummaryTable extends Table implements WindowListener {


    private static Logger logger = Logger.getLogger(JobSummaryTable.class.getName());

    private Job j;


    public JobSummaryTable(Job j) {
        this.j = j;
    }



    @Override
    public Object[][] getData() { return j.getJobSummary(); }



    @Override
    public String[] getHeaders() {
        return new String[] {"Order", "Component", "Prefix", "Status", "Style", "KillFlag", "RunTime"};
    }






    @Override
    public void setColumnWidths() {

        TableColumnModel cm = table.getColumnModel();
        cm.getColumn(0).setPreferredWidth(75);
        cm.getColumn(1).setPreferredWidth(150);
        cm.getColumn(2).setPreferredWidth(75);
        cm.getColumn(3).setPreferredWidth(100);
        cm.getColumn(4).setPreferredWidth(150);
        cm.getColumn(5).setPreferredWidth(75);
        cm.getColumn(6).setPreferredWidth(100);

    }

    @Override
    public void setTableSize(int width, int height) {

        table.setPreferredScrollableViewportSize(new Dimension(width, height));

    }


    @Override
    public void windowOpened(WindowEvent windowEvent) {

    }

    @Override
    public void windowClosing(WindowEvent windowEvent) {

    }

    @Override
    public void windowClosed(WindowEvent windowEvent) {
        try {
            sched.stopScheduler(true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void windowIconified(WindowEvent windowEvent) {

    }

    @Override
    public void windowDeiconified(WindowEvent windowEvent) {

    }

    @Override
    public void windowActivated(WindowEvent windowEvent) {

    }

    @Override
    public void windowDeactivated(WindowEvent windowEvent) {

    }




}
