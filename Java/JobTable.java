package com.ELTTool;


import javax.swing.*;
import javax.swing.table.TableColumnModel;
import java.awt.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Logger;



/**
 * This is table implementation for the job table
 */
public class JobTable extends Table {



    private static Logger logger = Logger.getLogger(JobTable.class.getName());

    private JobManager jm;
    private JComboBox<String> statusFilter;
    private JComboBox<String> groupNameFilter;
    private JLabel l;
    private SimpleDateFormat curTS;


    public JobTable(JobManager jm, JComboBox<String> statusFilter, JComboBox<String> groupNameFilter, JLabel l) {
        this.jm = jm;
        this.statusFilter = statusFilter;
        this.groupNameFilter = groupNameFilter;
        this.l = l;
        curTS = new SimpleDateFormat("HH:mm:ss");
    }




    @Override
    public Object[][] getData() {
        l.setText("GMT: " + curTS.format(new Date()));
        return jm.listJobs( (String) statusFilter.getSelectedItem(), (String) groupNameFilter.getSelectedItem() );
    }



    @Override
    public String[] getHeaders() {
        return new String[] {"Name",  "Status", "RunTime", "JobId", "GroupId", "Category"};
    }



    @Override
    public void setColumnWidths() {

        TableColumnModel cm = table.getColumnModel();
        cm.getColumn(0).setPreferredWidth(300);
        cm.getColumn(1).setPreferredWidth(100);
        cm.getColumn(2).setPreferredWidth(100);
        cm.getColumn(3).setPreferredWidth(150);
        cm.getColumn(4).setPreferredWidth(150);
        cm.getColumn(5).setPreferredWidth(200);

    }

    @Override
    public void setTableSize(int width, int height) {

        table.setPreferredScrollableViewportSize(new Dimension(width, height));

    }


}
