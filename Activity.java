package com.ELTTool;



import com.mongodb.client.FindIterable;
import org.bson.Document;

import javax.swing.*;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.logging.Logger;


/**
 * This puts together the job/group activity display
 */
public class Activity implements Runnable {



    private static Logger logger = Logger.getLogger(Activity.class.getName());

    private ConnectionManager cm;
    private DBHelper dh;
    private Long jobId;
    private String jobName;
    private Long groupId;
    private String mongoRef;



    public Activity(DBHelper dh, Long jobId, String jobName, Long groupId, ConnectionManager cm, String mongoRef) {
        this.dh = dh;
        this.jobId = jobId;
        this.jobName = jobName;
        this.groupId = groupId;
        this.cm = cm;
        this.mongoRef = mongoRef;
    }



    @Override
    public void run() {

        try {


            //ResultSet rs = dh.getJobSummary(jobId);
            Mongo m = cm.getMongoConnection(mongoRef);
            m.startConnection(0, jobId, groupId);
            FindIterable fi = m.getActivity(jobId);
            Iterator rs = fi.iterator();
            StringBuilder sb = new StringBuilder();

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");


            while(rs.hasNext()) {

                Document d = (Document) rs.next();

                String ts =   sdf.format(new Date(d.getLong("metricTimestamp")));
                String comp = d.getString("component");
                String mName = d.getString("metricName");
                String value = String.valueOf(d.get("metricValue"));
                String order = String.valueOf(d.getInteger("order"));
                String out = ts + "     " + order + "   " + comp + " : " + mName + " = " + value;

                sb.append(out).append(System.lineSeparator());



            }


            JFrame js = new JFrame();
            js.setTitle("Job activity for: " + jobName + "  -  jobId: " + jobId);
            JTextArea ja = new JTextArea(sb.toString());
            JScrollPane scrollPane = new JScrollPane(ja);
            js.getContentPane().add(scrollPane);

            js.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
            js.setSize(600,500);
            js.setLocationRelativeTo(null);
            js.setVisible(true);



        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, jobName, jobId, groupId));
        }

    }






}
