package com.ELTTool;


import org.bson.Document;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Logger;



/**
 *The writes all metrics to the metrics table and is a propertyChangeListener
 */
public class MetricsCollector implements PropertyChangeListener {



    private static Logger logger = Logger.getLogger(MetricsCollector.class.getName());


    private ConnectionManager cm;
    private DBConnection dbConn;
    private DBHelper dh;

    private Mongo m;

    private String dbRef;
    private Connection.DBTypes dbType;

    private String schema;

    SimpleDateFormat rddf;




    public MetricsCollector (String dbRef, Connection.DBTypes dbType, ConnectionManager cm, DBHelper dh, String schema) {

        this.dbRef = dbRef;
        this.dbType = dbType;
        this.cm = cm;
        this.dh = dh;
        this.schema = schema;

        rddf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

    }



    public void setDBConnection() {

        try {

            m = cm.getMongoConnection("eltTest");
            m.startConnection(0, 0L, 0L);

            //this.dbConn = cm.getDBConnection("dev", Connection.DBTypes.Snowflake, MetricsCollector.class.getName(), MetricsCollector.class.getName(), null, null);
            //dbConn.startConnection(60, null, null);


        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, null, null, null));
        }

    }




    @Override
    public void propertyChange(PropertyChangeEvent pce) {

        try {

            String ts = rddf.format(new Date()) + "Z";

            String[] metricIds = pce.getPropertyName().split("\\|");
            //String metricValue = String.valueOf(pce.getNewValue());


            Document d = new Document();

            // put the metric value
            d.append("metricValue", pce.getNewValue());
            d.append("metricTimestamp", new Date().getTime());

            for (int i = 0; i < metricIds.length; i++ ){

                switch (i) {

                    case 0:
                        d.append("jobName", (String) metricIds[i]);
                        break;
                    case 1:
                        if (!metricIds[i].equals("null"))
                            d.append("jobId", Long.valueOf(metricIds[i]));
                        break;
                    case 2:
                        if (!metricIds[i].equals("null"))
                            d.append("groupName", (String) metricIds[i]);
                        break;
                    case 3:
                        if (!metricIds[i].equals("null"))
                            d.append("groupId", Long.valueOf(metricIds[i]));
                        break;
                    case 4:
                        if (!metricIds[i].equals("null"))
                            d.append("component", (String) metricIds[i]);
                        break;
                    case 5:
                        d.append("metricName", (String) metricIds[i]);
                        break;
                    case 6:
                        if (!metricIds[i].equals("null"))
                            d.append("prefix", (String) metricIds[i]);
                        break;
                    case 7:
                        if (!metricIds[i].equals("null"))
                            d.append("order", Integer.valueOf(metricIds[i]));
                        break;

                }


            }

            //logger.info(d.toString());

            m.insertDocument(d, "Metrics");
            //((Snowflake) dbConn).executeStatement(insertStmt, true);


        } catch (Exception e ) {
            logger.severe(LoggingUtils.getErrorEntry(e, null, null, null));
        }


            
    }







}
