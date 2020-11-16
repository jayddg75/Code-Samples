package com.ELTTool;

import com.sun.rowset.CachedRowSetImpl;
import org.knowm.xchart.XChartPanel;
import org.knowm.xchart.XYChart;
import org.knowm.xchart.XYChartBuilder;
import org.knowm.xchart.XYSeries;
import javax.sql.rowset.CachedRowSet;
import javax.swing.*;
import java.awt.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.List;
import java.util.logging.Logger;

/**
 *This generates the graph for a job
 */
public class GraphJob extends JPanel {




    private static Logger logger = Logger.getLogger(GraphJob.class.getName());


    private final String rabKeyword = "RabbitClient";
    private final String ftlKeyword = "FileToLanding";
    private final String speKeyword = "SchedSPExecute";
    private final String sqpKeyword = "SQLPull";
    private final String apiKeyword = "APICall";


    private final String statusKy = "status";
    private final String recordCountKy = "recordCount";
    private final String fileCountKy = "fileCount";
    private final String copyIntoKy = "copyInto";
    private final String spCheckKy = "spCheck";
    private final String spCallKy = "spCall";
    private final String filePutKy = "filePut";

    private final String xStart = "x:";
    private final String yStart = "y:";






    private ArrayList<String> components;
    private String execStmt;
    private DBConnection dbConn;





    public GraphJob(DBConnection dbConn, String execStmt, ArrayList<String> components) {


        try {

            ArrayList<String> rawRunList = new ArrayList<>();
            ArrayList<String> runList = new ArrayList<>();


            // put together the list of metrics that the job uses ...

            for (String c : components) {

                switch (c) {

                    case ftlKeyword:
                        rawRunList.add(copyIntoKy);
                        rawRunList.add(filePutKy);
                        break;
                    case rabKeyword:
                    case sqpKeyword:
                    case apiKeyword:
                        rawRunList.add(recordCountKy);
                        rawRunList.add(fileCountKy);
                        break;
                    case speKeyword:
                        rawRunList.add(spCallKy);
                        rawRunList.add(spCheckKy);
                        break;

                }

            }


            // get the distinct list of metrics
            for (String rr : rawRunList)
                if (!runList.contains(rr))
                    runList.add(rr);

            // add the statuses
            runList.add(statusKy);







            //--------------------------------------------------------------------
            // pull the data based on what the job

            HashMap<String, Object> data = new HashMap<>();
            HashMap<String, Object> statusData = new HashMap<>();

            for (String r : runList) {


                ResultSet rs = dbConn.executeQuery(execStmt.replace("${metric}", r), true);
                CachedRowSet crs = new CachedRowSetImpl();
                crs.populate(rs);
                logger.info("Job metric: " + r + " | RowCount: " + crs.size());

                if (crs.size() > 0) {


                    // for the status chart each component gets it own series
                    if (r.equals(statusKy)) {


                        while (crs.next()) {

                            String componentName = crs.getString(1);
                            String xCompData = xStart + componentName;
                            String yCompData = yStart + componentName;


                            // if the hash map already holds the data just add it to the ones in there
                            if (statusData.containsKey(xCompData)) {

                                ((ArrayList<Integer>) statusData.get(yCompData)).add(crs.getInt(3));
                                ((ArrayList<Date>) statusData.get(xCompData)).add(crs.getDate(4));

                            } else {


                                // else create a new one and add it to the hash map
                                ArrayList<Integer> yList = new ArrayList<>();
                                ArrayList<Date> xList = new ArrayList<>();

                                yList.add(crs.getInt(3));
                                xList.add(crs.getDate(4));

                                statusData.put(xCompData, xList);
                                statusData.put(yCompData, yList);


                            }


                        }


                    } else {


                        // all the other charts just get one series
                        ArrayList<Integer> yList = new ArrayList<>();
                        ArrayList<Date> xList = new ArrayList<>();


                        while (crs.next()) {

                            yList.add(crs.getInt(3));
                            xList.add(crs.getDate(4));

                        }

                        data.put("x:" + r, xList);
                        data.put("y:" + r, yList);


                    } // end if-else block


                } // end checking for results

            }   // end looping through list









            //--------------------------------------------------------------------------------------
            // put the charts together


            int defWidth = 800;
            int defHeight = 300;



            ArrayList<XYChart> charts = new ArrayList<>();

            XYChart statusChart = null;

            //-----------------------------------------------
            // the status chart ...

            if (statusData.size() > 0) {

                statusChart = new XYChartBuilder().width(defWidth).height(defHeight).title("Component Statuses").xAxisTitle("Time CST").yAxisTitle("Status").build();

                for (String s : components) {

                    logger.info("Status: " + s);

                    if (statusData.containsKey(xStart + s)) {

                        logger.info("Adding series for: " + s);

                        // get the x and y data points for the component ...
                        List<Date> xData = (List<Date>) statusData.get(xStart + s);
                        List<Integer> yData = (List<Integer>) statusData.get(yStart + s);
                        statusChart.addSeries(s, xData, yData);
                        statusChart.getStyler().setDefaultSeriesRenderStyle(XYSeries.XYSeriesRenderStyle.Scatter);

                    }



                } // end looping on components

                statusChart.getStyler().setTimezone(TimeZone.getTimeZone("America/Chicago"));
                statusChart.getStyler().setDatePattern("HH:mm:ss");
                charts.add(statusChart);

            } // end checking for any component data












            for (String r : runList) {


                if(data.containsKey(xStart + r)) {

                    logger.info("Adding series for : " + r);

                    XYChart chart = new XYChartBuilder().width(defWidth).height(defHeight).title(r).xAxisTitle("Time CST").yAxisTitle("Counts").build();
                    chart.addSeries(r, (List<Date>) data.get(xStart + r), (List<Integer>) data.get(yStart + r) );
                    chart.getStyler().setDefaultSeriesRenderStyle(XYSeries.XYSeriesRenderStyle.Scatter);
                    chart.getStyler().setTimezone(TimeZone.getTimeZone("America/Chicago"));
                    chart.getStyler().setDatePattern("HH:mm:ss");
                    charts.add(chart);


                }

            }





            //-----------------------------------------------------------------
            // put the charts and panels together, if there is any data that was pulled



            logger.info("Charts to produce: " + charts.size());

            if (charts.size() == 0) {

                NotificationBox.displayNotification("There is no data to display");

            } else {

                ArrayList<JPanel> jpList = new ArrayList<>();
                XChartPanel status = new XChartPanel(statusChart);
                JPanel jp = null;

                int a = 0;

                for (int i = 0; i < charts.size(); i++) {

                    a++;
                    logger.info("i: " + i + " | a: " + a);

                    if (a == 1) {


                        jp = new JPanel();
                        XChartPanel cp = new XChartPanel(charts.get(i));
                        jp.add(status, BorderLayout.NORTH);
                        a++;

                        if (i == charts.size() - 1)
                            jpList.add(jp);

                    } else {

                        XChartPanel cp = new XChartPanel(charts.get(i));
                        jp.add(cp, BorderLayout.SOUTH);
                        a = 0;
                        jpList.add(jp);

                    }






                }



                setLayout(new BoxLayout(this, BoxLayout.Y_AXIS));

                // add to the final panel
                for (JPanel xc : jpList)
                    add(xc);


            }


        } catch (SQLException e) {
            logger.severe(LoggingUtils.getErrorEntry(e, null, null, null));
        }





    }








}
