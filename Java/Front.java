package com.ELTTool;




import javax.swing.*;
import java.awt.*;
import java.awt.event.*;
import java.io.File;
import java.util.*;
import java.util.logging.Logger;




/**
 *This is considered a pivotal class which is called directly from main.  This is responsible for app startup and bring up the GUI and handles all the Swing action listeners
 * <br><br>
 *Config file properties:<br>
 *<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;backendDBRef = the backend connection reference connection as defined in the connections file<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;backendDBType = the backend connection type<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;backendSchema = the backend schema used to fill in for statements and defines where all the base, landing, and audit table will be created<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;runTableChecks = true or false, run the table checks at the start of the job for each prefix defined on the job<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;runBaseTableChecks = true or flase, run the base table checks on ELT Tool startup<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;individualStartComps = true or false, ask at the start of the job to run an individual component.  False will just run all; more for dev<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;context = the context to run in, passed to all jobs and used on all parameters to get the right value for the environment in which it is ran<br>
 */
public class Front {


    private static Logger logger = Logger.getLogger(Front.class.getName());


    private JobManager jm;
    private ConnectionManager cm;
    private DBHelper dh;
    private MetricsCollector mc;
    private GroupManager gm;


    private Properties fileProps;


    private String backendDBRef;
    private Connection.DBTypes backendDBType;
    private String backendSchema;
    private String mongoRef;
    private boolean individualStartComps;
    private boolean runTableChecks;
    private boolean runBaseTableChecks;

    private String context;


    private JFrame mainFrame;
    private JComboBox<String> statusCB;
    private JComboBox<String> groupCB;

    private Tree tr;






    public Front(Properties configProps, Properties fileProps) {


        try {


            this.fileProps = fileProps;
            this.backendDBRef = configProps.getProperty("backendDBRef");
            this.backendDBType = Connection.DBTypes.valueOf(configProps.getProperty("backendDBType"));
            this.backendSchema = configProps.getProperty("backendSchema");
            this.individualStartComps = Boolean.parseBoolean(configProps.getProperty("individualStartComps"));
            this.runTableChecks = Boolean.parseBoolean(configProps.getProperty("runTableChecks"));
            this.runBaseTableChecks = Boolean.parseBoolean(configProps.getProperty("runBaseTableChecks"));
            this.mongoRef = configProps.getProperty("mongoRef");


            //-------------------------------------------------------------------
            // pull in the context set on the config file and validate it

            String[] contexts = {"dev", "qa", "stage", "prod"};
            String cont = configProps.getProperty("context");
            boolean contextMatch = false;
            for (String s : contexts)
                if (cont.equals(s))
                    contextMatch = true;

            if (!contextMatch)
                throw new Exception("Invalid context passed in: " + cont);
            else
                this.context = cont;



        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, null, null, null));
            NotificationBox.displayNotification(e.getMessage());
            System.exit(-1);
        }


    }









    public String getContext() {
        return context;
    }





    public void start() {


        try {



            //----------------------------------------------------------------------------------
            //----------------------------------------------------------------------------------
            //----------------------------------------------------------------------------------
            //----------------------------------------------------------------------------------
            // start up central classes



            logger.info("starting ELTTool");

            // set timezone to GMT
            TimeZone utcTimeZone = TimeZone.getTimeZone("UTC");
            TimeZone.setDefault(utcTimeZone);

            logger.info("Set time zone to GMT");

            // start the connection manager
            cm = new ConnectionManager(fileProps);
            cm.loadConnections();

            logger.info("Started the Connection Manager");




            dh = new DBHelper(fileProps, backendSchema, backendDBRef, backendDBType, cm);

            if(runBaseTableChecks)
                dh.tableCheckRun("", backendSchema, true);

            logger.info("Started the DB Helper");



            mc = new MetricsCollector(backendDBRef, backendDBType, cm, dh, backendSchema);
            mc.setDBConnection();

            logger.info("Started the Metrics Collector");


            jm = new JobManager(fileProps, cm, dh, mc, backendSchema, backendDBRef, backendDBType, runTableChecks, individualStartComps, groupCB, context, mongoRef);
            jm.loadAllJobs();

            logger.info("Started the Job Manager");



            gm = new GroupManager(fileProps, jm);
            gm.loadAllGroups();

            jm.setGroupManager(gm);

            logger.info("Started the group manager");











            //----------------------------------------------------------------------------------
            //----------------------------------------------------------------------------------
            //----------------------------------------------------------------------------------
            //----------------------------------------------------------------------------------
            // start up the main frame gui



            int width = 1000;
            int height = 700;


            Color panelBckColor = new Color(50, 120, 120, 150);

            mainFrame = new JFrame();
            mainFrame.setTitle("ELT Tool");

            JPanel explorerPanel = new JPanel();
            explorerPanel.setBorder(BorderFactory.createLineBorder(Color.gray));

            JPanel filterPanel = new JPanel(new FlowLayout(FlowLayout.CENTER));
            filterPanel.setBackground(panelBckColor);

            JPanel tablePanel = new JPanel();
            tablePanel.setBackground(panelBckColor);
            tablePanel.setBorder(BorderFactory.createLineBorder(Color.gray));

            JPanel statusPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
            statusPanel.setBackground(panelBckColor);



            filterPanel.setPreferredSize(new Dimension(width, 35));
            statusPanel.setPreferredSize(new Dimension(width, 35));
            explorerPanel.setPreferredSize(new Dimension( (int) (width * .3), 500));
            tablePanel.setPreferredSize(new Dimension( (int) (width * .7), 500));


            mainFrame.getContentPane().add(BorderLayout.NORTH, filterPanel);
            mainFrame.getContentPane().add(BorderLayout.EAST, tablePanel);
            mainFrame.getContentPane().add(BorderLayout.SOUTH, statusPanel);
            mainFrame.getContentPane().add(BorderLayout.WEST, explorerPanel);




            //-------------------------------------------------------
            // set the tree for the explorer panel




            tr = new Tree(gm, jm, context, fileProps);
            JScrollPane treeScrollPane = new JScrollPane(tr.getTree());
            explorerPanel.add(treeScrollPane);
            treeScrollPane.setPreferredSize(new Dimension( (int) (width * .3) - 5, 495));



            //-------------------------------------------------------
            // set the status panel to show the context

            statusPanel.setBorder(BorderFactory.createLineBorder(Color.gray));

            JLabel contextLabel = new JLabel("Context: " + context);
            contextLabel.setForeground(Color.WHITE);

            JLabel ctGMTLabel = new JLabel();
            ctGMTLabel.setForeground(Color.WHITE);


            statusPanel.add(contextLabel);
            statusPanel.add(new JSeparator(SwingConstants.VERTICAL));
            statusPanel.add(ctGMTLabel);

            //-------------------------------------------------------
            // filter panel for the jobs

            Dimension d = new Dimension(100, 25);
            UIManager.setLookAndFeel("javax.swing.plaf.nimbus.NimbusLookAndFeel");

            JLabel statusDBLabel = new JLabel("Job Status");
            statusDBLabel.setForeground(Color.WHITE);
            statusCB = new JComboBox(Status.getAllStatusDesc());
            statusCB.setSelectedIndex(0); // set it to all initially

            JLabel groupIdLabel = new JLabel("GroupName");
            groupIdLabel.setForeground(Color.WHITE);


            groupCB = new JComboBox<>(gm.getGroupList());
            groupCB.setSelectedIndex(0);

            groupCB.setPreferredSize(d);
            statusCB.setPreferredSize(d);

            filterPanel.add(statusDBLabel);
            filterPanel.add(statusCB);
            filterPanel.add(groupIdLabel);
            filterPanel.add(groupCB);


            //-------------------------------------------------------
            // jobs table

            JobTable jt = new JobTable(jm, statusCB, groupCB, ctGMTLabel);
            JScrollPane scrollPane = new JScrollPane(jt.getTable());
            jt.setTableSize((int) (width * .7) -5, 495);
            tablePanel.add(scrollPane);


            //---------------------------------------------------------------------------
            // Menu bars

            JMenuBar mainBar = new JMenuBar();
            String iconFolder = fileProps.getProperty(Main.fpIconsFolderKW) + "\\";



            //---------------------------------------------------------------------------
            // jobs menu


            JMenu jobMenu = new JMenu("Jobs");
            JMenuItem jobEdit = new JMenuItem("Edit", new ImageIcon( iconFolder + "icons8-edit-18.png"));
            JMenuItem jobLoad = new JMenuItem("Load", new ImageIcon( iconFolder + "icons8-plus-18.png"));
            JMenuItem jobRemove = new JMenuItem("Remove", new ImageIcon( iconFolder + "icons8-remove-18.png"));

            mainBar.add(jobMenu);
            jobMenu.add(jobEdit);
            jobMenu.add(jobLoad);
            jobMenu.add(jobRemove);


            jobEdit.addActionListener(new editJobListen());
            jobLoad.addActionListener(new loadJobListen());
            jobRemove.addActionListener(new removeJobListen());





            //---------------------------------------------------------------------------
            // groups menu

            JMenu groupsMenu = new JMenu("Groups");
            JMenuItem groupLoad = new JMenuItem("Load", new ImageIcon( iconFolder + "icons8-plus-18.png"));
            JMenuItem groupRemove = new JMenuItem("Remove", new ImageIcon( iconFolder + "icons8-remove-18.png"));

            mainBar.add(groupsMenu);
            groupsMenu.add(groupLoad);
            groupsMenu.add(groupRemove);

            groupLoad.addActionListener(new addGroupListener());



            //---------------------------------------------------------------------------
            // final main JFame settings ...


            mainFrame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
            mainFrame.setLocationRelativeTo(null);
            mainFrame.pack();
            mainFrame.setVisible(true);




        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, null, null, null));

        }


    }











    //--------------------------------------------------------------------
    //--------------------------------------------------------------------
    //--------------------------------------------------------------------
    // action listeners






    class editJobListen implements ActionListener {

        @Override
        public void actionPerformed(ActionEvent actionEvent) {

            try {

                String chosenJob = jm.chooseJob(Status.Running.getStatusDesc(), false);

                if (!(chosenJob == null)) {


                    int r = GetInput.GetYesNoAlter("Action", "Choose Action", JobManager.startKW, JobManager.stopKW);

                    String compId = null;
                    String compAction = null;

                    switch (r) {
                        case 0:  // start a component
                            compId = jm.chooseComponent(chosenJob, Status.Stopped.getStatusDesc());
                            compAction = JobManager.startKW;
                            break;
                        case 1:   // stop a component
                            compId = jm.chooseComponent(chosenJob, Status.Running.getStatusDesc());
                            compAction = JobManager.stopKW;
                            break;
                        case -1:  // they cancelled
                            //
                            break;

                    }


                    if ( !(compId == null)) {

                        logger.info(chosenJob + " | " + compId + " | " + compAction);
                        jm.startStopComponent(chosenJob, compId, compAction);

                    } else {
                      NotificationBox.displayNotification("Component not set");
                    }



                } else {
                    NotificationBox.displayNotification("Job not set");
                }

            } catch (Exception e) {
                logger.severe(LoggingUtils.getErrorEntry(e, null, null, null));
            }





        }



    }
    
    
    
    
    
    


    class loadJobListen implements ActionListener {

        @Override
        public void actionPerformed(ActionEvent actionEvent) {


            try {

                File f = new FileChooser().chooseFile(Front.class.getSimpleName(), null, null);

                if (f == null) {
                    NotificationBox.displayNotification("No file choosen");
                } else {
                    jm.loadJob(f, false);
                    tr.buildTree();
                }



            } catch (Exception e) {
                logger.severe(LoggingUtils.getErrorEntry(e, null, null, null));
            }

        }

    }




    class removeJobListen implements ActionListener {

        @Override
        public void actionPerformed(ActionEvent actionEvent) {

        }

    }



    //---------------------------------------------------------------
    // group menu listeners




    class addGroupListener implements ActionListener {


        @Override
        public void actionPerformed(ActionEvent e) {



            try {

                File f = new FileChooser().chooseFile(Front.class.getSimpleName(), null, null);

                if (f == null) {
                    NotificationBox.displayNotification("No file choosen");
                } else {
                    gm.loadGroup(f);
                    tr.buildTree();
                }



            } catch (Exception er) {
                logger.severe(LoggingUtils.getErrorEntry(er, null, null, null));
            }


        }




    }

































}
