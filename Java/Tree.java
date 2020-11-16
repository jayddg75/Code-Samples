package com.ELTTool;




import javax.swing.*;
import javax.swing.event.PopupMenuEvent;
import javax.swing.event.PopupMenuListener;
import javax.swing.tree.*;
import java.awt.Component;
import java.awt.event.*;
import java.io.File;
import java.util.Properties;
import java.util.logging.LogManager;
import java.util.logging.Logger;


/**
 * This class setups and maintains the job and group explorer
 */
public class Tree {


    private static Logger logger = Logger.getLogger(Tree.class.getName());


    private DefaultMutableTreeNode top;
    private JTree tree;
    private JobManager jm;
    private GroupManager gm;


    public static final String groupTopNodeKW = "Groups";
    public static final String jobsTopNodeKW = "Jobs";


    private String popupSelType;
    private String popupSelNode;

    private int lastSelectedTreeRow;

    private JPopupMenu groupPopup;
    private JPopupMenu jobPopup;
    private String context;
    private Properties fileProps;

    private boolean freezeSelection;
    private String iconFolder;







    public Tree (GroupManager gm, JobManager jm, String context, Properties fileProps) {

        top = new DefaultMutableTreeNode("All");
        tree = new JTree(top);

        this.gm = gm;
        this.jm = jm;
        this.context = context;
        this.fileProps = fileProps;

        freezeSelection = false;
        iconFolder = fileProps.getProperty(Main.fpIconsFolderKW) + "\\";

        tree.setCellRenderer(new TreeRenderer());









        tree.addMouseMotionListener(new MouseMotionAdapter() {


            @Override
            public void mouseMoved(MouseEvent e) {

                JTree tree = (JTree) e.getSource();
                int overRow = tree.getRowForLocation(e.getX(), e.getY());


                if (!freezeSelection)
                    if(overRow ==-1) {

                        tree.clearSelection();
                        lastSelectedTreeRow =-1;

                    } else if(overRow != lastSelectedTreeRow) {

                        tree.setSelectionRow(overRow);
                        lastSelectedTreeRow = overRow;

                    }

            }



        });



        tree.addMouseListener(new MouseAdapter() {



            @Override
            public void mouseReleased(MouseEvent e) {

                if (e.isPopupTrigger()) {

                    int x = e.getX();
                    int y = e.getY();
                    TreePath tp = tree.getPathForLocation(x, y);

                    if (tp != null) {

                        freezeSelection = true;
                        popupSelType = tp.getPathComponent(1).toString();
                        popupSelNode = tp.getLastPathComponent().toString();
                        //logger.info("Type: " + popupSelType + " | node: " + popupSelNode);

                        if (popupSelType.equals(jobsTopNodeKW)) {

                            if (jm.validateJobName(popupSelNode)) {
                                jobPopup.show(e.getComponent(), x, y);
                            }

                        } else if (popupSelType.equals(groupTopNodeKW)) {

                            if (gm.validateGroupName(popupSelNode))
                                groupPopup.show(e.getComponent(), x, y);

                        }

                    }

                }

            }


        });



        jobPopup = new JPopupMenu();
        groupPopup = new JPopupMenu();

        jobPopup.addPopupMenuListener(new PopupListener());
        groupPopup.addPopupMenuListener(new PopupListener());

        JMenuItem prj = new JMenuItem("Run", new ImageIcon( iconFolder + "icons8-restart-18.png"));
        JMenuItem psj = new JMenuItem("Stop", new ImageIcon( iconFolder + "icons8-delete-18.png"));
        JMenuItem psuj = new JMenuItem("Summary", new ImageIcon( iconFolder + "icons8-document-18.png"));
        JMenuItem pdj = new JMenuItem("Detail", new ImageIcon( iconFolder + "icons8-settings-18.png"));
        JMenuItem paj = new JMenuItem("Activity", new ImageIcon( iconFolder + "icons8-news-18.png"));
        JMenuItem prlj = new JMenuItem("Reload", new ImageIcon( iconFolder + "icons8-refresh-18.png"));


        JMenuItem prg = new JMenuItem("Run", new ImageIcon( iconFolder + "icons8-restart-18.png"));
        JMenuItem psg = new JMenuItem("Stop", new ImageIcon( iconFolder + "icons8-delete-18.png"));


        jobPopup.add(prj);
        jobPopup.add(psj);
        jobPopup.addSeparator();
        jobPopup.add(pdj);
        jobPopup.add(psuj);
        jobPopup.add(paj);
        jobPopup.addSeparator();
        jobPopup.add(prlj);

        groupPopup.add(prg);
        groupPopup.add(psg);


        prj.addActionListener(new popupRunListener());
        psj.addActionListener(new popupStopListener());
        pdj.addActionListener(new popupDetailsListener());
        psuj.addActionListener(new popupSummaryListener());
        paj.addActionListener(new popupActivityListener());
        prlj.addActionListener(new popupReloadListener());

        prg.addActionListener(new popupRunListener());
        psg.addActionListener(new popupStopListener());

        buildTree();


    }




    public JTree getTree() { return tree; }







    public void buildTree() {


        try {

            logger.info("Building tree");

            if(tree.contains(1, 2)){

                logger.info("Removing nodes");

                DefaultTreeModel dtm = (DefaultTreeModel) tree.getModel();
                DefaultMutableTreeNode jn  = (DefaultMutableTreeNode) tree.getPathForRow(2).getLastPathComponent();
                DefaultMutableTreeNode gn  = (DefaultMutableTreeNode) tree.getPathForRow(1).getLastPathComponent();
                dtm.removeNodeFromParent(jn);
                dtm.removeNodeFromParent(gn);
                dtm.reload();

            }

            gm.buildGroupNodes(top, groupTopNodeKW);
            jm.buildJobNodes(top, jobsTopNodeKW);
            tree.expandRow(0);
            tree.expandRow(2);


        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, null, null, null));
        }



    }













    class popupRunListener implements ActionListener {


        @Override
        public void actionPerformed(ActionEvent actionEvent) {

            freezeSelection = false;

            if (popupSelType.equals(groupTopNodeKW))
                gm.runGroup(popupSelNode, context);
            else
                jm.startJob(popupSelNode, false);



        }
    }





    class popupStopListener implements ActionListener {


        @Override
        public void actionPerformed(ActionEvent actionEvent) {

            freezeSelection = false;

            if (popupSelType.equals(groupTopNodeKW))
                gm.stopGroup(popupSelNode);
            else
                jm.stopJob(popupSelNode);

        }
    }







    class popupDetailsListener implements ActionListener {


        @Override
        public void actionPerformed(ActionEvent actionEvent) {

            freezeSelection = false;

            Runnable r = new JobDetails(jm.getJob(popupSelNode));
            Thread th = new Thread(r);
            th.start();


        }

    }





    class popupSummaryListener implements ActionListener {


        @Override
        public void actionPerformed(ActionEvent actionEvent) {

            freezeSelection = false;

            jm.getJobSummary(popupSelNode);

        }

    }



    class popupActivityListener implements ActionListener {


        @Override
        public void actionPerformed(ActionEvent actionEvent) {

            freezeSelection = false;

            jm.getJobActivity(popupSelNode);

        }

    }





    class popupReloadListener implements ActionListener {


        @Override
        public void actionPerformed(ActionEvent actionEvent) {

            freezeSelection = false;

            File f = new File(fileProps.getProperty(Main.fpJobFolderKW) + "/" + popupSelNode + ".json");
            jm.loadJob(f, false);

        }

    }


















    class PopupListener implements PopupMenuListener {


        @Override
        public void popupMenuWillBecomeVisible(PopupMenuEvent popupMenuEvent) {

        }

        @Override
        public void popupMenuWillBecomeInvisible(PopupMenuEvent popupMenuEvent) {
            freezeSelection = false;
        }

        @Override
        public void popupMenuCanceled(PopupMenuEvent popupMenuEvent) {

        }
    }










    private class TreeRenderer extends DefaultTreeCellRenderer {

        ImageIcon nodeOpenIcon = new ImageIcon(iconFolder + "icons8-opened-folder-18.png");
        ImageIcon nodeClosedIcon = new ImageIcon(iconFolder + "icons8-folder-18.png");
        ImageIcon topIcon = new ImageIcon(iconFolder + "icons8-toolbox-18.png");
        ImageIcon compIcon = new ImageIcon(iconFolder + "icons8-connect-18.png");
        ImageIcon jobIcon = new ImageIcon(iconFolder + "icons8-file-18.png");
        ImageIcon groupIcon = new ImageIcon(iconFolder + "icons8-briefcase-18.png");



        public Component getTreeCellRendererComponent(JTree tree, Object value, boolean sel, boolean expanded, boolean leaf, int row, boolean hasFocus) {

            super.getTreeCellRendererComponent(tree, value, sel, expanded, leaf, row, hasFocus);


            DefaultMutableTreeNode n = (DefaultMutableTreeNode) value;

            boolean jobFlag = false;
            if (!n.isRoot()) {

                TreeNode[] nodes = n.getPath();
                for (int i = 0; i < nodes.length; i++)
                    if (nodes[i].toString().equals(jobsTopNodeKW))
                        jobFlag = true;

            }

            if (n.isRoot()) {

                // set the root
                setIcon(topIcon);

            } else if ( n.getParent().equals(tree.getModel().getRoot()) ) {

                // set the job and group folder
                setOpenIcon(nodeOpenIcon);
                setClosedIcon(nodeClosedIcon);

            } else if ( n.getParent().toString().equals(jobsTopNodeKW) || n.getParent().toString().equals(groupTopNodeKW) ) {

                if (n.getParent().toString().equals(jobsTopNodeKW) )
                    setIcon(jobIcon);
                else
                    setIcon(groupIcon);

            } else if (!leaf && !jobFlag) {

                // set the job on the group folder to the job icon
                setIcon(jobIcon);

            } else if (!leaf && jobFlag) {

                // set the components
                setIcon(compIcon);

            } else {

                setIcon(null);

            }


            return this;

        }






    }






}
