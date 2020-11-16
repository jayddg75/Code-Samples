package com.ELTTool;


import org.json.JSONArray;
import org.json.JSONObject;

import javax.swing.tree.DefaultMutableTreeNode;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Properties;
import java.util.logging.Logger;


/**
 *This loads, gets, and runs groups
 */
public class GroupManager {




    private static Logger logger = Logger.getLogger(GroupManager.class.getName());

    private Properties fileProps;
    private JobManager jm;

    private ArrayList<Group> groups;



    private static final String memberKW = "members";
    private static final String nameKW = "name";
    private static final String orderKW = "order";
    private static final String waitKW = "wait";







    public GroupManager(Properties fileProps, JobManager jm) {
        this.fileProps = fileProps;
        this.jm = jm;
        groups = new ArrayList<>();
    }




    public ArrayList<Group> getGroups() {
        return groups;
    }




    public String[] getGroupList() {

        String[] out = new String[groups.size() + 1];

        out[0] = Status.All.toString();

        int i = 1;
        for (Group g : groups){
            out[i] = g.getGroupName();
            i++;
        }

        return out;

    }






    public boolean validateGroupName(String groupName) {

        boolean match = false;
        for (Group g : groups)
            if (g.getGroupName().equals(groupName))
                match = true;


        return match;

    }





    public void buildGroupNodes(DefaultMutableTreeNode top, String topGroupNodeName) {



        DefaultMutableTreeNode groupsTop = new DefaultMutableTreeNode(topGroupNodeName);
        top.add(groupsTop);

        for (Group g : groups) {

            DefaultMutableTreeNode group = new DefaultMutableTreeNode(g.getGroupName());
            groupsTop.add(group);

            for (Member m : g.getMemberList()) {

                DefaultMutableTreeNode mem = new DefaultMutableTreeNode(m.getJobName());
                group.add(mem);
                mem.add(new DefaultMutableTreeNode("Order: " +  m.getOrder()));
                mem.add(new DefaultMutableTreeNode("Wait: " + m.getWait()));

            }



        }


    }





    public void loadAllGroups() {

        try {

            logger.info("Loading all groups");

            File groupsLocation = new File(fileProps.getProperty(Main.fpGroupFolderKW));
            File[] files = groupsLocation.listFiles();

            for (File f : files ) {
                loadGroup(f);
            }

            logger.info("Loaded all groups");

        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, null, null, null));
        }



    }






    public void loadGroup(File f) {


        try {


            logger.info("Loading group file: " + f.getName());

            //-------------------------------------------------------
            // read the file and convert to JSON object

            BufferedReader br = new BufferedReader(new FileReader(f));
            StringBuilder sb = new StringBuilder();

            String line;
            while ( (line = br.readLine()) != null )
                sb.append(line);

            br.close();


            JSONObject jo = new JSONObject(sb.toString());
            ArrayList<Member> jobList = new ArrayList<>();

            String groupName = jo.getString(nameKW);


            //-------------------------------------------------------------------
            //-------------------------------------------------------------------
            // check to see if the group was already loaded

            boolean loadGroup = true;
            boolean checkFlag = false;
            Group sameGroup = null;

            for (Group g : groups) {

                if (g.getGroupName().equals(groupName)) {

                    checkFlag = true;
                    sameGroup = g;

                }

            }



            if (checkFlag) {

                int r = GetInput.GetYesNo("Choose", "Do you want to replace the existing group?");

                if (r == 0) {


                    // remove the job from the jobs array and null it out
                    groups.remove(sameGroup);
                    sameGroup = null;

                } else {
                    loadGroup = false;
                }

            }





            // ---------------------------------------------------------------------------
            // load the group if needed

            if (loadGroup) {


                //------------------------------------------------
                // go through each job in the group ... validate and add

                JSONArray ja = jo.getJSONArray(memberKW);
                int i = 0;

                for (Object o : ja) {

                    i++;
                    JSONObject curJO = (JSONObject) o;

                    String jobName = curJO.getString(nameKW);
                    int jobOrder = curJO.getInt(orderKW);
                    boolean jobWait = curJO.getBoolean(waitKW);


                    // validate that the order is correct on the group file
                    if(jobOrder != i) {

                        String message = "Group: " + groupName + " has an invalid order at position: " + jobOrder;
                        NotificationBox.displayNotification(message);
                        logger.info(message);
                        break;

                    }


                    //validate the job name
                    if(jm.validateJobName(jobName)) {

                        Member m = new Member(jobName, jobWait, jobOrder);
                        jobList.add(m);

                    } else {

                        String message = "Group: " + groupName + " has an invalid job: " + jobName +  " at order: " + jobOrder;
                        NotificationBox.displayNotification(message);
                        logger.info(message);
                        break;

                    }




                } // end looping on the jobs on the group file


                // create the group and add it to the list
                Group g = new Group(jobList, jm, groupName);
                groups.add(g);



            }








        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, null, null, null));
        }




    }




    public String selectGroup() {


        String out = null;

        String[] groupNames = new String[groups.size()];

        for (int i = 0; i < groups.size(); i++ )
            groupNames[i] = groups.get(i).getGroupName();


        out = GetInput.GetChoice("Please choose the Group you want details about", "Choose Group", groupNames);

        if (out == null)
            NotificationBox.displayNotification("No choice was made");

        return out;



    }




    public Object[][] getGroupDetail(String groupName) {

        Object[][] out = null;

        for(Group g : groups)
            if (g.getGroupName().equals(groupName)) {

                out = new Object[g.getMemberList().size()][3];

                for (int i = 0; i < g.getMemberList().size(); i++) {

                    out[i][0] = i + 1;
                    out[i][1] = g.getMemberList().get(i).getJobName();
                    out[i][2] = jm.getJobStatus(g.getMemberList().get(i).getJobName());

                }

            }


        return out;


    }




    public void runGroup(String groupName, String context) {


        try {



            logger.info("Trying to run group: " + groupName);

            for (Group g : groups)
                if (g.getGroupName().equals(groupName)) {

                    // don't allow another start up of a group if it is not stopped
                    if (g.getStatus() == Status.Stopped.getStatusCode()) {

                        g.setContext(context);
                        Runnable r = g;
                        Thread th = new Thread(r);
                        th.setName(groupName);
                        th.start();


                    } else {

                        NotificationBox.displayNotification("Group " + groupName + " is not stopped and cannot be started until it is");

                    }



                }


        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, null, null, null));
        }


    }




    public void stopGroup(String groupName) {

        for (Group g : groups)
            if (g.getGroupName().equals(groupName))
                g.killGroup();




    }







}
