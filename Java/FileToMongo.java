package com.ELTTool;

import net.snowflake.client.jdbc.internal.org.checkerframework.checker.units.qual.A;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;
import java.util.logging.Logger;






public class FileToMongo extends Component {


    private static Logger logger = Logger.getLogger(FileToMongo.class.getName());

    private ConnectionManager cm;
    private Properties fileProps;

    private Mongo mon;
    private String ref;

    private File topWorkingFolder;
    private File loadFromFolder;
    private File workFolder;



    public FileToMongo(ConnectionManager cm, Properties fileProps){
        this.cm = cm;
        this.fileProps = fileProps;
    }




    @Override
    public void runComponent() {


        try {


            topWorkingFolder = new File(fileProps.getProperty(Main.fpLoadFolderKW) + "\\" + prefix);
            loadFromFolder = new File(topWorkingFolder.getPath() + "\\" + JobManager.componentLoadFolderNameKW);
            workFolder = new File(topWorkingFolder.getPath() + "\\" + JobManager.componentWorkFolderNameKW);


            mon = cm.getMongoConnection(ref);
            mon.startConnection(0, jobId, groupId);

            if (runStyle.equals(JobManager.batchRunStyle)) {

                loadFileList();

            } else {

                // keep looking for files in the load folder


            }



        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, jobName, jobId, groupId));
            killComponent();
        }




    }






    private void loadFileList() {

        for(File f : compPassList)
            loadFile(f);

    }






    private void loadFile(File f) {

        try {


            BufferedReader br = new BufferedReader(new FileReader(f));
            StringBuilder sb = new StringBuilder();
            String line;

            while( (line = br.readLine()) != null)
                sb.append(line);

            br.close();

            JSONArray ja = new JSONArray(sb.toString());



            ArrayList<Document> dList = new ArrayList<>();

            for (Object o : ja) {

                // put a document together for each object
                JSONObject jo = (JSONObject) o;
                Document d = new Document();

                Iterator<String> ky = jo.keys();

                while(ky.hasNext()){
                    String k = ky.next();
                    d.append(k, jo.get(k));
                }

                dList.add(d);


            }


            mon.insertDocuments(dList, prefix);


            //now that the file is read and put into the Mongo landing table delete the file
            while(!f.delete()) {
                // wait for the file to be deleted
            }


        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, jobName, jobId, groupId));
            killComponent();
        }





    }






    @Override
    public boolean loadComponent(JSONObject runParms, String context) {

        try {


            this.context = context;
            JSONArray pa = runParms.getJSONArray("Parameters");


            for (int i = 0; i < pa.length(); i++) {

                JSONObject curParm = pa.getJSONObject(i);
                String parmName = (String) curParm.names().get(0);

                switch (parmName) {

                    case "ref":
                        ref = (String) getJSONValue(curParm, context, Component.stringType, parmName);
                        break;

                }

            }



            return true;


        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, jobName, jobId, groupId));
            return false;
        }


    }



    @Override
    public void closeComponent() {

    }



    @Override
    public ArrayList<String> reportDetails() {
        return null;
    }




    private class fileChecker implements Runnable {


        @Override
        public void run() {



        }




    }




}
