package com.ELTTool;



import org.json.JSONArray;
import org.json.JSONObject;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;


/**
 *This component will handle calling APIs and works in three modes
 *<ul>
 *<li><i>list</i> - this will call the API and put the list back to the job for a subsequent component to pick up</li>
 *<li><i>list-db</i> - this will take a list from a previous component and make iterative calls (multiple threads) and create files</li>
 *<li><i>db</i> - this will call the API and make files</li>
 *</ul>
 *<br>
 *<br>
 *Job File Requirements
 * <br>
 * <ul>
     * <li>Order - Required:Integer - The order this component should run in: 1 (1st), 2 (2nd) ... </li>
     * <li>Component - Required:String - <i>APICall</i></li>
     * <li>Prefix - Required:Integer - The number in the Prefix array needed: 1, 2, etc...</li>
     * <li>RunStyle - Required:String - <i>batch</i></li>
     * <li>requestMethod - Required:String - only <i>GET</i> at this time</li>
     * <li>getOutput - Required:Boolean - only <i>true</i> supported at this time</li>
     * <li>mode - Required:String - one of the mode listed above</li>
     * <li>Parameters - Required:JSON Array - list of parms described below
         * <ul>
         * <li>url - Required:String - base url for the API</li>
         * <li>returnType - Required:String - the expected JSON return type for the API: <i>array</i> or <i>object</i></li>
         * <li>threadCount - Required for list-db mode:Integer - number of threads to use for list-db run</li>
         * <li>fileRecLimit - Required for list-db and db modes:Integer - the number of records to put in the file</li>
         * <li>moveToLoadFolder - Optional:Boolean - for longer runs, to move the files to the load folder instead of leaving them in the work folder</li>
         * <li>addKeyToRecord - Optional:Boolean - if you want to pass in all static and iterative parms into the outbound JSON object</li>
         * <li>staticParms - Required:JSON Array - list of static API parameters to pass into the API
         * <ul>
             * <li>name - String - name of the API parameter</li>
             * <li>type - String - type of API parameter: <i>header</i>, <i>query</i>, or <i>path</i></li>
             * <li>context fields and values - String - per the usual: <i>dev</i>, <i>qa</i>, <i>stage</i>, <i>prod</i></li>
         * </ul>
         * </li>
         * </ul>
     * </li>
 * </ul>
 */
public class APICall extends Component {


    private static Logger logger = Logger.getLogger(APICall.class.getName());


    private String url;
    private String mode;
    private Properties fileProps;

    private int fileRecLimit;
    private int threadCount;
    private String requestMethod;
    private boolean getOutput;

    private boolean addKeyToRecord;


    public static final String listModeKeyword = "list";
    public static final String listDBModeKeyword = "list-db";
    public static final String dbModeKeyword = "db";

    public static final String headerAPITypeKW = "header";
    public static final String queryAPITypeKW = "query";
    public static final String pathAPITypeKW = "path";

    public static final String nameAPIKW = "name";
    public static final String typeAPIKW = "type";
    public static final String valueAPIKW = "value";

    public static final String jsonArrayReturnKW = "array";
    public static final String jsonObjectReturnKW = "object";

    private final String recordSource = "A";

    private String workFolder;
    private String loadFolder;

    private JSONArray curObj;
    private boolean dbFlag;

    private JSONArray iterateList;
    private ArrayList<File> fileList;

    private AtomicInteger recordCount;
    private AtomicInteger fileCount;

    private AtomicBoolean killThreads;
    private ArrayBlockingQueue<JSONArray> queue;
    private ConcurrentLinkedQueue<File> multiFileList;

    private JSONArray staticParms;
    private String returnType;

    private boolean moveToLoadFolder;

    private ExecutorService execServ;

    private ProgressBar pb;
    private AtomicInteger iterateCount;






    //------------------------------------------------------------------------------------------
    // constructor


    public APICall(Properties fileProps) {

        this.fileProps = fileProps;
        recordCount = new AtomicInteger(0);
        fileCount = new AtomicInteger(0);
        killThreads = new AtomicBoolean(false);
        addKeyToRecord = false;
        moveToLoadFolder = false;


    }







    //------------------------------------------------------------------------------------------
    // getters and setters


    public JSONArray getIterateList() { return iterateList; }

    public void setIterateList(JSONArray iterateList) {

        this.iterateList = new JSONArray();
        this.iterateList = iterateList;

    }


    public ArrayList<File> getFileList() { return fileList; }

    public String getMode() { return mode; }










    //------------------------------------------------------------------------------------------
    // main methods


    /**
     * The component required run method that runs the job
     */
    @Override
    public void runComponent() {


        //-------------------------------------------------
        // different run modes:
        // list = just pass the array out for another component to pick up
        // list-db = pick up the JSONObject from the job and call the API recursively and land in the db
        // db = call API and land in the db


        try {

            pb = null;
            recordCount.set(0);
            fileCount.set(0);
            iterateCount = new AtomicInteger(0);
            fileList = new ArrayList<>();

            LoadFolders.checkLoadFolders(fileProps, prefix);
            workFolder = fileProps.getProperty(Main.fpLoadFolderKW) + "\\" + prefix + "\\" + JobManager.componentWorkFolderNameKW;
            loadFolder = fileProps.getProperty(Main.fpLoadFolderKW) + "\\" + prefix + "\\" + JobManager.componentLoadFolderNameKW;

            // check to see if this needs to get landed, just not for list mode, the other two need to create files
            dbFlag = false;
            if ( mode.equals(dbModeKeyword) || mode.equals(listDBModeKeyword) )
                dbFlag = true;


            logger.info(LoggingUtils.getLogEntry("APICall mode: " + mode, jobName, jobId, groupId));
            logger.info(LoggingUtils.getLogEntry("dbFlag: " + dbFlag + mode, jobName, jobId, groupId));




            multiFileList = new ConcurrentLinkedQueue<>();


            // set the behavior for each mode
            if (mode.equals(listDBModeKeyword)) {


                logger.info(LoggingUtils.getLogEntry("List size: " + iterateList.length(), jobName, jobId, groupId));

                //--------------------------------------------------------------------
                // load all the keys into a blocking queue for multiple threads to pick off of
                // iterating on ISE customers returns about 2800 orgs, so multithreading is a life saver here time wise

                queue = new ArrayBlockingQueue(iterateList.length());

                for(Object o : iterateList) {
                    curObj = (JSONArray) o;

                    //System.out.println(curObj.toString());

                    while ( !(queue.offer(curObj)) ) {
                        // loop while waiting for the string to get inserted into the queue
                    }

                }


                logger.info(LoggingUtils.getLogEntry("Queue size: " + queue.size(), jobName, jobId, groupId));

                //-----------------------------------------------------------------------
                // start up the threads for the consumers ...

                logger.info(LoggingUtils.getLogEntry("Running threads count: " + threadCount, jobName, jobId, groupId));


                pb = new ProgressBar(0, iterateList.length(), "API List-DB for job: " + jobName, false);

                ArrayList<Future<?>> futures = new ArrayList<>();
                execServ = Executors.newFixedThreadPool(threadCount);

                for (int i = 0; i < threadCount; i++)
                    futures.add(execServ.submit(new CallIterateAPI()));


                // wait for all threads to be done
                boolean allThreadsDone = false;
                while (!allThreadsDone) {

                    // wait one second
                    Thread.sleep(1000);

                    int doneCount = 0;
                    for(Future<?> f : futures) {
                        if (f.isDone())
                            doneCount++;
                    }

                    if (doneCount == threadCount) {
                        allThreadsDone = true;
                    }

                }


                logger.info(LoggingUtils.getLogEntry("Iterate count: " + iterateCount.get(), jobName, jobId, groupId));

                // add the files into the input/output file array list
                for(File f : multiFileList) {
                    fileList.add(f);
                }




            } else if (mode.equals(listModeKeyword)) {

                iterateList = callAPI(staticParms, null);
                logger.info(LoggingUtils.getLogEntry("API List count: " + iterateList.length(), jobName, jobId, groupId));

            } else {

                JSONArray jo = callAPI(staticParms, null);
                writeFile(jo);
                fileCount.incrementAndGet();

                // add the files into the input/output file array list
                for(File f : multiFileList) {
                    fileList.add(f);
                }

            }



            support.firePropertyChange(getPropertyChangeMetricId(Metrics.apiIterations.toString()), 0, iterateCount.get());
            support.firePropertyChange(getPropertyChangeMetricId(Metrics.recordCount.toString()), 0, recordCount.get());
            support.firePropertyChange(getPropertyChangeMetricId(Metrics.fileCount.toString()), 0, fileCount.get());

            completeComponent();



        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, jobName, jobId, groupId));
            killComponent();
        }


    }









    /**
     *Called for an iterative list-db mode run.  This is multi-threaded for efficiency getting through the list.
     */
    class CallIterateAPI implements Runnable {


        @Override
        public void run() {

            try {



                int recCount = 0;

                Object curKey = null;
                JSONArray out = new JSONArray();

                // while it pulled a value and the queue is not empty
                while ( (curKey = queue.poll()) != null ) {

                    iterateCount.getAndIncrement();

                    //logger.info("Picked up record: " + recCount );

                    // call the API
                    JSONArray curList = callAPI( staticParms, (JSONArray) curKey );

                    // deal with the record counts
                    recCount = recCount + curList.length();

                    pb.updateProgressBar(iterateCount.get());

                    boolean writeFile = false;
                    if (recCount >= fileRecLimit) {
                        writeFile = true;
                    }


                    // strip out the individual elements in the return from call API array
                    for (Object o : curList)
                        out.put((JSONObject) o);




                    if (writeFile) {
                        writeFile(out);
                        fileCount.getAndIncrement();
                        recCount = 0;
                        out = new JSONArray();
                    }



                } // end iterating on queue



                // write out what is left after the queue is empty, if there is anything to write out
                if (out.length() > 0) {
                    writeFile(out);
                    fileCount.getAndIncrement();
                    out = null;
                }


            } catch (Exception e) {
                logger.severe(LoggingUtils.getErrorEntry(e, jobName, jobId, groupId));
                killComponent();
            }




        }




    }





    private JSONArray callAPI(JSONArray staticParms, JSONArray loopParms) {



        try {




            //------------------------------------------------------------------
            //------------------------------------------------------------------
            //------------------------------------------------------------------
            // call the API

            //-------------------------------------------------------------------
            // format for calling API
            // baseURL/path?query1=value1&query2=value2&etc...
            // headers are properties


            //System.out.println(loopParms);

            //-------------------------------------------------------------
            // go through the parms

            StringBuilder sbQ = new StringBuilder();
            StringBuilder sbP = new StringBuilder();

            HashMap<String, String> headerProperties = new HashMap<>();


            //----------------------------------------------------------
            // static stuff

            for (Object o : staticParms) {

                JSONObject sJo = (JSONObject) o;

                switch (sJo.getString(typeAPIKW)) {

                    case headerAPITypeKW:
                        headerProperties.put(sJo.getString(nameAPIKW), (String) sJo.get(context));
                        break;
                    case queryAPITypeKW:
                        sbQ.append("&").append(sJo.getString(nameAPIKW)).append("=").append((String) sJo.get(context));
                        break;
                    case pathAPITypeKW:
                        sbP.append("/").append((String) sJo.get(context));

                }


            }


            //----------------------------------------------------------
            // loop stuff


            // set the loop property if iterating
            if (mode.equals(listDBModeKeyword)) {

                for (Object o : loopParms) {

                    JSONObject lJo = (JSONObject) o;

                    switch (lJo.getString(typeAPIKW)) {

                        case headerAPITypeKW:
                            headerProperties.put(lJo.getString(nameAPIKW), lJo.getString(valueAPIKW));
                            break;
                        case queryAPITypeKW:
                            sbQ.append("&").append(lJo.getString(nameAPIKW)).append("=").append(lJo.getString(valueAPIKW));
                            break;
                        case pathAPITypeKW:
                            sbP.append("/").append(lJo.getString(valueAPIKW));

                    }
                }


            }


            //----------------------------------------------------------------------------------------------
            //----------------------------------------------------------------------------------------------
            // build out the url with any query or path parms and add the properties


            StringBuilder urlOut = new StringBuilder();
            urlOut.append(url);

            // add any paths
            if (sbP.length() > 0)
                urlOut.append(sbP.toString());

            if (sbQ.length() > 0)
                urlOut.append("?").append(sbQ.toString().substring(1));


            // make sure to replace any spaces with %20
            String urlFinal = urlOut.toString().replace(" ", "%20");

            //logger.info(urlFinal);


            URL hUrl = new URL(urlFinal);
            HttpURLConnection hCon = (HttpURLConnection) hUrl.openConnection();
            hCon.setRequestMethod(requestMethod);
            hCon.setDoOutput(getOutput);


            // set the request properties
            for (HashMap.Entry<String, String> e : headerProperties.entrySet()) {
                //logger.info(e.getKey() + " | " + e.getValue());
                hCon.setRequestProperty(e.getKey(), e.getValue());
            }







            //------------------------------------------------------------------
            //------------------------------------------------------------------
            //------------------------------------------------------------------
            // deal with the response

            int responseCode = hCon.getResponseCode();

            // check the codes for an error response
            boolean errorResponse = false;
            if (responseCode > 299 || responseCode < 200)
                errorResponse = true;

            //logger.info(LoggingUtils.logEntry("Response Code: " + responseCode + " | message: " + hCon.getResponseMessage(), jobName));



            JSONArray rawJa = null;

            if (!errorResponse) {

                BufferedReader br = new BufferedReader(new InputStreamReader(hCon.getInputStream()));
                StringBuilder cstr = new StringBuilder();
                String cline;

                while ((cline = br.readLine()) != null)
                    cstr.append(cline).append(System.lineSeparator());

                br.close();


                if (returnType.equals(jsonArrayReturnKW)) {

                    rawJa = new JSONArray(cstr.toString());

                } else {

                    JSONObject newJo = new JSONObject(cstr.toString());
                    rawJa = new JSONArray();
                    rawJa.put(newJo);

                }


            }




            JSONArray out = new JSONArray();
            JSONObject parms = addParms(staticParms, loopParms, urlFinal, responseCode, hCon.getResponseMessage());


            if (rawJa == null || rawJa.length() == 0) {

                // deal with errors
                recordCount.getAndIncrement();
                out.put(so.standardRecord(recordSource, prefix, "{}", false, parms, false));


            } else {

                recordCount.getAndAdd(rawJa.length());

                if (mode.equals(listModeKeyword)) {

                    out = rawJa;

                } else {

                    so.setCrossMessageIds();
                    // calls that returned something
                    Iterator<Object> ri = rawJa.iterator();
                    while (ri.hasNext()) {

                        JSONObject jo = (JSONObject) ri.next();
                        out.put(so.standardRecord(recordSource, prefix, jo.toString(), false, parms, true));

                    }


                }


            }



            //logger.info("RecordOut: " + out.toString());
            return out;




        } catch (IOException e) {

            logger.severe(LoggingUtils.getErrorEntry(e, jobName, jobId, groupId));
            killComponent();
            return null;

        }



    }





    private JSONObject addParms(JSONArray staticParms, JSONArray loopParms, String urlFinal, int responseCode, String responseMessage) {


        // write an object down to the landing table noting something was wrong.
        JSONObject finalJO = new JSONObject();
        JSONArray parms = null;

        boolean hasStatic = false;
        boolean hasLoop = false;

        if(staticParms != null && staticParms.length() > 0)
            hasStatic = true;


        if (loopParms != null && loopParms.length() > 0)
            hasLoop = true;

        if (hasStatic || hasLoop) {
            parms = new JSONArray();
        }



        if(hasStatic) {

            Iterator<Object> sp = staticParms.iterator();
            while (sp.hasNext()) {
                JSONObject jo = (JSONObject) sp.next();
                parms.put(jo);
            }

        }



        if(hasLoop) {

            Iterator<Object> lp = loopParms.iterator();
            while (lp.hasNext()) {
                JSONObject jo = (JSONObject) lp.next();
                parms.put(jo);
            }

        }



        JSONObject rJo = new JSONObject();
        rJo.put("url", urlFinal);
        rJo.put("responseCode", responseCode);
        rJo.put("responseMessage", responseMessage);


        // finalize object
        if (hasStatic || hasLoop) {
            finalJO.put("parms", parms);
        }

        finalJO.put("request", rJo);

        return finalJO;


    }










    private void writeFile (JSONArray toFile) {

        try {


            logger.info(LoggingUtils.getLogEntry("Writing file with record count " + toFile.length() + " | Thread: " + Thread.currentThread().getName(), jobName, jobId, groupId));

            String fileName = so.standardFileName(prefix, recordSource);
            String workingFile = workFolder + "\\" + fileName;
            String loadingFile = loadFolder + "\\" + fileName;

            logger.info(LoggingUtils.getLogEntry(workingFile, jobName, jobId, groupId));




            BufferedWriter bw = new BufferedWriter(new FileWriter(workingFile));
            bw.write(toFile.toString());
            bw.flush();
            bw.close();


            //----------------------------------------
            // either move the file to the load folder or add it to the pickup list

            File workFile = new File(workingFile);
            File loadFile = new File(loadingFile);

            if(moveToLoadFolder) {

                Files.move( Paths.get( workFile.getPath() ),  Paths.get( loadFile.getPath() ), StandardCopyOption.ATOMIC_MOVE);

            } else {

                while ( !multiFileList.add(workFile) ){
                    // make sure it got added
                }

            }



        } catch (IOException e) {

            logger.severe(LoggingUtils.getErrorEntry(e, jobName, jobId, groupId));
            killComponent();
        }


    }











    @Override
    public boolean loadComponent(JSONObject runParms, String context) {


        try {


            this.context = context;
            JSONArray pa = runParms.getJSONArray("Parameters");
            mode = (String) getJSONValue(runParms, "mode", "s");
            requestMethod = (String) getJSONValue(runParms, "requestMethod", "s");
            getOutput = (Boolean) getJSONValue(runParms, "getOutput", "b");


            for (int i = 0; i < pa.length(); i++) {

                JSONObject curParm = pa.getJSONObject(i);
                String parmName = (String) curParm.names().get(0);

                switch (parmName) {

                    case "url":
                        url = (String) getJSONValue(curParm, context, Component.stringType, parmName);
                        break;
                    case "fileRecLimit":
                        fileRecLimit = curParm.getJSONObject(parmName).isNull(context) ? 0 : (Integer) getJSONValue(curParm, context, Component.intType, parmName);
                        break;
                    case "threadCount":
                        threadCount = curParm.getJSONObject(parmName).isNull(context) ? 0 : (Integer) getJSONValue(curParm, context, Component.intType, parmName);
                        break;
                    case "addKeyToRecord":
                        addKeyToRecord = (Boolean) getJSONValue(curParm, context, Component.booleanType, parmName);
                        break;
                    case "staticParms":
                        staticParms = curParm.getJSONArray(parmName);
                        break;
                    case "returnType":
                        returnType = (String) getJSONValue(curParm, context, Component.stringType, parmName);
                        break;
                    case "moveToLoadFolder":
                        moveToLoadFolder = (Boolean) getJSONValue(curParm, context, Component.booleanType, parmName);
                        break;

                }

            }


            if (returnType == null)
                throw new Exception("returnType was not passed in on job: " + jobName + " for " + getComponentId());


            return false;


        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, jobName, jobId, groupId));
            killComponent();
            return false;
        }


    }





    @Override
    public void closeComponent() {

        if(pb != null)
            pb.closeProgressBar();

        if (mode.equals(listDBModeKeyword))
            ShutdownExecutor.shutdownSES(execServ, jobName, jobId, groupId);



    }






    @Override
    public ArrayList<String> reportDetails() {


        ArrayList<String> a = allCompReportDetails();

        addDetailString("threadCount", threadCount, a);
        addDetailString("url", url, a);
        addDetailString("mode", mode, a);
        addDetailString("fileRecLimit", order, a);
        addDetailString("addKeyToRecord", runStyle, a);
        addDetailString("returnType", groupId, a);
        addDetailString("staticParmsCount", groupName, a);
        addDetailString("moveToLoadFolder", status.get(), a);


        return a;


    }






}
