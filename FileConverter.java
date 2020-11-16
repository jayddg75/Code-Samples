package com.ELTTool;

import org.json.JSONArray;
import org.json.JSONObject;
import java.io.*;
import java.util.*;
import java.util.logging.Logger;



/**
 *This component will handle converting delimited files to the standard JSON format for subsequent processing
 *<br>
 *<br>
 *Job File Requirements
 * <br>
 * <ul>
     * <li>Order - Required - The order this component should run in: 1 (1st), 2 (2nd) ... </li>
     * <li>Component - Required - <i>FileConverter</i></li>
     * <li>Prefix - Required - The number in the Prefix array needed: 1, 2, etc...</li>
     * <li>RunStyle - Required - <i>batch</i></li>
     * <li>Parameters - Required - list of parms described below
     * <ul>
         * <li>filePath - Required - the file path for the delimited file</li>
         * <li>delimiter - Required - the expected delimited for the file</li>
         * <li>skipFirstRow - Optional - pass this in if you want to skip the first row (header row)</li>
         * <li>fileSchema - Required - the expected schema for the file. One entry for each column in the file
         * <ul>
             * <li>name - column name</li>
             * <li>type - data type: <i>I</i> for numeric, <i>S</i> for string
         * </ul>
         * </li>
     * </ul>
     * </li>
 * </ul>
 */
public class FileConverter extends Component {



    private static Logger logger = Logger.getLogger(FileConverter.class.getName());




    private File sourceFile;
    private String delimiter;

    private LinkedHashMap<String, String> fileSchema;
    private boolean skipFirstRow;

    private BufferedReader br;
    private BufferedWriter bw;

    private Properties fileProps;

    private int sourceFileRecordCount;
    private int targetFileRecordCount;
    private int targetFileCount;

    private ArrayList<File> fileList;






    public FileConverter(Properties fileProps) {

        this.fileProps = fileProps;
        fileList = new ArrayList<>();

    }









    //----------------------------------------------------------------------------------------
    // getters and setters



    public ArrayList<File> getFileList() { return fileList; }








    //----------------------------------------------------------------------------------------
    // general methods

    @Override
    public void runComponent() {


        try {


            bw = null;
            br = null;

            sourceFileRecordCount = 0;
            targetFileRecordCount = 0;
            targetFileCount = 0;





            //-------------------------------------------------------------------------------------
            // read the file and put into a standard record

            br = new BufferedReader(new FileReader(sourceFile));
            String line;

            JSONArray arrayOut = new JSONArray();



            while ( (line = br.readLine()) != null ) {


                if (sourceFileRecordCount == 0 && skipFirstRow){

                    // skip the first row if needed
                    sourceFileRecordCount++;

                } else {

                    JSONObject curObj = new JSONObject();
                    String[] data = line.split(delimiter);



                    int lCounter = 0;
                    for (Map.Entry<String, String> e : fileSchema.entrySet()){

                        //System.out.println("key: " + e.getKey() + " | value: " + e.getValue() + " | lCounter: " + lCounter + " | arrayValue: " + data[lCounter] );

                        switch (e.getValue()) {

                            case "I":
                                if(data[lCounter].trim().isEmpty())
                                    curObj.put(e.getKey(), JSONObject.NULL);
                                else
                                    curObj.put(e.getKey(), Double.valueOf(data[lCounter]));
                                break;
                            case "S":
                                if(data[lCounter].trim().isEmpty())
                                    curObj.put(e.getKey(), JSONObject.NULL);
                                else
                                    curObj.put(e.getKey(), data[lCounter]);
                                break;


                        }

                        lCounter++;

                    }

                    sourceFileRecordCount++;


                    // add the standardized record to the final array
                    arrayOut.put(so.standardRecord("F", prefix, curObj.toString(), false));



                }


            }

            br.close();

            targetFileRecordCount = arrayOut.length();

            logger.info(LoggingUtils.getLogEntry("sourceFileRecordCount: " + sourceFileRecordCount, jobName, jobId, groupId));
            logger.info(LoggingUtils.getLogEntry("targetFileRecordCount: " + targetFileRecordCount, jobName, jobId, groupId));




            //-------------------------------------------------------------------------------------
            // set Snowflake stuff and local folders


            String fileName = so.standardFileName(prefix, "F");
            String filePath = fileProps.getProperty(Main.fpLoadFolderKW) + "\\" + prefix + "\\Work\\" + fileName;

            logger.info(LoggingUtils.getLogEntry("file: " + filePath, jobName, jobId, groupId));
            fileList.add(new File(filePath));


            LoadFolders.checkLoadFolders(fileProps, prefix);

            //-------------------------------------------------------------------------------------
            // write out to the file

            logger.info(LoggingUtils.getLogEntry("Writing out file ...", jobName, jobId, groupId));

            BufferedWriter bw = new BufferedWriter(new FileWriter(filePath));
            bw.write(arrayOut.toString());
            bw.flush();
            bw.close();

            logger.info(LoggingUtils.getLogEntry("Done writing file", jobName, jobId, groupId));

            support.firePropertyChange(getPropertyChangeMetricId(Metrics.sourceFileRecordCount.toString()), -1, (long) sourceFileRecordCount);
            support.firePropertyChange(getPropertyChangeMetricId(Metrics.targetFileRecordCount.toString()), -1, (long) targetFileRecordCount);


            completeComponent();

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

            fileSchema = new LinkedHashMap<>();

            for (int i = 0; i < pa.length(); i++) {

                JSONObject curParm = pa.getJSONObject(i);
                String parmName = (String) curParm.names().get(0);

                switch (parmName) {

                    case "filePath":
                        sourceFile = new File( (String) getJSONValue(curParm, context, Component.stringType, parmName) );
                        break;
                    case "delimiter":
                        delimiter = (String) getJSONValue(curParm, context, Component.stringType, parmName);
                        break;
                    case "fileSchema":
                        JSONArray ja = curParm.getJSONArray(parmName);
                        for(Object o : ja) {
                            JSONObject jo = (JSONObject) o;
                            fileSchema.put((String) getJSONValue(jo, "name", Component.stringType), (String) getJSONValue(jo, "type", Component.stringType));
                        }
                        break;
                    case "skipFirstRow":
                        skipFirstRow = (Boolean) getJSONValue(curParm, context, Component.booleanType, parmName);
                        break;

                }

            }


            return true;

        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, jobName, jobId, groupId));
            killComponent();
            return false;
        }


    }









    @Override
    public void closeComponent() {

        try {


            if (br != null)
                br.close();

            if (bw != null)
                bw.close();



        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, jobName, jobId, groupId));
        }



    }




    @Override
    public ArrayList<String> reportDetails() {



        ArrayList<String> a = allCompReportDetails();

        addDetailString("sourceFile", sourceFile.toString(), a);
        addDetailString("delimiter", delimiter, a);
        addDetailString("columnCount", fileSchema.size(), a);
        addDetailString("skipFirstRow", skipFirstRow, a);


        return a;


    }




}
