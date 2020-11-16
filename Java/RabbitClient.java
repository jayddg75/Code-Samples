package com.ELTTool;



import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

import com.rabbitmq.client.*;
import org.json.JSONArray;
import org.json.JSONObject;





/**
 *This component will pull messages off a Rabbit broker
 *<br>
 *<br>
 *Job File Requirements
 * <br>
 * <ul>
 * <li>Order - Required - The order this component should run in: 1 (1st), 2 (2nd) ... </li>
 * <li>Component - Required - <i>RabbitClient</i></li>
 * <li>Prefix - Required - The number in the Prefix array needed: 1, 2, etc...</li>
 * <li>RunStyle - Required - <i>continuous</i></li>
 * <li>Parameters - Required - list of parms
 * <ul>
     * <li>userName - Required - broker creds user name</li>
     * <li>password - Required - broker creds password</li>
     * <li>host - Required - broker host</li>
     * <li>virtualHost - Required - broker virtual host</li>
     * <li>port - Required - broker port</li>
     * <li>queue - Required - broker queue name. If <i>createQueue</i> is true, then this queue will be created else it will look for that queue on the host</li>
     * <li>exchange - Required if creating a queue - broker exchange</li>
     * <li>fileRecLimit - Required - the rough number of records to put in a file. Works in coordination with <i>fileTimeLimitSecs</i></li>
     * <li>fileTimeLimitSecs - Required - after a record is received, this will determine how long before wrapping up a file if the record limit has not been reached</li>
     * <li>createQueue - Required - <i>true</i> or <i>false</i>. If true pass the exchange is required and the queue will be created with the name give in <i>queue</i></li>
     * <li>includeHeader - Optional - pass this as true if you want to include the message headers in the outgoing record</li>
     * <li>checkIntervalMillis - Required - how often to run internal checks on file limits</li>
     * <li>routingKey - Optional - if this is not passed then <i>#</i> will be used for the routing key</li>
     * <li>ssl - Optional - pass this parameter with <i>true</i> if you need to use SSL</li>
 * </ul>
 * </li>
 * </ul>
 */
public class RabbitClient extends Component {


    private static Logger logger = Logger.getLogger(RabbitClient.class.getName());

    //---------------------------------------------
    // parms


    private String userName;
    private String password;
    private String host;
    private String virtualHost;
    private int port;
    private boolean createQueue;
    private String queue;
    private String exchange;
    private int fileRecLimit;
    private int fileTimeLimitSecs;
    private boolean includeHeader;
    private int checkIntervalMillis;
    private boolean ssl;






    //----------------------------------------------
    // variables


    private Channel ch;
    private com.rabbitmq.client.Connection conn;

    private Properties fileProps;

    // folders
    private File targetFolderLocation;
    private File workFileLocation;
    private File loadFileLocation;
    private static String pathSep = "\\";


    private boolean decompress;

    //files
    private File workFile;
    private File loadFile;


    // operational variables
    private AtomicLong fileStart;
    private AtomicLong fileNumber;
    private AtomicLong recCount;
    private AtomicBoolean fileDone;
    private AtomicBoolean messageDone;
    private AtomicBoolean wrapUp;

    private AtomicInteger periodRecordCount;
    private AtomicInteger periodFileCount;
    private AtomicLong periodStart;


    private String routingKey;

    private String currentFileName;


    // writer stuff
    private BufferedWriter bw;      // writer for data records
    private ScheduledExecutorService ses;
    private ScheduledFuture<?> fileWriteJob;







    public RabbitClient(Properties fileProps) {
        this.fileProps = fileProps;
    }




    public void runComponent() {

        try {


            logger.info(LoggingUtils.getLogEntry("Running component: " + RabbitClient.class.getName(), jobName, jobId, groupId));

            ConnectToRabbit();



            //set the file locations
            targetFolderLocation = new File(fileProps.getProperty(Main.fpLoadFolderKW) + pathSep + prefix);
            workFileLocation = new File(targetFolderLocation + pathSep + JobManager.componentWorkFolderNameKW);
            loadFileLocation = new File(targetFolderLocation + pathSep + JobManager.componentLoadFolderNameKW);

            LoadFolders.checkLoadFolders(fileProps, prefix);


            fileNumber = new AtomicLong(0);
            fileStart = new AtomicLong(0);
            recCount = new AtomicLong(0);
            fileDone = new AtomicBoolean(false);
            messageDone = new AtomicBoolean(false);
            wrapUp = new AtomicBoolean(false);

            // initialize period metrics
            periodFileCount = new AtomicInteger(0);
            periodRecordCount = new AtomicInteger(0);
            periodStart = new AtomicLong(new Date().getTime());



            // start a push rabbit consumer
            ch.basicConsume(queue, false, new deliverCallback(), consumerTag -> {});


            // start up monitoring job and leave it running
            ses = Executors.newScheduledThreadPool(1);
            fileWriteJob = ses.scheduleAtFixedRate(new FileCheckJob(), checkIntervalMillis, checkIntervalMillis, TimeUnit.MILLISECONDS);


        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, jobName, jobId, groupId));
            killComponent();
        }



    }








    public boolean loadComponent(JSONObject runParms, String context) {


        try {


            JSONArray pa = runParms.getJSONArray("Parameters");


            for (int i = 0; i < pa.length(); i++) {

                JSONObject curParm = pa.getJSONObject(i);
                String parmName = (String) curParm.names().get(0);

                switch (parmName) {

                    
                    case "userName":
                        userName = (String) getJSONValue(curParm, context, Component.stringType, parmName);
                        break;
                    case "password":
                        password = (String) getJSONValue(curParm, context, Component.stringType, parmName);
                        break;
                    case "host":
                        host = (String) getJSONValue(curParm, context, Component.stringType, parmName);
                        break;
                    case "virtualHost":
                        virtualHost = (String) getJSONValue(curParm, context, Component.stringType, parmName);
                        break;
                    case "port":
                        port = (Integer) getJSONValue(curParm, context, Component.intType, parmName);
                        break;
                    case "queue":
                        queue = (String) getJSONValue(curParm, context, Component.stringType, parmName);
                        break;
                    case "exchange":
                        exchange = curParm.getJSONObject(parmName).isNull(context) ? null :  (String) getJSONValue(curParm, context, Component.stringType, parmName);
                        break;
                    case "createQueue":
                        createQueue = (Boolean) getJSONValue(curParm, context, Component.booleanType, parmName);
                        break;
                    case "fileRecLimit":
                        fileRecLimit = (Integer) getJSONValue(curParm, context, Component.intType, parmName);
                        break;
                    case "fileTimeLimitSecs":
                        fileTimeLimitSecs = (Integer) getJSONValue(curParm, context, Component.intType, parmName);
                        break;
                    case "includeHeader":
                        includeHeader = (Boolean) getJSONValue(curParm, context, Component.booleanType, parmName);
                        break;
                    case "checkIntervalMillis":
                        checkIntervalMillis = (Integer) getJSONValue(curParm, context, Component.intType, parmName);
                        break;
                    case "ssl":
                        ssl = (Boolean) getJSONValue(curParm, context, Component.booleanType, parmName);
                        break;
                    case "routingKey":
                        routingKey =  curParm.getJSONObject(parmName).isNull(context) ? null :  (String) getJSONValue(curParm, context, Component.stringType, parmName);
                        break;
                    case "decompress":
                        decompress = curParm.getJSONObject(parmName).isNull(context) ? false :  (Boolean) getJSONValue(curParm, context, Component.booleanType, parmName);
                        break;
                    default:
                        throw new Exception("Unable to find value for parameter of: " + parmName);

                } // end switch

            } // end iterating over the map






            return true;

        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, jobName, jobId, groupId));
            killComponent();
            return false;
        }



    }





    public void closeComponent() {


        try {

            ShutdownExecutor.shutdownSES(ses, jobName, jobId, groupId);

            // shut everything down


            if ( !(ch == null)) {
                ch.close();
            }

            if ( !(conn == null) ) {
                conn.close();
            }


            if ( !(bw == null) ) {
                bw.close();
            }



        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, jobName, jobId, groupId));
        }

    }







    public void ConnectToRabbit() {


        try {


            logger.info(LoggingUtils.getLogEntry("Connecting to Rabbit broker", jobName, jobId, groupId));


            ConnectionFactory factory = new ConnectionFactory();
            factory.setUsername(userName);
            factory.setPassword(password);
            factory.setHost(host);
            factory.setVirtualHost(virtualHost);
            factory.setPort(port);

            if (ssl) {
                logger.info(LoggingUtils.getLogEntry("Using SSL", jobName, jobId, groupId));
                factory.useSslProtocol();  // this is ok for dev stuff, not for full on production
            }

            conn = factory.newConnection();
            ch = conn.createChannel();


            logger.info(LoggingUtils.getLogEntry("Create queue: " + createQueue, jobName, jobId, groupId));

            if (createQueue) {

                ch.queueDeclare(queue, false, true, true, null);

                String routingKeyOut;
                if (routingKey != null) {
                    routingKeyOut = routingKey;
                } else {
                    routingKeyOut = "#";
                }

                ch.queueBind(queue, exchange, routingKeyOut);

            }

            logger.info(LoggingUtils.getLogEntry("Queue message count: " + ch.messageCount(queue), jobName, jobId, groupId));

        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, jobName, jobId, groupId));
            killComponent();
        }



    }


    /**
     * This inner class handles the deliverCallback for a received broker message
     */
    class deliverCallback implements DeliverCallback {


        @Override
        public void handle(String ct, Delivery delivery) {


            try {

                while (wrapUp.get() == true) {
                    // do not process messages while wrapping up a file
                }

                messageDone.set(false);



                //---------------------------------------------------------------------------------------
                // decompress the message if needed and output a string

                String body = null;

                if (decompress) {

                    InputStreamReader isr = new InputStreamReader(new GZIPInputStream(new ByteArrayInputStream(delivery.getBody())), StandardCharsets.UTF_8);
                    StringWriter sw = new StringWriter();
                    char[] chars = new char[1024];
                    for (int len; (len = isr.read(chars)) > 0; ) {
                        sw.write(chars, 0, len);
                    }
                    isr.close();

                    body = sw.toString();

                    //System.out.println(sw.toString());

                } else {

                    body = new String(delivery.getBody(), "UTF-8");

                }


                JSONObject j = so.standardRecord("R", prefix, body, false);

                if (includeHeader == true) {
                    j.put("properties", delivery.getProperties().toString());
                }

                long Tag = delivery.getEnvelope().getDeliveryTag();



                // check to see if a new data file is needed
                if (recCount.get() == 0) {

                    StartOutputFile();
                    bw.write("[" + j.toString() );

                } else {
                    bw.write("," + j.toString() );
                }

                j = null;  // null the JSON object in case they are big so it is not carried in memory

                // flush the buffer to make sure the entire record gets written down and increment the
                // the counter and then ack the message
                bw.flush();
                recCount.incrementAndGet();
                periodRecordCount.incrementAndGet();
                ch.basicAck(Tag, false);
                messageDone.set(true);

            } catch (IOException e) {
                logger.severe(LoggingUtils.getErrorEntry(e, jobName, jobId, groupId));
                killComponent();
            }


        }




    }








    //-----------------------------------------------
    // this starts a new file for output

    private void StartOutputFile() throws IOException {


        try {


            // set the date and file number
            fileStart.set(new Date().getTime());
            fileNumber.incrementAndGet();
            fileDone.set(false);


            // this is really just a tie breaker in case a two files get created at the same millisecond
            if (fileNumber.get() == 100) {
                fileNumber.set(1);
            }


            // create the file and open up the buffered writer
            currentFileName = so.standardFileName(prefix, "R");
            workFile = new File(workFileLocation + pathSep + currentFileName);
            loadFile = new File(loadFileLocation + pathSep + currentFileName);

            bw = new BufferedWriter(new FileWriter(workFile));


        } catch (IOException e) {
            logger.severe(LoggingUtils.getErrorEntry(e, jobName, jobId, groupId));
            killComponent();
        }


    }














    //--------------------------------------------------------------------------
    // inner class for timer task, that way I can make use of the
    // class variables :)


    /**
     * This inner class checks in progress files to see if they should wrap up and runs on a schedule passed in on the job
     */
    class FileCheckJob implements Runnable {



        @Override
        public void run() {


            try {

                // check the data time and record counts to see if the
                // file needs to be wrapped up

                Long curr = new Date().getTime();
                Long dataDiff = 0L;
                Long periodDiff = curr - periodStart.get();
                int periodMax = 5 * 1000 * 60;

                //System.out.println(periodDiff);


                if (periodDiff >= periodMax) {

                    support.firePropertyChange(getPropertyChangeMetricId(Metrics.recordCount.toString()), 0, periodRecordCount.get());
                    support.firePropertyChange(getPropertyChangeMetricId(Metrics.fileCount.toString()), 0, periodFileCount.get());
                    periodRecordCount.set(0);
                    periodFileCount.set(0);
                    periodStart.set(new Date().getTime());

                }




                if (recCount.get() > 0) {


                    dataDiff = curr - fileStart.get();


                    if ( dataDiff >=  (fileTimeLimitSecs * 1000) ) {
                        fileDone.set(true);
                    }


                    if (recCount.get() >= fileRecLimit) {
                        fileDone.set(true);
                    }



                    if (fileDone.get()) {
                        WrapUpFile();
                    }


                }


            } catch (Exception e) {
                logger.severe(LoggingUtils.getErrorEntry(e, jobName, jobId, groupId));
                killComponent();
            }


        }




    }







    //-----------------------------------------------
    // close out the file and move it to the load location
    // and put together the file metrics to load into Snowflake


    private void WrapUpFile() {


        try {



            // calculate RPS records per second that were processed  recCount / ( diff / 1000 )
            Long cd = new Date().getTime();
            float RPS = ( (float) recCount.get() ) / ( (cd - fileStart.get()) / 1000);


            //logger.info("Wrapping up file | RPS: " + RPS + " | QueueMessageCount: " + ch.messageCount(queue));
            wrapUp.set(true);


            // wait for the thread writing to the file is done writing its current record and then close out the file
            while (messageDone.get() == false ) {
                // do nothing
            }


            bw.write("]");
            bw.close();





            // move the file to the load folder as an atomic operation so that jobs picking up the files will be able to access it when it shows up
            Files.move( Paths.get( workFile.getPath() ),  Paths.get( loadFile.getPath() ), StandardCopyOption.ATOMIC_MOVE);

            //WriteFileMetrics(false);


            //----------------------------------------------------------------
            // wrap up

            wrapUp.set(false); // set wrap to done
            recCount.set(0); // reset the record count; a new file will be created
            periodFileCount.incrementAndGet();

            //System.out.println("Done wrapping up");

        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, jobName, jobId, groupId));
            killComponent();
        }





    }







    @Override
    public ArrayList<String> reportDetails() {


        ArrayList<String> a = allCompReportDetails();

        addDetailString("userName", userName, a);
        addDetailString("password", password, a);
        addDetailString("host", host, a);
        addDetailString("virtualHost", virtualHost, a);
        addDetailString("queue", queue, a);
        addDetailString("exchange", exchange, a);
        addDetailString("port", String.valueOf(port), a);
        addDetailString("createQueue", String.valueOf(createQueue), a);
        addDetailString("fileRecLimit", String.valueOf(fileRecLimit), a);
        addDetailString("fileTimeLimitSecs", String.valueOf(fileTimeLimitSecs), a);
        addDetailString("includeHeader", String.valueOf(includeHeader), a);
        addDetailString("checkIntervalMillis", String.valueOf(checkIntervalMillis), a);
        addDetailString("ssl", String.valueOf(ssl), a);
        addDetailString("decompress", String.valueOf(decompress), a);
        addDetailString("routingKey", routingKey, a);

        return a;


    }





}
