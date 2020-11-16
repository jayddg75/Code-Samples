package com.ELTTool;



import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.util.Properties;
import java.util.logging.Logger;



/**
 *This manages all connections and related activities; loading, getting, listing, etc ...
 */
public class ConnectionManager {



    private static Logger logger = Logger.getLogger(ConnectionManager.class.getName());

    private JSONArray connectionInfo;
    private String localConnectionsFile;




    public ConnectionManager(Properties fileProps) {

            localConnectionsFile = fileProps.getProperty(Main.fpOpFolderKW) + "\\Connections.json";

    }







    public DBConnection getDBConnection(String ref, Connection.DBTypes type, String job, String component, Long jobId, Long groupId) throws Exception {

        logger.info(LoggingUtils.getLogEntry("Getting connection for ref: " + ref + " type: " + type, job, jobId, groupId));

        DBConnection d = null;

        for (Object o : connectionInfo ) {

            JSONObject jo = (JSONObject) o;
            //logger.info(jo.toString());

            // check to see if it is a db connection
            if ( Connection.ConnectionTypes.valueOf(jo.getString(Connection.connKW.Type.toString())) == Connection.ConnectionTypes.Database ) {


                Connection.DBTypes curType = Connection.DBTypes.valueOf(jo.getString(Connection.connKW.DBType.toString()));
                String curRef = jo.getString(Connection.connKW.Reference.toString());

                if (curType == type && curRef.equals(ref)) {

                    switch (type) {

                        case Snowflake:
                            d = new Snowflake(jo, job, component);
                            break;
                        case SQLServer:
                            d = new SQLServer(jo, job, component);
                            break;

                    }


                }



            }






        }

        if (d == null) {
            throw new Exception("Database connection not found for ref: " + ref + " type: " + type);
        }

        return d;

    }






    public S3 getS3Connection(String ref, String jobName) throws Exception {


            S3 s = null;

            for (Object o : connectionInfo ) {

                JSONObject jo = (JSONObject) o;

                if ( Connection.ConnectionTypes.valueOf(jo.getString(Connection.connKW.Type.toString())) == Connection.ConnectionTypes.S3 ) {

                    String curRef = jo.getString(Connection.connKW.Reference.toString());

                    if (curRef.equals(ref))
                        s = new S3(jo, jobName);

                }



            }

            if (s == null)
                throw  new Exception("Unable to find S3 ref: " + ref);

            return s;


    }





    public Mongo getMongoConnection(String ref) throws Exception {

        Mongo m = null;

        for (Object o : connectionInfo ) {

            JSONObject jo = (JSONObject) o;

            if ( Connection.ConnectionTypes.valueOf(jo.getString(Connection.connKW.Type.toString())) == Connection.ConnectionTypes.Mongo ) {

                String curRef = jo.getString(Connection.connKW.Reference.toString());

                if (curRef.equals(ref)) {

                    String uri = jo.getString(Connection.connKW.URI.toString());
                    m = new Mongo(uri, "Metrics", null, null, null);

                }

            }

        }


        if (m == null)
            throw new Exception("Unable to find Mongo ref: " + ref);

        return m;


    }








    public void loadConnections() throws IOException {


            logger.info("Loading all connections");

            BufferedReader br = new BufferedReader(new FileReader(localConnectionsFile));
            StringBuilder sb = new StringBuilder();

            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }

            connectionInfo = new JSONArray(sb.toString());

            br.close();


    }











}
