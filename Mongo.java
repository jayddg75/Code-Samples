package com.ELTTool;



import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.*;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.FindIterable;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Filters;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.*;
import com.mongodb.client.model.Sorts;

import org.bson.Document;

import java.util.ArrayList;
import java.util.logging.Logger;




public class Mongo extends Connection {


    private static Logger logger = Logger.getLogger(Tree.class.getName());

    private MongoClient mc;
    private MongoDatabase db;

    private String uri;
    private String dbName;





    public Mongo (String uri, String dbName, String jobName, Long jobId, Long groupId) {
        this.uri = uri;
        this.dbName = dbName;
        this.jobName = jobName;
        this.jobId = jobId;
        this.groupId = groupId;
    }





    @Override
    public void startConnection(int statementTimeout, Long jobId, Long groupId) throws Exception {

        String mes = "Starting MongoDB connection db: " + dbName + " | uri: " + uri;
        logger.info(LoggingUtils.getLogEntry(mes, jobName, jobId, groupId));


        // set the connection string
        ConnectionString cs = new ConnectionString(uri);


        /*
        // set the default codec registry to handle POJO
        CodecRegistry pojoCodecRegistry = fromRegistries(MongoClientSettings.getDefaultCodecRegistry(),
                fromProviders(PojoCodecProvider.builder().automatic(true).build()));
        */


            // create the settings
            MongoClientSettings settings = MongoClientSettings.builder()
                    .applyConnectionString(cs)
                    //.codecRegistry(pojoCodecRegistry)
                    .applicationName("ELTTool")
                    .build();


            // create the client with the settings
            mc = MongoClients.create(settings);

            // set the database
            db = mc.getDatabase(dbName);


    }






    @Override
    public void shutdownConnection() throws Exception {

        String mes = "Closing MongoDB connection db: " + dbName + " | uri: " + uri;
        logger.info(LoggingUtils.getLogEntry(mes, jobName, jobId, groupId));

        if(mc != null)
            mc.close();

    }




    public void insertDocument(Document d, String collection) throws Exception {
        db.getCollection(collection).insertOne(d);
    }


    public void insertDocuments(ArrayList<Document> dList, String collection) throws Exception {
        db.getCollection(collection).insertMany(dList);
    }



    public FindIterable getActivity(Long jobId) {


        return db.getCollection("Metrics").find(and( eq("jobId", jobId), ne("metricName", "status") ))
                                        .projection(include("metricName", "metricValue", "metricTimestamp", "component", "order"))
                                        .sort(Sorts.descending("metricTimestamp"));


    }



}
