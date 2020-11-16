package com.ELTTool;



import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.json.JSONObject;
import java.util.logging.Logger;






public class S3 extends Connection {



    private static Logger logger = Logger.getLogger(S3.class.getName());

    private AmazonS3 s3;
    private BasicAWSCredentials creds;

    private String accessKeyId;
    private String secretKey;



    public S3(JSONObject parms, String jobName) {

        this.jobName = jobName;
        connectionType = ConnectionTypes.S3;
        connectionRef = parms.getString(connKW.Reference.toString());

        accessKeyId = parms.getString(connKW.KeyId.toString());
        secretKey = parms.getString(connKW.SecretKey.toString());
        creds = new BasicAWSCredentials(accessKeyId, secretKey);

    }







    public AmazonS3 getS3() { return s3; }
    public String getConnectionRef() { return connectionRef; }
    public String getAccessKeyId() { return accessKeyId; }
    public String getSecretKey() { return secretKey; }



    public void startConnection (int statementTimeout, Long jobId, Long groupId) throws Exception {


        try {

            this.jobId = jobId;
            this.groupId = groupId;

            AmazonS3ClientBuilder s3b = AmazonS3ClientBuilder.standard();

            s3 = s3b.withRegion(Regions.US_EAST_1)
                    .withCredentials(new AWSStaticCredentialsProvider(creds))
                        .build();


        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, jobName, jobId, groupId));
        }




    }




    public void shutdownConnection() throws Exception {

        if (s3 != null)
            s3.shutdown();

    }



}
