package com.ELTTool;

import org.json.JSONObject;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;


/**
 *This is used to standardize records and file names.  No error handling here to it always pushing
 * error up to the calling class
 */
public class StandardObjects {



    private AtomicLong fileNumber;
    private AtomicLong fileListNumber;

    private SimpleDateFormat rddf;

    private UUID crossMessageId;
    private String crossTimestamp;



    public StandardObjects() {
        fileNumber = new AtomicLong(0);
        fileListNumber = new AtomicLong(0);
        rddf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    }




    //----------------------------------------------------------------------------------------------------
    // this will produce a standard record for a file


    public JSONObject standardRecord(String source, String prefix, String data, boolean rowDate) {


        String ts = rddf.format(new Date()) + "Z";

        JSONObject jo = new JSONObject();
        JSONObject djo;

        if (rowDate) {
            djo = new JSONObject();
            djo.put("rowDate", ts);
            djo.put("operation", "update");
            djo.put("payload", new JSONObject(data));
        } else {
            djo = new JSONObject(data);
        }


        jo.put("uniqueId", prefix + "_" + UUID.randomUUID());
        jo.put("recordSource", source);
        jo.put("receivedTimestamp", ts);
        jo.put("data", djo);


        return  jo;

    }






    
    public JSONObject standardRecord(String source, String prefix, String data, boolean rowDate, JSONObject parms, boolean crossMessage) {


        JSONObject jo = new JSONObject();
        JSONObject djo;

        if (rowDate) {
            djo = new JSONObject();
            djo.put("rowDate", (crossMessage ? crossTimestamp : rddf.format(new Date()) + "Z") );
            djo.put("operation", "update");
            djo.put("payload", new JSONObject(data));
        } else {
            djo = new JSONObject(data);
        }


        jo.put("uniqueId", prefix + "_" + (crossMessage ? crossMessageId : UUID.randomUUID()) );
        jo.put("recordSource", source);
        jo.put("receivedTimestamp", (crossMessage ? crossTimestamp : rddf.format(new Date()) + "Z") );
        jo.put("parameters", parms);
        jo.put("data", djo);


        return  jo;

    }








    public void setCrossMessageIds() {

        crossMessageId = UUID.randomUUID();
        crossTimestamp = rddf.format(new Date()) + "Z";


    }





    // -----------------------------------------------------------------------------------------
    // use this to get a standard file name on a record producer component ...


    public String standardFileName(String prefix, String source) {


        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd.HHmmss.SSS");
        return prefix + "_" + source + "_" + sdf.format(new Date()) + "_" + fileNumber.getAndIncrement() + ".txt";

    }







}
