package com.ELTTool;




/**
 * This is the enum for the statuses
 */
public enum Status {

    All("All", 99),
    Error("Error", -1),
    Stopped("Stopped", 0),
    Starting("Starting", 1),
    Running("Running", 2),
    Completed("Completed", 3);



    private String statusDesc;
    private int statusCode;
    private static final int noCode = -99;

    Status(final String statusDesc, int statusCode) {
        this.statusDesc = statusDesc;
        this.statusCode = statusCode;
    }



    public String getStatusDesc() { return statusDesc; }
    public int getStatusCode() { return statusCode; }





    //-----------------------------------------------
    // provide 2-way status lookups


    public static int getCodeFromDesc(String statusDesc) throws Exception {

        int outCode = noCode;
        for (Status s : Status.values())
            if (s.getStatusDesc().equals(statusDesc))
                outCode = s.getStatusCode();

        if (outCode == noCode)
            throw new Exception("Code for status desc not found: " + statusDesc);

        return outCode;

    }



    public static String getDescFromCode(int statusCode) throws Exception {

        String outDesc = null;

        for (Status s : Status.values())
            if (s.getStatusCode() == statusCode)
                outDesc = s.getStatusDesc();

        if (outDesc == null)
            throw new Exception("Description for status code not found: " + statusCode);

        return outDesc;

    }


    public static String[] getAllStatusDesc() {

        String[] out = new String[Status.values().length];
        int i = 0;
        for(Status s : Status.values()) {
            out[i] = s.getStatusDesc();
            i++;
        }


        return out;
    }



}
