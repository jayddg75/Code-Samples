package com.ELTTool;




public class Member {

    private String jobName;
    private boolean wait;
    private int order;



    public Member(String jobName, boolean wait, int order){
        this.jobName = jobName;
        this.wait = wait;
        this.order = order;
    }



    public String getJobName() {
        return jobName;
    }

    public boolean getWait() {
        return wait;
    }

    public int getOrder() {
        return order;
    }




}
