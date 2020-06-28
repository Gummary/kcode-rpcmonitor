package com.kuaishou.kcode.model;

public class Message {

    private String mainService;
    private String calledService;
    private String mainIp;
    private String calledIP;
    private boolean isSuccess;
    private int responseTime;
    private int minuteTimeStamp;

    public Message(String mainService, String calledService, String mainIp, String calledIP, boolean isSuccess, int responseTime, int minuteTimeStamp) {
        this.mainService = mainService;
        this.calledService = calledService;
        this.mainIp = mainIp;
        this.calledIP = calledIP;
        this.isSuccess = isSuccess;
        this.responseTime = responseTime;
        this.minuteTimeStamp = minuteTimeStamp;
    }

    public String getMainService() {
        return mainService;
    }

    public String getCalledService() {
        return calledService;
    }

    public String getMainIp() {
        return mainIp;
    }

    public String getCalledIP() {
        return calledIP;
    }

    public boolean isSuccess() {
        return isSuccess;
    }

    public int getResponseTime() {
        return responseTime;
    }

    public int getMinuteTimeStamp() {
        return minuteTimeStamp;
    }
}
