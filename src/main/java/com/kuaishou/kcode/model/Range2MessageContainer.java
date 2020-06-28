package com.kuaishou.kcode.model;

import java.util.HashMap;

public class Range2MessageContainer {

    private HashMap<String, HashMap<String, Range2Result>> range2Map;

    public HashMap<String, HashMap<String, Range2Result>> getRange2Map() {
        return range2Map;
    }

    public int getMinuteTimeStamp() {
        return minuteTimeStamp;
    }

    private int minuteTimeStamp;

    public Range2MessageContainer(int minuteTimeStamp) {
        this.minuteTimeStamp = minuteTimeStamp;
        range2Map = new HashMap<>();
    }

    public void addMessage(Message msg) {
        String service = msg.getMainService() + msg.getCalledService();
        String ip = msg.getMainIp() + msg.getCalledIP();

        range2Map.putIfAbsent(service, new HashMap<>());
        HashMap<String, Range2Result> ipMap = range2Map.get(service);

        ipMap.putIfAbsent(ip, new Range2Result(msg.getMainIp(), msg.getCalledIP()));
        Range2Result range2Result = ipMap.get(ip);
        range2Result.fillMessage(msg.isSuccess(), msg.getResponseTime());
    }

}
