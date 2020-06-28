package com.kuaishou.kcode.model;

import java.util.HashMap;
import java.util.Map;

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

    public void mergeContainer(Range2MessageContainer other) {
        for (Map.Entry<String, HashMap<String, Range2Result>> methodEntry :
                other.range2Map.entrySet()) {

            String methodName = methodEntry.getKey();

            if(!range2Map.containsKey(methodName)) {
                range2Map.put(methodName, methodEntry.getValue());
                continue;
            }
            HashMap<String, Range2Result> myIPMap = range2Map.get(methodName);

            for (Map.Entry<String, Range2Result> ipEntry :
                    methodEntry.getValue().entrySet()) {

                String ip = ipEntry.getKey();
                if(!myIPMap.containsKey(ip)) {
                    myIPMap.put(ip, ipEntry.getValue());
                } else {
                    myIPMap.get(ip).mergeResult(ipEntry.getValue());
                }

            }


        }
    }

}
