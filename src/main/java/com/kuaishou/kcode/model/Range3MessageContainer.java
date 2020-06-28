package com.kuaishou.kcode.model;


import java.util.HashMap;
import java.util.Map;

public class Range3MessageContainer {
    private final int minuteTimeStamp;

    private HashMap<String, SuccessRate>  successRateMap;

    public int getMinuteTimeStamp() {
        return minuteTimeStamp;
    }

    public HashMap<String, SuccessRate> getSuccessRateMap() {
        return successRateMap;
    }

    public Range3MessageContainer(int currentTimeStamp) {
        minuteTimeStamp = currentTimeStamp;
        successRateMap = new HashMap<>();
    }

    public void addMessage(Message msg) {
        String calledService = msg.getCalledService();
        successRateMap.putIfAbsent(calledService, new SuccessRate());
        SuccessRate successRate = successRateMap.get(calledService);
        if(msg.isSuccess()) {
            successRate.success.incrementAndGet();
        }
        successRate.total.incrementAndGet();
    }

    public void mergeContainer(Range3MessageContainer container) {
        HashMap<String, SuccessRate> otherSuccessateMap = container.getSuccessRateMap();
        for (Map.Entry<String, SuccessRate> calledEntry:
        otherSuccessateMap.entrySet()){
            if(successRateMap.containsKey(calledEntry.getKey())) {
                successRateMap.get(calledEntry.getKey()).mergeSuccessate(calledEntry.getValue());
            } else {
                successRateMap.put(calledEntry.getKey(), calledEntry.getValue());
            }
        }
    }
}
