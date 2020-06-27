package com.kuaishou.kcode.model;

import java.util.HashMap;

public class MinuteMessageContainer {

    public int getMinuteTimeStamp() {
        return minuteTimeStamp;
    }

    private int minuteTimeStamp;

    public HashMap<String, HashMap<String, Range2Result>> getMessages() {
        return messages;
    }

    private HashMap<String, HashMap<String, Range2Result>> messages;

    public MinuteMessageContainer(int minuteTimeStamp, HashMap<String, HashMap<String, Range2Result>> messages) {
        this.minuteTimeStamp = minuteTimeStamp;
        this.messages = messages;
    }
}
