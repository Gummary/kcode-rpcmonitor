package com.kuaishou.kcode.model;

public class Range3Result implements Comparable{
    private String timeStamp;
    private double successRate;

    public String getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(String timeStamp) {
        this.timeStamp = timeStamp;
    }

    public double getSuccessRate() {
        return successRate;
    }

    public void setSuccessRate(double successRate) {
        this.successRate = successRate;
    }

    public void MergeResult(Range3Result result) {
    }


    public Range3Result(String timeStamp, double successRate) {
        this.timeStamp = timeStamp;
        this.successRate = successRate;
    }

    @Override
    public int compareTo(Object o) {
        Range3Result that = (Range3Result)o;
        return this.timeStamp.compareTo(that.timeStamp);
    }
}
