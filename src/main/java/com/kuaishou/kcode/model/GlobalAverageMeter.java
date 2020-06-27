package com.kuaishou.kcode.model;

import java.util.HashMap;
import java.util.Map;

public class GlobalAverageMeter {

    private class AverageMeter {
        private double total;
        private int n;
        private long val;
        private double average;
        private boolean isStarted;

        private long startTimeStamp = 0;

        public AverageMeter() {
            reset();
        }

        public void reset() {
            total = 0;
            n = 0;
            val = 0;
            average = 0;
            isStarted = false;

            startTimeStamp = 0L;
        }

        public void startTimer() {
            isStarted = true;
            startTimeStamp = System.currentTimeMillis();
        }

        public void updateTimer() {
            long end = System.currentTimeMillis();
            Update(end - startTimeStamp);
            startTimeStamp = end;
        }

        public void UpdateStart() {
            startTimeStamp = System.currentTimeMillis();
        }

        public void Update(long val) {
            total += val;
            n += 1;
            average = total / n;
            this.val = val;
        }

        
        public double getTotal() {
            return total;
        }

        public int getN() {
            return n;
        }

        public long getVal() {
            return val;
        }

        public double getAverage() {
            return average;
        }

        public boolean isStarted() {
            return isStarted;
        }

        @Override
        public String toString() {
            return String.format("Average %.7f, TotalNumber %d,Total Time %f", average, n, total);
        }
    }

    private Map<String, AverageMeter> timers ;

    public GlobalAverageMeter() {
        timers = new HashMap<>();
    }

    public void createTimer(String timerName) {
        timers.putIfAbsent(timerName, new AverageMeter());
    }

    public void resetTimer(String timerName) {
        timers.get(timerName).reset();
    }

    public void updateTimer(String timerName) {
        timers.get(timerName).updateTimer();
    }

    public void startTimer(String timerName) {
        timers.get(timerName).startTimer();
    }

    public void updateTimerStart(String timerName) {
        timers.get(timerName).UpdateStart();
    }

    public boolean isTimerStarted(String timerName) {
        return timers.get(timerName).isStarted();
    }

    public String getStatisticString() {
        String formatString = "%s\n";
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<String, AverageMeter> entry:
            timers.entrySet()){
            builder.append(String.format(formatString, entry.getKey()));
            builder.append(String.format(formatString, entry.getValue().toString()));
        }

        return builder.toString();
    }


    public void getStatistic() throws Exception {

        throw new Exception(getStatisticString());
    }

    public void getStatistic(String appendMsg) throws Exception {

        throw new Exception(getStatisticString()+appendMsg+"\n");
    }

}
