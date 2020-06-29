package com.kuaishou.kcode.handler;

import com.kuaishou.kcode.model.Range3MessageContainer;
import com.kuaishou.kcode.model.Range3Result;
import com.kuaishou.kcode.model.SuccessRate;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class Range3ResultCalculator implements Runnable {

    private LinkedBlockingQueue<Range3MessageContainer> range3MessageContainers;
    private HashMap<String, Range3Result> range3Result;

    private int currentTimestamp = -1;
    private Range3MessageContainer currentContainer;

    public Range3ResultCalculator(LinkedBlockingQueue<Range3MessageContainer> range3MessageContainers, HashMap<String, Range3Result> range3Result) {
        this.range3MessageContainers = range3MessageContainers;
        this.range3Result = range3Result;
    }

    private void calculateResult() {
        if(currentTimestamp == -1) {
            return;
        }

        HashMap<String, SuccessRate> successRateMap = currentContainer.getSuccessRateMap();

        System.out.println("Range 3 got " + currentTimestamp);

        for (Map.Entry<String, SuccessRate> entry :
                successRateMap.entrySet()) {
            range3Result.putIfAbsent(entry.getKey(), new Range3Result());
            Range3Result result = range3Result.get(entry.getKey());

            SuccessRate successRate = entry.getValue();
            double rate = (double) successRate.success.get() / successRate.total.get();
            result.addTimeStampSuccessate(currentTimestamp, rate);
        }
    }


    @Override
    public void run() {
        while(true) {
            Range3MessageContainer container = null;
            try {
                container = range3MessageContainers.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            assert container != null;
            if(container.getMinuteTimeStamp() == -1 &&
                    container.getSuccessRateMap().size() == 0) { // no more data
                System.out.println(String.format("%d Got Last block", System.currentTimeMillis()));
                calculateResult();
                for (Range3Result result :
                        range3Result.values()) {
                    result.calculatePrefixSum();
                }
                System.out.println(String.format("%d :Range3 done", System.currentTimeMillis()));
                break;
            }

            int timeStamp = container.getMinuteTimeStamp();
            if(timeStamp != currentTimestamp) {
                calculateResult();
                currentTimestamp = timeStamp;
                currentContainer = container;
                continue;
            }
            currentContainer.mergeContainer(container);
        }
    }
}
