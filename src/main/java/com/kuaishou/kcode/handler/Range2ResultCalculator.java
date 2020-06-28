package com.kuaishou.kcode.handler;

import com.kuaishou.kcode.model.*;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class Range2ResultCalculator implements Runnable {

    private final DecimalFormat format;
    // TimeStamp, mainservice calledservice
    private HashMap<Integer, HashMap<String, ArrayList<String>>> range2ResultMap;
    private LinkedBlockingQueue<Range2MessageContainer> range2MessageContainerQueue;
    private final StringBuilder resultBuilder = new StringBuilder();

    private int currentTimeStamp = -1;
    private Range2MessageContainer currentContainer;


    public Range2ResultCalculator(LinkedBlockingQueue<Range2MessageContainer> queue,
                                  HashMap<Integer, HashMap<String, ArrayList<String>>> range2ResultMap) {
        range2MessageContainerQueue = queue;
        this.range2ResultMap = range2ResultMap;

        format = new DecimalFormat("#.00");
        format.setRoundingMode(RoundingMode.DOWN);

    }

    private void calculateResult() {

        if(currentTimeStamp == -1) {
            return;
        }

        HashMap<String, HashMap<String, Range2Result>> functionMap = currentContainer.getRange2Map();
        System.out.println("Range 2 " + currentTimeStamp);

        range2ResultMap.putIfAbsent(currentTimeStamp, new HashMap<>());
        HashMap<String, ArrayList<String>> timestampMap = range2ResultMap.get(currentTimeStamp);

        for (Map.Entry<String, HashMap<String, Range2Result>> node : functionMap.entrySet()) {
            // mainService calledService
            String key = node.getKey();
            HashMap<String, Range2Result> valueMap = node.getValue();
            Iterator<Map.Entry<String, Range2Result>> resultIterator = valueMap.entrySet().iterator();
            ArrayList<String> resultList = new ArrayList<>();
            while (resultIterator.hasNext()) {
                Range2Result resultNode = resultIterator.next().getValue();
                resultBuilder.setLength(0);
                resultBuilder.append(resultNode.mainIP).append(",")
                        .append(resultNode.calledIP).append(",")
                        .append(resultNode.computeSuccessRate(format)).append(",")
                        .append(resultNode.computeP99());
                resultList.add(resultBuilder.toString());
            }
//                        String date = DateUtils.minuteTimeStampToDate(workMinuteStamp);
            timestampMap.put(key, resultList);
//                        computedRange2Result.put(key + date, resultList);
        }

    }



    @Override
    public void run() {
        while (true) {
            try {
                Range2MessageContainer range2MessageContainer = range2MessageContainerQueue.take();

                if(range2MessageContainer.getMinuteTimeStamp() == -1 &&
                        range2MessageContainer.getRange2Map().size() == 0) {  // no more data
                    System.out.println("Range2 done");
                    break;
                }

                int timeStamp = range2MessageContainer.getMinuteTimeStamp();
                if(timeStamp != currentTimeStamp) {
                    calculateResult();
                    currentTimeStamp = timeStamp;
                    currentContainer = range2MessageContainer;
                    continue;
                }
                currentContainer.mergeContainer(range2MessageContainer);


            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
