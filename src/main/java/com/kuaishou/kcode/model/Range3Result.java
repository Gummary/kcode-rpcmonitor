package com.kuaishou.kcode.model;

import java.util.HashMap;

public class Range3Result {

    private class PrefixSumNode {
        private double prefixSumSuccessate;
        private int prefixSumCount;

        public void addSuccessate(double successate) {
            prefixSumCount += 1;
            prefixSumSuccessate += successate;
        }

        public PrefixSumNode() {
            prefixSumSuccessate = 0.0;
            prefixSumCount = 0;
        }

        public PrefixSumNode(PrefixSumNode node) {
            this.prefixSumCount = node.prefixSumCount;
            this.prefixSumSuccessate = node.prefixSumSuccessate;
        }

        public PrefixSumNode copy() {
            return new PrefixSumNode(this);
        }
    }

    private int startTimeStamp = Integer.MAX_VALUE;
    private int endTimeStamp = Integer.MIN_VALUE;

    private HashMap<Integer, Double> timestampSuccessate = new HashMap<>();
    private PrefixSumNode[] nodes = null;


    public void addTimeStampSuccessate(int timestamp, double successate) {
        startTimeStamp = Math.min(startTimeStamp, timestamp);
        endTimeStamp = Math.max(endTimeStamp, timestamp);
        timestampSuccessate.put(timestamp, successate);
    }

    public void calculatePrefixSum() {
        nodes = new PrefixSumNode[endTimeStamp-startTimeStamp+1];
        PrefixSumNode accumulateNode = new PrefixSumNode();
        for (int i = 0; i < nodes.length; i++) {
            Double rate = timestampSuccessate.get(startTimeStamp+i);
            if(rate == null) {
                nodes[i]  = accumulateNode.copy();
            }else {
                accumulateNode.addSuccessate(rate);
                nodes[i] = accumulateNode.copy();
            }
        }
        // 结果已全部保存
        timestampSuccessate.clear();
    }

    public double getResult(int leftTimeStamp, int rightTimeStamp) {
        if(rightTimeStamp < startTimeStamp) {
            return 0.0;
        }
        leftTimeStamp = leftTimeStamp - startTimeStamp;
        rightTimeStamp = rightTimeStamp - startTimeStamp;
        int leftIndex = Math.max(0, leftTimeStamp);
        int rightIndex = Math.min(nodes.length-1, rightTimeStamp);

        if(leftIndex == 0) {
            return nodes[rightIndex].prefixSumSuccessate / nodes[rightIndex].prefixSumCount;
        } else {
            int count = nodes[rightIndex].prefixSumCount - nodes[leftIndex - 1].prefixSumCount;
            double rate = nodes[rightIndex].prefixSumSuccessate - nodes[leftIndex - 1].prefixSumSuccessate;
            return rate / count;
        }
    }


}
