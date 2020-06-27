package com.kuaishou.kcode.model;


import java.text.DecimalFormat;
import java.util.Comparator;
import java.util.concurrent.PriorityBlockingQueue;

public class Range2Result {
    public SuccessRate successRate;
    public PriorityBlockingQueue<Integer> queue;
    public String mainIP;
    public String calledIP;
    public int ansP99;


    public Range2Result(String mainIP, String calledIP) {
        super();
        this.mainIP = mainIP;
        this.calledIP = calledIP;
        this.queue = new PriorityBlockingQueue<Integer>(200, new Comparator<Integer>() {

            @Override
            public int compare(Integer o1, Integer o2) {
                return o2 - o1;
            }
        });
        this.successRate = new SuccessRate();
        this.ansP99 = -1;
    }



    public void fillMessage(boolean isSuccess, int useTime) {
        queue.add(useTime);
        if(isSuccess) {
            successRate.success.incrementAndGet();
        }
        successRate.total.incrementAndGet();
    }

    public String computeSuccessRate(DecimalFormat format) {
//        System.out.println(String.format("%s %s %d %d %s",mainIP, calledIP, successRate.success.get(), successRate.total.get(), successRate.computeSuccessRate(format)));
        return successRate.computeSuccessRate(format);
    }


    public synchronized int computeP99() {

        if(ansP99 < 0) {
            int ans = -1;
            double p99 = queue.size() * 0.01;
            int index = 0;
            if(p99 == Math.ceil(p99)) { // integer
                index = (int) Math.ceil(p99) + 1;
            } else {
                index = (int) Math.ceil((double)queue.size()*0.01);
            }
//            int index = queue.size() - (int);
            int count = 0;
            while(count < index) {
                ans = queue.poll();
                count++;
            }
            ansP99 = ans;
            return ans;
        }else {
            return ansP99;
        }

    }

}