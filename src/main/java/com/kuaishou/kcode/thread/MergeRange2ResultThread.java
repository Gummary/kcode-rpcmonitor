package com.kuaishou.kcode.thread;

import com.kuaishou.kcode.model.DateUtils;
import com.kuaishou.kcode.model.MinuteMessageContainer;
import com.kuaishou.kcode.model.Range2Result;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

public class MergeRange2ResultThread implements Runnable {

    private LinkedBlockingQueue<MinuteMessageContainer> containersQueue;
    private HashMap<Integer, HashMap<String, HashMap<String, Range2Result>>> allRange2Result;
    Thread thread;

    public MergeRange2ResultThread(LinkedBlockingQueue<MinuteMessageContainer> queue) {
        containersQueue = queue;
        this.allRange2Result = new HashMap<>(16);
        thread = null;

    }

    public void start() {
        thread = new Thread(this);
        thread.start();
    }

    public void join() throws InterruptedException {
        if(thread != null) {
            thread.join();
        }
    }

    public int getTimeStampSize() {
        return allRange2Result.size();
    }

    public Set<Integer> getAllTimeStamp() {
        return allRange2Result.keySet();
    }

    public HashMap<String, HashMap<String, Range2Result>> getMethodIPResult(int timestamp) {
        return allRange2Result.get(timestamp);
    }

    @Override
    public void run() {
        while (true) {
            try {
                MinuteMessageContainer container = containersQueue.take();
                int minute = container.getMinuteTimeStamp();
                HashMap<String, HashMap<String, Range2Result>> newMethodIPMap = container.getMessages();
                if (minute == -1 && newMethodIPMap == null) { // no more data
//                    System.out.println("Merge Done");
                    break;
                }

//                System.out.println(String.format("Merge %d", minute));
                if (allRange2Result.containsKey(minute)) {
                    HashMap<String, HashMap<String, Range2Result>> savedMethodIPMap = allRange2Result.get(minute);
                    for (Map.Entry<String, HashMap<String, Range2Result>> methodip :
                            newMethodIPMap.entrySet()) {

                        String method = methodip.getKey();
                        HashMap<String, Range2Result> newIPResult = methodip.getValue();

                        if (savedMethodIPMap.containsKey(method)) {
                            HashMap<String, Range2Result> savedIpResult = savedMethodIPMap.get(method);
                            for (Map.Entry<String, Range2Result> ipresult :
                                    newIPResult.entrySet()) {

                                String ip = ipresult.getKey();
                                Range2Result result = ipresult.getValue();

                                if (savedIpResult.containsKey(ip)) {
                                    savedIpResult.get(ip).mergeResult(result);
                                } else {
                                    savedIpResult.put(ip, result);
                                }
                            }
                        } else {
                            savedMethodIPMap.put(method, newIPResult);
                        }

                    }
                } else {
                    allRange2Result.put(minute, newMethodIPMap);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
