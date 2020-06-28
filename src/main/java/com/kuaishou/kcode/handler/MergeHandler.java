package com.kuaishou.kcode.handler;

import com.kuaishou.kcode.KcodeRpcMonitor;
import com.kuaishou.kcode.KcodeRpcMonitorImpl;
import com.kuaishou.kcode.model.Message;
import com.kuaishou.kcode.model.Range2MessageContainer;
import com.kuaishou.kcode.model.Range3MessageContainer;

import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

public class MergeHandler implements Runnable {

    private final ArrayBlockingQueue<Message> messageQueue;
    private final LinkedBlockingQueue<Range2MessageContainer> range2MessageContainerQueue;
    private LinkedBlockingQueue<Range3MessageContainer> range3MessageContainerQueue;

    private int currentTimeStamp = -1;
    private Range2MessageContainer currentRange2MessageContainer;
    private Range3MessageContainer currentRange3MessageContainer;

    private ArrayList<Range2MessageContainer> cachedRange2List;
    private ArrayList<Range3MessageContainer> cachedRange3List;

    private KcodeRpcMonitorImpl kcode;
    private CountDownLatch latch;

    public MergeHandler(KcodeRpcMonitorImpl kcode,
                        ArrayBlockingQueue<Message> messageQueue,
                        LinkedBlockingQueue<Range2MessageContainer> messageContainerQueue,
                        LinkedBlockingQueue<Range3MessageContainer> range3MessageContainerQueue) {

        this.kcode = kcode;
        this.messageQueue = messageQueue;
        this.range2MessageContainerQueue = messageContainerQueue;
        this.range3MessageContainerQueue = range3MessageContainerQueue;

    }

    public void setLatch(CountDownLatch latch) {
        this.latch = latch;
    }

    @Override
    public void run() {
        long start = System.nanoTime();
        long totalTime = 0;
        long count = 0;
        while (true) {
            try {
                start = System.nanoTime();
                count++;
                Message msg = messageQueue.take();
//                System.out.println("Got message");

                if (msg.getMinuteTimeStamp() == -1 && msg.getResponseTime() == -1) { // no more data
                    range2MessageContainerQueue.add(currentRange2MessageContainer);
                    range3MessageContainerQueue.add(currentRange3MessageContainer);
                    range2MessageContainerQueue.add(new Range2MessageContainer(-1));
                    range3MessageContainerQueue.add(new Range3MessageContainer(-1));
                    System.out.println("Merge Done");
                    break;
                }

                if (currentTimeStamp != msg.getMinuteTimeStamp()) {
                    if (currentTimeStamp != -1) {
                        System.out.println("Submit timestamp " + currentTimeStamp);
                        range2MessageContainerQueue.add(currentRange2MessageContainer);
                        range3MessageContainerQueue.add(currentRange3MessageContainer);
                        latch.countDown();
                        latch.await();
                        kcode.setMergeLatch(this);
                    }
                    currentTimeStamp = msg.getMinuteTimeStamp();
                    currentRange2MessageContainer = new Range2MessageContainer(currentTimeStamp);
                    currentRange3MessageContainer = new Range3MessageContainer(currentTimeStamp);

                }
                currentRange2MessageContainer.addMessage(msg);
                currentRange3MessageContainer.addMessage(msg);
                totalTime += System.nanoTime() - start;
                if (count % 1e6 == 0) {

                    System.out.println(String.format("MERGE Average Time %f Total Numer %d",  totalTime / count / 1e6, count));
                }

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
