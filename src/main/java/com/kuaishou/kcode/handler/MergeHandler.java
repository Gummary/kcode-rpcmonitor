package com.kuaishou.kcode.handler;

import com.kuaishou.kcode.model.Message;
import com.kuaishou.kcode.model.Range2MessageContainer;
import com.kuaishou.kcode.model.Range3MessageContainer;

import java.util.concurrent.LinkedBlockingQueue;

public class MergeHandler implements Runnable{

    private final LinkedBlockingQueue<Message> messageQueue;
    private final LinkedBlockingQueue<Range2MessageContainer>  range2MessageContainerQueue;
    private LinkedBlockingQueue<Range3MessageContainer> range3MessageContainerQueue;

    private int currentTimeStamp = -1;
    private Range2MessageContainer currentRange2MessageContainer;
    private Range3MessageContainer currentRange3MessageContainer;

    public MergeHandler(LinkedBlockingQueue<Message> messageQueue,
                        LinkedBlockingQueue<Range2MessageContainer> messageContainerQueue,
                        LinkedBlockingQueue<Range3MessageContainer> range3MessageContainerQueue) {
        this.messageQueue = messageQueue;
        this.range2MessageContainerQueue = messageContainerQueue;
        this.range3MessageContainerQueue = range3MessageContainerQueue;
    }

    @Override
    public void run() {
        while(true) {
            try {
                Message msg = messageQueue.take();
//                System.out.println("Got message");

                if(msg.getMinuteTimeStamp() == -1 && msg.getResponseTime() == -1) { // no more data
                    range2MessageContainerQueue.add(currentRange2MessageContainer);
                    range3MessageContainerQueue.add(currentRange3MessageContainer);
                    range2MessageContainerQueue.add(new Range2MessageContainer(-1));
                    range3MessageContainerQueue.add(new Range3MessageContainer(-1));
                    System.out.println("Merge Done");
                    break;
                }

                if(currentTimeStamp != msg.getMinuteTimeStamp()) {
                    if(currentTimeStamp != -1) {
                        System.out.println("Submit timestamp " + currentTimeStamp);
                        range2MessageContainerQueue.add(currentRange2MessageContainer);
                        range3MessageContainerQueue.add(currentRange3MessageContainer);
                    }
                    currentTimeStamp = msg.getMinuteTimeStamp();
                    currentRange2MessageContainer = new Range2MessageContainer(currentTimeStamp);
                    currentRange3MessageContainer = new Range3MessageContainer(currentTimeStamp);
                }
                currentRange2MessageContainer.addMessage(msg);
                currentRange3MessageContainer.addMessage(msg);

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
