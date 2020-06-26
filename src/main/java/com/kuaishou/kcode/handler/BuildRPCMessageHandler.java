package com.kuaishou.kcode.handler;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import com.kuaishou.kcode.KcodeRpcMonitorImpl;
import com.kuaishou.kcode.model.FileRPCMessage;
import com.kuaishou.kcode.model.Range2Result;
import com.kuaishou.kcode.model.SuccessRate;

public class BuildRPCMessageHandler implements Runnable {


    private KcodeRpcMonitorImpl kcode;
    private MappedByteBuffer targetBuffer;
    private int startIndex;
    private int endIndex;
    private ConcurrentHashMap<String, ConcurrentHashMap<Integer, SuccessRate>> range3Result;
    private ConcurrentHashMap<Integer, ConcurrentHashMap<String, ConcurrentHashMap<String, Range2Result>>> range2MessageMap;


    private String remindBuffer = "";
    /**
     * 因为一个batch中数据基本为同一个分钟时间戳，所以做个缓存
     */
    private int cachedMinute = -1;
    private ConcurrentHashMap<String, ConcurrentHashMap<String, Range2Result>> cachedMap;


    public BuildRPCMessageHandler(KcodeRpcMonitorImpl kcode,
                                  ConcurrentHashMap<Integer, ConcurrentHashMap<String, ConcurrentHashMap<String, Range2Result>>> range2MessageMap,
                                  ConcurrentHashMap<String, ConcurrentHashMap<Integer, SuccessRate>> range3Result) {
        this.kcode = kcode;
        this.range2MessageMap = range2MessageMap;
        this.range3Result = range3Result;
    }

    @Override
    public void run() {
        int[] splitIdxList = new int[6];//一条消息有6块
        int countSpiltIdx = 0;
        byte curByte;
        int messageStart = 0;

        StringBuilder builder = new StringBuilder();
        // 处理被截断的第一条数据
        if (!"".equals(remindBuffer)) {
            builder.append(remindBuffer);
            while ((curByte = targetBuffer.get(startIndex)) != '\n') {
                startIndex += 1;
                builder.append((char) curByte);
            }
            String logString = builder.toString();
            buildStringMessage(logString);
            // 跳过第一条数据的回车
            startIndex += 1;
        }
        messageStart = startIndex;
        // main传进来的endIndex包含当前block的回车，而需要用回车判断数据的结束，所以是<=
        for (int i = startIndex; i <= endIndex; i++) {
            curByte = targetBuffer.get(i);

            if (curByte == ',') {
                splitIdxList[countSpiltIdx] = i;
                countSpiltIdx++;
            }
            if (curByte == '\n') {

                buildMessage(targetBuffer, messageStart, splitIdxList);

                messageStart = i + 1;
                countSpiltIdx = 0;
            }
        }

        //回调并更新
        kcode.getCurrentIdxAndUpdateIt(this);
    }

    private void buildStringMessage(String message) {
        String[] info = message.split(",");
        String mainService = info[0];
        String mainIP = info[1];
        String calledService = info[2];
        String calledIP = info[3];
        int isSuccess = info[4].charAt(0) == 't' ? 1 : 0;
        int useTime = Integer.parseInt(info[5]);
        int secondTimeStamp = (int) TimeUnit.MICROSECONDS.toMinutes(Long.parseLong(info[6]));

        submitMessage(mainService, mainIP, calledService, calledIP, isSuccess, useTime, secondTimeStamp);

    }

    private void buildMessage(ByteBuffer buffer, int messageStartIdx, int[] splitIdxList) {
        String mainService = buildString(buffer, messageStartIdx, splitIdxList[0]);
        String mainIP = buildString(buffer, splitIdxList[0] + 1, splitIdxList[1]);
        String calledService = buildString(buffer, splitIdxList[1] + 1, splitIdxList[2]);
        String calledIP = buildString(buffer, splitIdxList[2] + 1, splitIdxList[3]);
        int isSuccess = buildBoolean(buffer, splitIdxList[3] + 1);
        int useTime = buildInt(buffer, splitIdxList[4] + 1, splitIdxList[5]);
        int secondTimeStamp = buildMinuteTimeStamp(buffer, splitIdxList[5] + 1);

        submitMessage(mainService, mainIP, calledService, calledIP, isSuccess, useTime, secondTimeStamp);
    }

    //二阶段统计
    private void submitMessage(String mainService, String mainIP, String calledService, String calledIP, int isSuccess, int useTime, int secondTimeStamp) {

        if (cachedMinute != secondTimeStamp) {
            cachedMinute = secondTimeStamp;
            range2MessageMap.putIfAbsent(secondTimeStamp, new ConcurrentHashMap<>());
            cachedMap = range2MessageMap.get(secondTimeStamp);
        }

        String range2Key = mainService + calledService;
        cachedMap.putIfAbsent(range2Key, new ConcurrentHashMap<>());
        ConcurrentHashMap<String, Range2Result> ipResult = cachedMap.get(range2Key);


        String range2IPKey = mainIP + '-' + calledIP;
        ipResult.putIfAbsent(range2IPKey, new Range2Result(mainIP, calledIP));
        Range2Result result = ipResult.get(range2IPKey);
        result.fillMessage(isSuccess, useTime);

        //三阶段统计
        range3Result.putIfAbsent(calledService, new ConcurrentHashMap<>());
        ConcurrentHashMap<Integer, SuccessRate> successRateMap = range3Result.get(calledService);

        successRateMap.putIfAbsent(secondTimeStamp, new SuccessRate());
        SuccessRate successRate = successRateMap.get(secondTimeStamp);
        if (isSuccess > 0) {
            successRate.success.incrementAndGet();
        }
        successRate.total.incrementAndGet();
    }

    private String buildString(ByteBuffer buffer, int startIdx, int endIndex) {
        StringBuilder builder = new StringBuilder();
        for (int i = startIdx; i < endIndex; i++) {
            builder.append((char) buffer.get(i));
        }
        return builder.toString();
    }

    private int buildInt(ByteBuffer buffer, int startIdx, int endIndex) {
        int buildInt = 0;
        for (int i = startIdx; i < endIndex; i++) {
            buildInt *= 10;
            buildInt += buffer.get(i) - '0';

        }
        return buildInt;
    }

    private int buildBoolean(ByteBuffer buffer, int startIdx) {
        byte curByte = buffer.get(startIdx);
        if (curByte == 't') {
            return 1;
        } else {
            return 0;
        }
    }

    /**
     * 返回秒级时间戳除以60的结果
     *
     * @param startIdx
     * @return
     */
    private int buildMinuteTimeStamp(ByteBuffer buffer, int startIdx) {
        int buildTimeStamp = 0;
        for (int i = startIdx; i < startIdx + 9; i++) {
            buildTimeStamp *= 10;
            buildTimeStamp += buffer.get(i) - '0';
        }
        return buildTimeStamp / 6;
    }


    public void setNewByteBuff(MappedByteBuffer targetBuffer, String remindBuffer, int startIndex, int endIndex) {
        this.targetBuffer = targetBuffer;
        this.remindBuffer = remindBuffer;
        this.startIndex = startIndex;
        this.endIndex = endIndex;
    }
}
