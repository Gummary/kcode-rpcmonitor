package com.kuaishou.kcode.handler;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import com.kuaishou.kcode.KcodeRpcMonitorImpl;
import com.kuaishou.kcode.model.FileRPCMessage;
import com.kuaishou.kcode.model.GlobalAverageMeter;
import com.kuaishou.kcode.model.Range2Result;
import com.kuaishou.kcode.model.SuccessRate;
import com.kuaishou.kcode.utils.BufferParser;

public class BuildRPCMessageHandler implements Runnable {


    private KcodeRpcMonitorImpl kcode;
    private MappedByteBuffer targetBuffer;
    private int startIndex;
    private int endIndex;
    public HashMap<String, HashMap<Integer, SuccessRate>> range3Result;
    public ConcurrentHashMap<Integer, ConcurrentHashMap<String, ConcurrentHashMap<String, Range2Result>>> range2MessageMap;


    private String remindBuffer = "";
    /**
     * 因为一个batch中数据基本为同一个分钟时间戳，所以做个缓存
     */
    private int cachedMinute = -1;
    private ConcurrentHashMap<String, ConcurrentHashMap<String, Range2Result>> cachedMap;


    // TIMER SETTING
    public final GlobalAverageMeter averageMeter;
    private final static String PARSEDATATIMER = "PARSEDATATIMER";
    private final static String ADDRESULT2TIMER = "ADDRESULT2TIMER";
    private final static String ADDRESULT3TIMER = "ADDRESULT3TIMER";
    private final static String FINDLRTIMER = "FINDLRTIMER";
    private final static String RUNTIMER = "RUNTIMER";


    public BuildRPCMessageHandler(KcodeRpcMonitorImpl kcode,
                                  ConcurrentHashMap<Integer, ConcurrentHashMap<String, ConcurrentHashMap<String, Range2Result>>> range2MessageMap) {
        this.kcode = kcode;
        this.range2MessageMap = range2MessageMap;
//        this.range3Result = range3Result;
//        this.range2MessageMap = new HashMap<>();
        this.range3Result = new HashMap<>();

        averageMeter = new GlobalAverageMeter();
        averageMeter.createTimer(PARSEDATATIMER);
        averageMeter.createTimer(ADDRESULT2TIMER);
        averageMeter.createTimer(ADDRESULT3TIMER);
        averageMeter.createTimer(RUNTIMER);
        averageMeter.createTimer(FINDLRTIMER);
    }

    @Override
    public void run() {
        averageMeter.updateStart(RUNTIMER);
        averageMeter.updateStart(FINDLRTIMER);
        byte curByte;
        int messageStart = startIndex;
        StringBuilder builder = new StringBuilder();
        builder.append(remindBuffer);
        // 处理被截断的第一条数据
        while ((curByte = targetBuffer.get(messageStart)) != '\n') {
            messageStart += 1;
            builder.append((char) curByte);
        }
        // 第一个batch还要处理上一个block的尾部
        if (startIndex == 0) {
            String logString = builder.toString();
            buildStringMessage(logString);
        }
        // 跳过第一条数据的回车
        messageStart++;
        // 右边界向后找回车
        while (targetBuffer.get(endIndex) != '\n') {
            endIndex += 1;
        }

        BufferParser bufferParser = new BufferParser(messageStart, targetBuffer);
        averageMeter.updateTimer(FINDLRTIMER);

        // main传进来的endIndex包含当前block的回车，而需要用回车判断数据的结束，所以是<=
        while (bufferParser.getOffset() <= endIndex) {
            buildMessage(bufferParser);
        }
        //回调并更新
        kcode.getCurrentIdxAndUpdateIt(this);
        averageMeter.updateTimer(RUNTIMER);
    }

    public HashMap<Integer, SuccessRate> getRange3Rate(String key) {
        return range3Result.get(key);
    }

    private void buildStringMessage(String message) {
        String[] info = message.split(",");
        String mainService = info[0];
        String mainIP = info[1];
        String calledService = info[2];
        String calledIP = info[3];
        boolean isSuccess = info[4].charAt(0) == 't';
        int useTime = Integer.parseInt(info[5]);
        int secondTimeStamp = (int) TimeUnit.MILLISECONDS.toMinutes(Long.parseLong(info[6]));

        submitMessage(mainService, mainIP, calledService, calledIP, isSuccess, useTime, secondTimeStamp);

    }

    private void buildMessage(BufferParser parser) {
//        if(!threadAverageMeter.isTimerStarted(PARSERTIMER)) {
//            threadAverageMeter.startTimer(PARSERTIMER);
//        }
//        threadAverageMeter.updateStart(PARSERTIMER);

        averageMeter.updateStart(PARSEDATATIMER);
        String mainService = parser.parseString();
        String mainIP = parser.parseString();
        String calledService = parser.parseString();
        String calledIP = parser.parseString();
        boolean isSuccess = parser.parseBoolean();
        int useTime = parser.parseInt();
        int secondTimeStamp = parser.parseMinuteTimeStamp();
        averageMeter.updateTimer(PARSEDATATIMER);

//        threadAverageMeter.updateTimer(PARSERTIMER);

        submitMessage(mainService, mainIP, calledService, calledIP, isSuccess, useTime, secondTimeStamp);
    }

    //二阶段统计
    private void submitMessage(String mainService, String mainIP, String calledService, String calledIP, boolean isSuccess, int useTime, int secondTimeStamp) {

        averageMeter.updateStart(ADDRESULT2TIMER);

        if (cachedMinute != secondTimeStamp) {
            cachedMinute = secondTimeStamp;
            range2MessageMap.putIfAbsent(secondTimeStamp, new ConcurrentHashMap<>());
            cachedMap = range2MessageMap.get(secondTimeStamp);
        }

        String range2Key = mainService + '-' + calledService;
        cachedMap.putIfAbsent(range2Key, new ConcurrentHashMap<>());
        ConcurrentHashMap<String, Range2Result> ipResult = cachedMap.get(range2Key);


        String range2IPKey = mainIP + '-' + calledIP;
        ipResult.putIfAbsent(range2IPKey, new Range2Result(mainIP, calledIP));
        Range2Result result = ipResult.get(range2IPKey);
        result.fillMessage(isSuccess, useTime);

        averageMeter.updateTimer(ADDRESULT2TIMER);

        averageMeter.updateStart(ADDRESULT3TIMER);
        //三阶段统计
        range3Result.putIfAbsent(calledService, new HashMap<>());
        HashMap<Integer, SuccessRate> successRateMap = range3Result.get(calledService);

        successRateMap.putIfAbsent(secondTimeStamp, new SuccessRate());
        SuccessRate successRate = successRateMap.get(secondTimeStamp);
        if (isSuccess) {
            successRate.success.incrementAndGet();
        }
        successRate.total.incrementAndGet();
        averageMeter.updateTimer(ADDRESULT3TIMER);
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
