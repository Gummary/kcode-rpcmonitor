package com.kuaishou.kcode.handler;

import java.nio.MappedByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

import com.kuaishou.kcode.KcodeRpcMonitorImpl;
import com.kuaishou.kcode.model.Range2Result;
import com.kuaishou.kcode.model.SuccessRate;
import com.kuaishou.kcode.utils.BufferParser;

public class BuildRPCMessageHandler implements Runnable {

//    public GlobalAverageMeter threadAverageMeter = new GlobalAverageMeter();
    private final static String PARSERTIMER = "PARSER";
    private final static String CALRANGE2TIMER = "RANGE2RESULT";
    private final static String CALRANGE3TIMER = "RANGE3RESULT";

    private MappedByteBuffer targetBuffer;
    private int startIndex;
    private int endIndex;
    private ConcurrentHashMap<String, ConcurrentHashMap<Integer, SuccessRate>> range3Result;
    // timestamp, mainService, calledService, mainIp, calledIp, Range2Result
    private ConcurrentHashMap<Integer, // timestamp
                ConcurrentHashMap<String, // mainService
                        ConcurrentHashMap<String, // calledService
                                ConcurrentHashMap<String, // mainIP
                                        ConcurrentHashMap<String, // calledIP
                                                Range2Result>>>>> range2MessageMap;
//    private ConcurrentHashMap<Integer, ConcurrentHashMap<String, ConcurrentHashMap<String, Range2Result>>> range2MessageMap;

    private KcodeRpcMonitorImpl kcode;

    private String remindBuffer = "";
    /**
     * 因为一个batch中数据基本为同一个分钟时间戳，所以做个缓存
     */
    private int cachedMinute = -1;
//    private ConcurrentHashMap<String, ConcurrentHashMap<String, Range2Result>> cachedMap;
    private ConcurrentHashMap<String,
                ConcurrentHashMap<String,
                        ConcurrentHashMap<String,
                                ConcurrentHashMap<String, Range2Result>>>> cachedMap;


    public BuildRPCMessageHandler(KcodeRpcMonitorImpl kcode,
                                  ConcurrentHashMap<Integer, ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentHashMap<String, Range2Result>>>>> range2MessageMap,
                                  ConcurrentHashMap<String, ConcurrentHashMap<Integer, SuccessRate>> range3Result) {
        this.kcode = kcode;
        this.range2MessageMap = range2MessageMap;
        this.range3Result = range3Result;

//        threadAverageMeter.createTimer(PARSERTIMER);
//        threadAverageMeter.createTimer(CALRANGE2TIMER);
//        threadAverageMeter.createTimer(CALRANGE3TIMER);
    }

    @Override
    public void run() {
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
        messageStart ++;
        // 右边界向后找回车
        while(targetBuffer.get(endIndex) != '\n') {
            endIndex += 1;
        }

        BufferParser bufferParser = new BufferParser(messageStart, targetBuffer);

        // main传进来的endIndex包含当前block的回车，而需要用回车判断数据的结束，所以是<=
        while(bufferParser.getOffset() <= endIndex) {
            buildMessage(bufferParser);
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
        boolean isSuccess = info[4].charAt(0) == 't';
        int useTime = Integer.parseInt(info[5]);
        int secondTimeStamp = (int) (Long.parseLong(info[6])/60000);

        submitMessage(mainService, mainIP, calledService, calledIP, isSuccess, useTime, secondTimeStamp);

    }

    private void buildMessage(BufferParser parser) {
//        if(!threadAverageMeter.isTimerStarted(PARSERTIMER)) {
//            threadAverageMeter.startTimer(PARSERTIMER);
//        }
//        threadAverageMeter.updateStart(PARSERTIMER);

        String mainService = parser.parseString();
        String mainIP = parser.parseString();
        String calledService = parser.parseString();
        String calledIP = parser.parseString();
        boolean isSuccess = parser.parseBoolean();
        int useTime = parser.parseInt();
        int secondTimeStamp = (int)(parser.parseLong()/60000);

//        threadAverageMeter.updateTimer(PARSERTIMER);

        submitMessage(mainService, mainIP, calledService, calledIP, isSuccess, useTime, secondTimeStamp);
    }

    private void submitMessage(String mainService, String mainIP, String calledService, String calledIP, boolean isSuccess, int useTime, int secondTimeStamp) {

//        System.out.println(String.format("Get new log %s %s %s %s %b %d %d", mainService, mainIP, calledService, calledIP, isSuccess, useTime, secondTimeStamp));

        if (cachedMinute != secondTimeStamp) {
            cachedMinute = secondTimeStamp;
            range2MessageMap.putIfAbsent(secondTimeStamp, new ConcurrentHashMap<>());
            cachedMap = range2MessageMap.get(secondTimeStamp);
        }

        //二阶段统计
//        if(!threadAverageMeter.isTimerStarted(CALRANGE2TIMER)) {
//            threadAverageMeter.startTimer(CALRANGE2TIMER);
//        }
//        threadAverageMeter.updateStart(CALRANGE2TIMER);

        cachedMap.putIfAbsent(mainService, new ConcurrentHashMap<>());
        ConcurrentHashMap<String,
                ConcurrentHashMap<String,
                        ConcurrentHashMap<String, Range2Result>>> map1 = cachedMap.get(mainService);

        map1.putIfAbsent(calledService, new ConcurrentHashMap<>());
        ConcurrentHashMap<String, ConcurrentHashMap<String, Range2Result>> map2 = map1.get(calledService);

        map2.putIfAbsent(mainIP, new ConcurrentHashMap<>());
        ConcurrentHashMap<String, Range2Result> map3 = map2.get(mainIP);

        map3.putIfAbsent(calledIP, new Range2Result(mainIP, calledIP));
        Range2Result result = map3.get(calledIP);
        result.fillMessage(isSuccess, useTime);

//        threadAverageMeter.updateTimer(CALRANGE2TIMER);


        //三阶段统计
//        if(!threadAverageMeter.isTimerStarted(CALRANGE3TIMER)){
//            threadAverageMeter.startTimer(CALRANGE3TIMER);
//        }
//        threadAverageMeter.updateStart(CALRANGE3TIMER);

        range3Result.putIfAbsent(calledService, new ConcurrentHashMap<>());
        ConcurrentHashMap<Integer, SuccessRate> successRateMap = range3Result.get(calledService);

        successRateMap.putIfAbsent(secondTimeStamp, new SuccessRate());
        SuccessRate successRate = successRateMap.get(secondTimeStamp);
        if (isSuccess) {
            successRate.success.incrementAndGet();
        }
        successRate.total.incrementAndGet();

//        threadAverageMeter.updateTimer(CALRANGE3TIMER);
    }

    public void setNewByteBuff(MappedByteBuffer targetBuffer, String remindBuffer, int startIndex, int endIndex) {
//        System.out.println(String.format("%d, %d", startIndex, endIndex));
        this.targetBuffer = targetBuffer;
        this.remindBuffer = remindBuffer;
        this.startIndex = startIndex;
        this.endIndex = endIndex;
    }
}
