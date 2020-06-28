package com.kuaishou.kcode;

import com.kuaishou.kcode.handler.BuildRPCMessageHandler;
import com.kuaishou.kcode.handler.MergeHandler;
import com.kuaishou.kcode.handler.Range2ResultCalculator;
import com.kuaishou.kcode.handler.Range3ResultCalculator;
import com.kuaishou.kcode.model.*;
import com.kuaishou.kcode.utils.DateUtils;

import java.io.RandomAccessFile;
import java.math.RoundingMode;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class KcodeRpcMonitorImpl implements KcodeRpcMonitor {

    private static final long BLOCK_SIZE = 1000*1024*1024;
    private static final int TOTAL_THREAD_NUM = 8;
    private static final int READ_THREAD_NUM = 1;
    private static final int MERGE_THREAD_NUM = 1;
    private static final int CAL_THREAD_NUM = 1;

    private static final ExecutorService rpcMessageHandlerPool = Executors.newFixedThreadPool(TOTAL_THREAD_NUM);
    private final LinkedBlockingQueue<BuildRPCMessageHandler> idleRPCMessageHandler;
    private final LinkedBlockingQueue<Message> messagesQueue;
    private final LinkedBlockingQueue<Range2MessageContainer> range2MessageContainers;
    private final LinkedBlockingQueue<Range3MessageContainer> range3MessageContainers;

    private final HashMap<Integer, HashMap<String, ArrayList<String>>> range2ResultMap;
    private final HashMap<String, Range3Result> range3ResultMap;

    private final BuildRPCMessageHandler rpcMessageHandler;
    private final MergeHandler mergeHandler;
    private final Range2ResultCalculator range2ResultCalculator;
    private final Range3ResultCalculator range3ResultCalculator;

    private static DecimalFormat format;
    private static StringBuilder range2KeyBuilder;

    public KcodeRpcMonitorImpl() {

        format = new DecimalFormat("#.00");
        format.setRoundingMode(RoundingMode.DOWN);
        range2KeyBuilder = new StringBuilder();

        idleRPCMessageHandler = new LinkedBlockingQueue<>();
        messagesQueue = new LinkedBlockingQueue<>();
        range2MessageContainers = new LinkedBlockingQueue<>();
        range3MessageContainers = new LinkedBlockingQueue<>();

        range2ResultMap = new HashMap<>();
        range3ResultMap = new HashMap<>();

        rpcMessageHandler = new BuildRPCMessageHandler(this, messagesQueue);
        mergeHandler = new MergeHandler(messagesQueue, range2MessageContainers, range3MessageContainers);
        range2ResultCalculator = new Range2ResultCalculator(range2MessageContainers, range2ResultMap);
        range3ResultCalculator = new Range3ResultCalculator(range3MessageContainers, range3ResultMap);

        idleRPCMessageHandler.add(rpcMessageHandler);
    }

    @Override
    public void prepare(String path) throws Exception {
        RandomAccessFile randomAccessFile;
        randomAccessFile = new RandomAccessFile(path, "r");

        FileChannel rpcDataFileChannel = randomAccessFile.getChannel();
        long fileSize = randomAccessFile.length();
        //下取整
        int maxBlockSize = (int) (fileSize / BLOCK_SIZE);
        //存在剩余 -> block数 + 1
        maxBlockSize = fileSize % BLOCK_SIZE == 0 ? maxBlockSize : maxBlockSize + 1;

        rpcMessageHandlerPool.execute(mergeHandler);
        rpcMessageHandlerPool.execute(range2ResultCalculator);
        rpcMessageHandlerPool.execute(range3ResultCalculator);

        String remindBuffer = "";
        // 分块读取文件
        for (int currentBlock = 0; currentBlock < maxBlockSize; currentBlock++) {
            System.out.println("Read block " + currentBlock);
            int mapSize;
            mapSize = (int) ((currentBlock == maxBlockSize - 1) ? (fileSize - (maxBlockSize - 1) * BLOCK_SIZE) : BLOCK_SIZE);
            MappedByteBuffer mappedByteBuffer = rpcDataFileChannel.map(FileChannel.MapMode.READ_ONLY, currentBlock * BLOCK_SIZE, mapSize);
            mappedByteBuffer.load();
            System.out.println(String.format("Load block %d/%d %b", currentBlock, maxBlockSize, mappedByteBuffer.isLoaded()));
            int lastLR = mapSize - 1;
            while (mappedByteBuffer.get(lastLR) != '\n') {
                lastLR -= 1;
            }

            BuildRPCMessageHandler rpcMessageHandler = idleRPCMessageHandler.take();
            rpcMessageHandler.setNewByteBuff(mappedByteBuffer, remindBuffer, 0, lastLR);
            rpcMessageHandlerPool.execute(rpcMessageHandler);

            StringBuilder builder = new StringBuilder();
            lastLR += 1;
            while (lastLR < mapSize) {
                builder.append((char) mappedByteBuffer.get(lastLR));
                lastLR++;
            }
            remindBuffer = builder.toString();
        }

        messagesQueue.add(new Message("", "", "", "", true, -1, -1));
        System.out.println("Read Done");

        rpcMessageHandlerPool.shutdown();
        rpcMessageHandlerPool.awaitTermination(50, TimeUnit.SECONDS);
    }

    @Override
    public List<String> checkPair(String caller, String responder, String time) {
        range2KeyBuilder.setLength(0);
        int timeKey = DateUtils.DateToMinuteTimeStamp(time);
        String range2Key = range2KeyBuilder.append(caller).append(responder).toString();


        HashMap<String, ArrayList<String>> service2Result = range2ResultMap.get(timeKey);
        if(service2Result==null) {
            return new ArrayList<>();
        }

        ArrayList<String> result = service2Result.get(range2Key);

        return result == null ? new ArrayList<>() : result;
    }

    @Override
    public String checkResponder(String responder, String start, String end) throws Exception {
        Range3Result range3Result = range3ResultMap.get(responder);
        String resultString = ".00%";
        if (range3Result == null) {
            resultString = "-1.00%";
        } else {
            double resultDouble = range3Result.getResult(DateUtils.DateToMinuteTimeStamp(start), DateUtils.DateToMinuteTimeStamp(end));
            resultDouble *= 100;
            if (resultDouble - 0.0d > 1e-4) {
                resultString = format.format(resultDouble) + "%";
            }
        }
        return resultString;
    }

    public void getCurrentIdxAndUpdateIt(BuildRPCMessageHandler buildRPCMessageHandler) {
        idleRPCMessageHandler.add(buildRPCMessageHandler);
    }
}
