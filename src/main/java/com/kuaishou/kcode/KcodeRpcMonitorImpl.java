package com.kuaishou.kcode;

import com.kuaishou.kcode.handler.BuildRPCMessageHandler;
import com.kuaishou.kcode.model.Range2Result;
import com.kuaishou.kcode.model.Range3Result;
import com.kuaishou.kcode.model.SuccessRate;

import java.io.FileInputStream;
import java.io.IOException;
import java.math.RoundingMode;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author kcode
 * Created on 2020-06-01
 * 实际提交时请维持包名和类名不变
 */

public class KcodeRpcMonitorImpl implements KcodeRpcMonitor {

//    public static final long BLOCK_SIZE = 1000 * 1024 * 1024;
    public static final long BLOCK_SIZE = Integer.MAX_VALUE;
    private static final int CORE_THREAD_NUM = 8;
    private static final ExecutorService rpcMessageHandlerPool = Executors.newFixedThreadPool(CORE_THREAD_NUM);//new ThreadPoolExecutor(CORE_THREAD_NUM, MAX_THREAD_NUM, TIME_OUT, TimeUnit.SECONDS, new SynchronousQueue<>());
    public FileChannel rpcDataFileChannel;
    private final ConcurrentHashMap<Integer, ConcurrentHashMap<String, ConcurrentHashMap<String, Range2Result>>> range2MessageMap = new ConcurrentHashMap<Integer, ConcurrentHashMap<String, ConcurrentHashMap<String, Range2Result>>>(45);
//    private final ConcurrentHashMap<String, ConcurrentHashMap<Integer, SuccessRate>> range3Result;
    private final BuildRPCMessageHandler[] writeRPCMessageHandlers = new BuildRPCMessageHandler[CORE_THREAD_NUM];
    private final BlockingQueue<BuildRPCMessageHandler> readyedMessageHandlers = new LinkedBlockingQueue<>();


    private final static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
    private static DecimalFormat format;


    //利用线程池优化2,3阶段
    private static final ExecutorService range23ComputePool = Executors.newFixedThreadPool(CORE_THREAD_NUM);
    private static final AtomicInteger computeIdx = new AtomicInteger();
    private static final ConcurrentHashMap<String, ArrayList<String>> computedRange2Result = new ConcurrentHashMap<>(500000);
    private static final ConcurrentHashMap<String, ArrayList<Range3Result>> computedRange3Result = new ConcurrentHashMap<>(50000);
    private static final HashMap<String, String> cachedRange3Result = new HashMap<>();


    // Timer Setting
//    private static GlobalAverageMeter globalAverageMeter = new GlobalAverageMeter();
    private final static String PREPARETIMER = "PREPARE";
    private final static String CALRANGE2 = "CALRANGE2";
    private final static String CALRAGNE3 = "CALRAGNE3";
    private final static String READTIMER = "READTIMER";
    private final static String PARSERTIMER = "PARSER";

    //TEST
    // 不要修改访问级别
    public KcodeRpcMonitorImpl() {
        format = new DecimalFormat("#.00");
        format.setRoundingMode(RoundingMode.DOWN);
//        range3Result = new ConcurrentHashMap<>();
        for (int i = 0; i < writeRPCMessageHandlers.length; i++) {
            writeRPCMessageHandlers[i] = new BuildRPCMessageHandler(this, range2MessageMap);
            readyedMessageHandlers.add(writeRPCMessageHandlers[i]);
        }


//        globalAverageMeter.createTimer(PREPARETIMER);
//        globalAverageMeter.createTimer(CALRANGE2);
//        globalAverageMeter.createTimer(CALRAGNE3);
//        globalAverageMeter.createTimer(READTIMER);
//        globalAverageMeter.createTimer(PARSERTIMER);

    }


    @Override
    public void prepare(String path) {
//        globalAverageMeter.updateStart(PREPARETIMER);
//        RandomAccessFile randomAccessFile;
        try {
//            randomAccessFile = new RandomAccessFile(path, "r");
//            this.rpcDataFile = randomAccessFile;
//            this.rpcDataFileChannel = randomAccessFile.getChannel();
//            long fileSize = randomAccessFile.length();
            FileInputStream fileInputStream = new FileInputStream(path);
            FileChannel fileChannel = fileInputStream.getChannel();
            rpcDataFileChannel = fileChannel;
            long fileSize = fileChannel.size();
//			System.out.println(String.format("file length:%d", fileSize));
            //下取整
            int maxBlockSize = (int) (fileSize / BLOCK_SIZE);
            //存在剩余 -> block数 + 1
            maxBlockSize = fileSize % BLOCK_SIZE == 0 ? maxBlockSize : maxBlockSize + 1;

            String remindBuffer = "";
//            System.out.println(String.format("Total block %d", maxBlockSize));
            // 分块读取文件
            for (int currentBlock = 0; currentBlock < maxBlockSize; currentBlock++) {
//                globalAverageMeter.updateStart(READTIMER);
                int mapSize;
                mapSize = (int) ((currentBlock == maxBlockSize - 1) ? (fileSize - (maxBlockSize - 1) * BLOCK_SIZE) : BLOCK_SIZE);
//                System.out.println(String.format("Read block %d", currentBlock));
                MappedByteBuffer mappedByteBuffer = rpcDataFileChannel.map(FileChannel.MapMode.READ_ONLY, currentBlock * BLOCK_SIZE, mapSize);
                mappedByteBuffer.load();
//                System.out.println(String.format("%d: Load block %d/%d", System.currentTimeMillis(), currentBlock, maxBlockSize));

                int lastLR = mapSize - 1;
                while (mappedByteBuffer.get(lastLR) != '\n') {
                    lastLR -= 1;
                }
                // 每个线程读取等量的数据
                int readSize = mapSize / CORE_THREAD_NUM;
                int startIndex = 0;

                for (int i = 0; i < CORE_THREAD_NUM; i++) {
//                    globalAverageMeter.updateStart(PARSERTIMER);
                    BuildRPCMessageHandler rpcMessageHandler = readyedMessageHandlers.take();
                    int endIndex = startIndex + readSize;
                    // 对于第一个线程，可能要读取上一个BLOCK剩下的部分
                    if (i == 0) {
                        rpcMessageHandler.setNewByteBuff(mappedByteBuffer, remindBuffer, startIndex, endIndex, String.format("%d %d/%d",currentBlock, i, CORE_THREAD_NUM));
                    } else if (i == CORE_THREAD_NUM - 1) { // 最后一个线程读取到这一块的醉猴一个回车
                        rpcMessageHandler.setNewByteBuff(mappedByteBuffer, "", startIndex, lastLR, String.format("%d %d/%d", currentBlock, i, CORE_THREAD_NUM));
                    } else {//其他线程都是完整的数据
                        rpcMessageHandler.setNewByteBuff(mappedByteBuffer, "", startIndex, endIndex, String.format("%d %d/%d", currentBlock, i, CORE_THREAD_NUM));
                    }
                    startIndex = endIndex;
                    rpcMessageHandlerPool.execute(rpcMessageHandler);
//                    globalAverageMeter.updateTimer(PARSERTIMER);
                }
                StringBuilder builder = new StringBuilder();
                lastLR += 1;
                while (lastLR < mapSize) {
                    builder.append((char) mappedByteBuffer.get(lastLR));
                    lastLR++;
                }
                remindBuffer = builder.toString();
//                globalAverageMeter.updateTimer(READTIMER);

            }

            rpcMessageHandlerPool.shutdown();
            rpcMessageHandlerPool.awaitTermination(10, TimeUnit.SECONDS);

//            globalAverageMeter.updateStart(CALRANGE2);
            System.out.println(String.format("%d Start comput range2", System.currentTimeMillis()));
            computeRange2Result();
            System.out.println(String.format("%d Finish comput range2", System.currentTimeMillis()));
//            globalAverageMeter.updateTimer(CALRANGE2);
//            globalAverageMeter.updateStart(CALRAGNE3);
            computeRange3Result();
//            globalAverageMeter.updateTimer(CALRAGNE3);

//            randomAccessFile.close();
            fileInputStream.close();

        } catch (InterruptedException | IOException ignored) {
        } finally {
            range23ComputePool.shutdownNow();
            rpcMessageHandlerPool.shutdownNow();
        }
//        globalAverageMeter.updateTimer(PREPARETIMER);

//        StringBuilder statisticBuilder = new StringBuilder();
//        statisticBuilder.append(globalAverageMeter.getStatisticString());
//        for (int i = 0; i < writeRPCMessageHandlers.length / 2; i++) {

//            statisticBuilder.append(String.format("Thread %d\n", i));
//            statisticBuilder.append(writeRPCMessageHandlers[i].averageMeter.getStatisticString()).append('\n');
//        }

//        throw new Exception(statisticBuilder.toString());
//        System.out.println(statisticBuilder.toString());

    }

    private void computeRange3Result() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(CORE_THREAD_NUM);
        String[] keyList = writeRPCMessageHandlers[0].range3Result.keySet().toArray(new String[0]);
        computeIdx.set(0);
        for (int i = 0; i < CORE_THREAD_NUM; i++) {
            range23ComputePool.execute(() -> {
                int workIndex = computeIdx.getAndIncrement();
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");

                while (workIndex < keyList.length) {
                    String workKey = keyList[workIndex];
                    HashMap<Integer, SuccessRate> mergedResult = new HashMap<>();
                    // MERGE RESULT FROM EACH THREAD
                    for (int j = 0; j < CORE_THREAD_NUM; j++) {
                        BuildRPCMessageHandler handle = writeRPCMessageHandlers[j];
                        HashMap<Integer, SuccessRate> minuteSuccessRate = handle.getRange3Rate(workKey);
                        if (minuteSuccessRate == null) {
                            continue;
                        }
                        for (Entry<Integer, SuccessRate> entry :
                                minuteSuccessRate.entrySet()) {
                            if (mergedResult.containsKey(entry.getKey())) {
                                mergedResult.get(entry.getKey()).mergeSuccessRate(entry.getValue());
                            } else {
                                mergedResult.put(entry.getKey(), entry.getValue());
                            }
                        }
                    }
                    ArrayList<Range3Result> currentKeyResults = new ArrayList<>();
                    for (Entry<Integer, SuccessRate> entry :
                            mergedResult.entrySet()) {
                        int minuteTimeStamp = entry.getKey();
                        String dateTimeStamp = simpleDateFormat.format(new Date(minuteTimeStamp * 60000L));
                        SuccessRate successRate = entry.getValue();
                        double rate = (double) successRate.success.get() / successRate.total.get();
                        currentKeyResults.add(new Range3Result(dateTimeStamp, rate));
                    }
                    Collections.sort(currentKeyResults);
                    computedRange3Result.put(workKey, currentKeyResults);
                    workIndex = computeIdx.getAndIncrement();
                }
                latch.countDown();
            });
        }
        latch.await();
    }

    private void computeRange2Result() throws InterruptedException {

        CountDownLatch latch = new CountDownLatch(CORE_THREAD_NUM);
        Integer[] keyList = range2MessageMap.keySet().toArray(new Integer[0]);
        computeIdx.set(0);
        for (int i = 0; i < CORE_THREAD_NUM; i++) {
            range23ComputePool.execute(() -> {
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
                int workIndex = computeIdx.getAndIncrement();
                while (workIndex < keyList.length) {
                    int workMinuteStamp = keyList[workIndex];
                    ConcurrentHashMap<String, ConcurrentHashMap<String, Range2Result>> functionMap = range2MessageMap.get(workMinuteStamp);
                    for (Entry<String, ConcurrentHashMap<String, Range2Result>> node : functionMap.entrySet()) {
                        String key = node.getKey();
                        ConcurrentHashMap<String, Range2Result> valueMap = node.getValue();
                        Iterator<Entry<String, Range2Result>> resultIterator = valueMap.entrySet().iterator();
                        ArrayList<String> resultList = new ArrayList<>();
                        while (resultIterator.hasNext()) {
                            Range2Result resultNnode = resultIterator.next().getValue();
                            String builder = resultNnode.mainIP + ',' +
                                    resultNnode.calledIP + ',' +
                                    resultNnode.computeSuccessRate(format) + ',' +
                                    resultNnode.computeP99();
                            resultList.add(builder);
                        }
                        String date = simpleDateFormat.format(new Date(workMinuteStamp * 60000L));
                        computedRange2Result.put(key + date, resultList);
                    }
                    workIndex = computeIdx.getAndIncrement();
                }
                latch.countDown();
            });
        }
        latch.await();
    }

    @Override
    public List<String> checkPair(String caller, String responder, String time) {

        String range2Key = caller + "-" + responder + time;
        ArrayList<String> result = computedRange2Result.get(range2Key);
//        globalAverageMeter.updateStage2Query();
        return result == null ? new ArrayList<>() : result;
    }


    @Override
    public String checkResponder(String responder, String start, String end) {
//        globalAverageMeter.getStatistic("Count: "+count);

        String cachedKey = responder + start + end;
        String result = cachedRange3Result.get(cachedKey);
        if(result == null) {
            ArrayList<Range3Result> results = computedRange3Result.get(responder);
            if (results == null) {
                result = "-1.00%";
            } else {
                double rate = 0.0d;
                int count = 0;
                for (Range3Result minuteResult :
                        results) {
                    if (minuteResult.getTimeStamp().compareTo(start) >= 0) {
                        if (minuteResult.getTimeStamp().compareTo(end) > 0) {
                            break;
                        }
                        rate += minuteResult.getSuccessRate();
                        count += 1;
                    }
                }
                double resultDouble = rate * 100 / count;
                String resultString = format.format(resultDouble);
                result = ".00%";
                if (resultDouble - 0.0d >= 1e-4) {
                    result = resultString + "%";
                }
            }
            cachedRange3Result.put(cachedKey, result);
        }
        return result;
    }

    /**
     * 当前WriteRPCMessageHandler获得自己的读取任务
     * 设置该writeRPCMessageHandler在当前block中处理的数据范围
     * 如果这个读取任务超过两个Block，那么更新blocks数组的设置（1 -> 0后1 -> null）及WriteRPCMessageHandler的标志位true
     *
     * @param writeRPCMessageHandler
     */
    public void getCurrentIdxAndUpdateIt(BuildRPCMessageHandler writeRPCMessageHandler) {
        readyedMessageHandlers.add(writeRPCMessageHandler);
    }

}
