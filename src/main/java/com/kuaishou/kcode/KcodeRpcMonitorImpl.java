package com.kuaishou.kcode;

import com.kuaishou.kcode.handler.BuildRPCMessageHandler;
import com.kuaishou.kcode.model.*;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.math.RoundingMode;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.text.DecimalFormat;
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

    public static final long BLOCK_SIZE = Integer.MAX_VALUE;
    private static final int CORE_THREAD_NUM = 8;
    private static final ExecutorService rpcMessageHandlerPool = Executors.newFixedThreadPool(CORE_THREAD_NUM);//new ThreadPoolExecutor(CORE_THREAD_NUM, MAX_THREAD_NUM, TIME_OUT, TimeUnit.SECONDS, new SynchronousQueue<>());
    private static final ExecutorService blockHandlerPool = Executors.newSingleThreadExecutor();
    public RandomAccessFile rpcDataFile;
    public FileChannel rpcDataFileChannel;
    private final ConcurrentHashMap<Integer, ConcurrentHashMap<String, ConcurrentHashMap<String, Range2Result>>> range2MessageMap = new ConcurrentHashMap<Integer, ConcurrentHashMap<String, ConcurrentHashMap<String, Range2Result>>>();
    private final ConcurrentHashMap<String, ConcurrentHashMap<Integer, SuccessRate>> range3Result;
    private final BuildRPCMessageHandler[] writeRPCMessageHandlers = new BuildRPCMessageHandler[CORE_THREAD_NUM];
    private final BlockingQueue<BuildRPCMessageHandler> readyedMessageHandlers = new LinkedBlockingQueue<>();

    private static DecimalFormat format;


    //利用线程池优化2,3阶段
    private static final ExecutorService range23ComputePool = Executors.newFixedThreadPool(CORE_THREAD_NUM);
    private static final AtomicInteger computeIdx = new AtomicInteger();
    private static final ConcurrentHashMap<String, ArrayList<String>> computedRange2Result = new ConcurrentHashMap<>(500000);
    private static final ConcurrentHashMap<String, Range3Result> computedRange3Result = new ConcurrentHashMap<>(512);
    private static final StringBuilder range2KeyBuilder = new StringBuilder();

    // Timer Setting
    private static GlobalAverageMeter globalAverageMeter = new GlobalAverageMeter();
    private static final String PREPARETIMER = "PREPARE";
    private static final String RANGE2TIMER = "RANGE2";
    private static final String RANGE3TIMER = "RANGE3";
    private static int range3CalledTime = 0;

    
    //TEST
    // 不要修改访问级别
    public KcodeRpcMonitorImpl() {
        format = new DecimalFormat("#.00");
        format.setRoundingMode(RoundingMode.DOWN);
        range3Result = new ConcurrentHashMap<>();
        for (int i = 0; i < writeRPCMessageHandlers.length; i++) {
            writeRPCMessageHandlers[i] = new BuildRPCMessageHandler(this, range2MessageMap, range3Result);
            readyedMessageHandlers.add(writeRPCMessageHandlers[i]);
        }

        globalAverageMeter.createTimer(PREPARETIMER);
        globalAverageMeter.createTimer(RANGE2TIMER);
        globalAverageMeter.createTimer(RANGE3TIMER);
    }


    @Override
    public void prepare(String path) {
        globalAverageMeter.startTimer(PREPARETIMER);
        RandomAccessFile randomAccessFile = null;
        try {
            randomAccessFile = new RandomAccessFile(path, "r");
            this.rpcDataFile = randomAccessFile;
            this.rpcDataFileChannel = randomAccessFile.getChannel();
            long fileSize = randomAccessFile.length();
            //下取整
            int maxBlockSize = (int) (fileSize / BLOCK_SIZE);
            //存在剩余 -> block数 + 1
            maxBlockSize = fileSize % BLOCK_SIZE == 0 ? maxBlockSize : maxBlockSize + 1;

            String remindBuffer = "";
            // 分块读取文件
            for (int currentBlock = 0; currentBlock < maxBlockSize; currentBlock++) {
                int mapSize;
                mapSize = (int) ((currentBlock == maxBlockSize - 1) ? (fileSize - (maxBlockSize - 1) * BLOCK_SIZE) : BLOCK_SIZE);
                MappedByteBuffer mappedByteBuffer = rpcDataFileChannel.map(FileChannel.MapMode.READ_ONLY, currentBlock * BLOCK_SIZE, mapSize);

                int lastLR = mapSize - 1;
                while (mappedByteBuffer.get(lastLR) != '\n') {
                    lastLR -= 1;
                }
                // 每个线程读取等量的数据
                int readSize = mapSize / CORE_THREAD_NUM;
                int[] readerStartIndex = new int[CORE_THREAD_NUM];
                int[] endIndex = new int[CORE_THREAD_NUM];

                for (int i = 0; i < CORE_THREAD_NUM; i++) {
                    endIndex[i] = readerStartIndex[i] + readSize;
                    // 为除了最后一个线程的线程找到有边界
                    if (i < CORE_THREAD_NUM - 1) {
                        while (mappedByteBuffer.get(endIndex[i]) != '\n') {
                            endIndex[i]--;
                        }
                        readerStartIndex[i + 1] = endIndex[i] + 1;
                    } else { //最后一个线程的有边界是整个block的右边界
                        endIndex[i] = lastLR;
                    }
                }
                for (int i = 0; i < CORE_THREAD_NUM; i++) {
                    BuildRPCMessageHandler rpcMessageHandler = readyedMessageHandlers.take();
                    // 对于第一个线程，可能要读取上一个BLOCK剩下的部分
                    if (i == 0) {
                        rpcMessageHandler.setNewByteBuff(mappedByteBuffer, remindBuffer, readerStartIndex[i], endIndex[i]);
                    } else { //其他线程都是完整的数据
                        rpcMessageHandler.setNewByteBuff(mappedByteBuffer, "", readerStartIndex[i], endIndex[i]);
                    }
                    rpcMessageHandlerPool.execute(rpcMessageHandler);
                }
                StringBuilder builder = new StringBuilder();
                lastLR += 1;
                while (lastLR < mapSize) {
                    builder.append((char) mappedByteBuffer.get(lastLR));
                    lastLR++;
                }
                remindBuffer = builder.toString();
            }

            rpcMessageHandlerPool.shutdown();
            rpcMessageHandlerPool.awaitTermination(10, TimeUnit.SECONDS);

            computeRange2Result();
            computeRange3Result();


        } catch (InterruptedException | IOException ignored) {
        } finally {
            range23ComputePool.shutdown();

            globalAverageMeter.updateTimer(PREPARETIMER);
            if (randomAccessFile != null) {
            	try {
					randomAccessFile.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
            
        }
    }

    private void computeRange3Result() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(CORE_THREAD_NUM);
        String[] keyList = range3Result.keySet().toArray(new String[0]);
        computeIdx.set(0);
        for (int i = 0; i < CORE_THREAD_NUM; i++) {
            range23ComputePool.execute(() -> {
                int workIndex = computeIdx.getAndIncrement();

                while (workIndex < keyList.length) {
                    String workKey = keyList[workIndex];

                    ConcurrentHashMap<Integer, SuccessRate> minuteSuccessRate = range3Result.get(workKey);
                    Range3Result range3Result = new Range3Result();
                    for (Entry<Integer, SuccessRate> entry :
                            minuteSuccessRate.entrySet()) {
                        int minuteTimeStamp = entry.getKey();
                        SuccessRate successRate = entry.getValue();
                        double rate = (double) successRate.success.get() / successRate.total.get();
                        range3Result.addTimeStampSuccessate(minuteTimeStamp, rate);
                    }
                    range3Result.calculatePrefixSum();
                    computedRange3Result.put(workKey, range3Result);
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
                int workIndex = computeIdx.getAndIncrement();
                StringBuilder builder = new StringBuilder();
                while (workIndex < keyList.length) {
                    int workMinuteStamp = keyList[workIndex];
                    ConcurrentHashMap<String, ConcurrentHashMap<String, Range2Result>> functionMap = range2MessageMap.get(workMinuteStamp);
                    for (Entry<String, ConcurrentHashMap<String, Range2Result>> node : functionMap.entrySet()) {
                        String key = node.getKey();
                        ConcurrentHashMap<String, Range2Result> valueMap = node.getValue();
                        Iterator<Entry<String, Range2Result>> resultIterator = valueMap.entrySet().iterator();
                        ArrayList<String> resultList = new ArrayList<>();
                        while (resultIterator.hasNext()) {
                            Range2Result resultNode = resultIterator.next().getValue();
                            builder.setLength(0);
                            builder.append(resultNode.mainIP).append(",")
                                    .append(resultNode.calledIP).append(",")
                                    .append(resultNode.computeSuccessRate(format)).append(",")
                                    .append(resultNode.computeP99());
                            resultList.add(builder.toString());
                        }
                        String date = DateUtils.minuteTimeStampToDate(workMinuteStamp);
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

        if (!globalAverageMeter.isTimerStarted(RANGE2TIMER)) {
            globalAverageMeter.startTimer(RANGE2TIMER);
            globalAverageMeter.updateTimerStart(RANGE2TIMER);
        }
        

        range2KeyBuilder.setLength(0);
        String range2Key = range2KeyBuilder.append(caller).append(responder).append(time).toString();
        ArrayList<String> result = computedRange2Result.get(range2Key);

        
        return result == null ? new ArrayList<>() : result;
    }


    @Override
    public String checkResponder(String responder, String start, String end) throws Exception {

        if (!globalAverageMeter.isTimerStarted(RANGE3TIMER)) {
        	globalAverageMeter.updateTimer(RANGE2TIMER);
            globalAverageMeter.startTimer(RANGE3TIMER);
            range3CalledTime = 0;
        }

        Range3Result range3Result = computedRange3Result.get(responder);
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

        range3CalledTime++;
        
        if(range3CalledTime >= 3e5) {
        	globalAverageMeter.updateTimer(RANGE3TIMER);
            globalAverageMeter.getStatistic();
        }

        return resultString;
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

    @Deprecated
    public void writeMinuteRPCMEssgaeToFile(int Minute) {
//    	ConcurrentHashMap<String, ConcurrentLinkedQueue<FileRPCMessage>> minuteMap = range2MessageMap.get(Minute);
//    	if(minuteMap == null) {
//    		new WriteMessageToFileThread(writeToFileHandlerPool, minuteMap, files, Minute).start();
//    	}
    }
}
