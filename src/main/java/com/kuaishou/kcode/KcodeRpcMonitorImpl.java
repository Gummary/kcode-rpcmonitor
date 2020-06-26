package com.kuaishou.kcode;

import com.kuaishou.kcode.handler.BuildRPCMessageHandler;
import com.kuaishou.kcode.handler.DirectMemoryBlockHandler;
import com.kuaishou.kcode.model.*;

import java.io.IOException;
import java.io.RandomAccessFile;
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

    public static final long BLOCK_SIZE = 500 * 1024 * 1024;
    public static final int MESSAGE_BATCH_SIZE = 50 * 1024 * 1024;
    private static final int LOAD_BLOCK_THRESHOLD = 400 * 1024 * 1024;
    private static final int CORE_THREAD_NUM = 8;
    private static final ExecutorService rpcMessageHandlerPool = Executors.newFixedThreadPool(CORE_THREAD_NUM);//new ThreadPoolExecutor(CORE_THREAD_NUM, MAX_THREAD_NUM, TIME_OUT, TimeUnit.SECONDS, new SynchronousQueue<>());
    private static final ExecutorService blockHandlerPool = Executors.newSingleThreadExecutor();
    public RandomAccessFile rpcDataFile;
    public FileChannel rpcDataFileChannel;
    private ConcurrentHashMap<Integer, ConcurrentHashMap<String, ConcurrentHashMap<String, Range2Result>>> range2MessageMap = new ConcurrentHashMap<Integer, ConcurrentHashMap<String, ConcurrentHashMap<String, Range2Result>>>();
    private ConcurrentHashMap<String, ConcurrentHashMap<Integer, SuccessRate>> range3Result;
    private DirectMemoryBlockHandler directMemoryBlockHandler;
    private BuildRPCMessageHandler[] writeRPCMessageHandlers = new BuildRPCMessageHandler[CORE_THREAD_NUM];
    private final MappedByteBuffer[] blocks = new MappedByteBuffer[2];
    private final BlockingQueue<BuildRPCMessageHandler> readyedMessageHandlers = new LinkedBlockingQueue<>();
    private int MaxBlockSize = 0;//总共要读的块数
    private int curBlockIdx = 0;//当前读到的block数
    private int curHandledBlockIdx = 0;
    private int messageStartIdx = 0;//下一个MessageHandler从哪个位置开始处理
    MappedByteBuffer curBlock = null;
    private final Object lockObject = new Object();//更新下一个任务时的锁
    private static StringBuilder stringBuilder = new StringBuilder(100);

    private static DecimalFormat format;


//    private static GlobalAverageMeter globalAverageMeter = new GlobalAverageMeter();
    //利用线程池优化2,3阶段
    private static final ExecutorService range23ComputePool = Executors.newFixedThreadPool(CORE_THREAD_NUM);
    private static final AtomicInteger computeIdx = new AtomicInteger();
    private static final ConcurrentHashMap<String, ArrayList<String>> computedRange2Result = new ConcurrentHashMap<>(500000);
    private static final ConcurrentHashMap<String, ArrayList<Range3Result>> computedRange3Result = new ConcurrentHashMap<>(500000);

    //TEST
    // 不要修改访问级别
    public KcodeRpcMonitorImpl() {
        format = new DecimalFormat("#.00");
        format.setRoundingMode(RoundingMode.DOWN);
        range3Result = new ConcurrentHashMap<>();
        for (int i = 0; i < writeRPCMessageHandlers.length; i++) {
            writeRPCMessageHandlers[i] = new BuildRPCMessageHandler(this, range2MessageMap, range3Result);
        }
    }


    @Override
    public void prepare(String path) {
//    	globalAverageMeter.startPrepareTotalTime();
        RandomAccessFile randomAccessFile;
        boolean needReadNext = true;
        try {
            randomAccessFile = new RandomAccessFile(path, "r");
            directMemoryBlockHandler = new DirectMemoryBlockHandler(this, randomAccessFile.getChannel(), 0, BLOCK_SIZE);
            Future<MappedByteBuffer> future = blockHandlerPool.submit(directMemoryBlockHandler);
            this.rpcDataFile = randomAccessFile;
            this.rpcDataFileChannel = randomAccessFile.getChannel();
            long fileSize = randomAccessFile.length();
//			System.out.println(String.format("file length:%d", fileSize));
            //下取整
            MaxBlockSize = (int) (fileSize / BLOCK_SIZE);
            //存在剩余 -> block数 + 1
            MaxBlockSize = fileSize % BLOCK_SIZE == 0 ? MaxBlockSize : MaxBlockSize + 1;
            //long curTimestamp = System.currentTimeMillis();
            blocks[0] = future.get();
            //System.out.println(System.currentTimeMillis() - curTimestamp);//TEST

            curBlock = blocks[0];//当前MessageHandler处理的块的位置
            for (BuildRPCMessageHandler rpcMessageHandler : writeRPCMessageHandlers) {
                //TODO 是否需要判断当前block可读长度 > 这个Block要工作的长度？
                getCurrentIdxAndUpdateIt(rpcMessageHandler);

            }

            while (curBlockIdx < MaxBlockSize) {
                BuildRPCMessageHandler writeRPCMessageHandler = readyedMessageHandlers.take();
                if (writeRPCMessageHandler.isBeyondTwoBlock()) {
                    curBlockIdx++;
                    needReadNext = true;
                }
                rpcMessageHandlerPool.execute(writeRPCMessageHandler);
                //异步任务：当目前block处理字节数大于阈值时，读取下一个block
                //System.out.println(writeRPCMessageHandler.printInfo());
                if (writeRPCMessageHandler.getStartIndex() > LOAD_BLOCK_THRESHOLD && needReadNext
                        && !writeRPCMessageHandler.isBeyondTwoBlock()) { //需要加载下一个block的数据
                    needReadNext = false;
//					System.out.println(String.format("current ready to read blockIdx:%d, total block:%d ", curBlockIdx + 1, MaxBlockSize));
                    long startPosition = (curBlockIdx + 1L) * BLOCK_SIZE;
                    if (startPosition >= fileSize) {
                        continue;
                    }
                    directMemoryBlockHandler.setStartPosition(startPosition);
                    directMemoryBlockHandler.setLength(Math.min(fileSize - startPosition, BLOCK_SIZE));
                    blockHandlerPool.submit(directMemoryBlockHandler);
                }
            }
            rpcMessageHandlerPool.shutdown();
            rpcMessageHandlerPool.awaitTermination(20000, TimeUnit.MILLISECONDS);

            computeRange2Result();
            computeRange3Result();

//            Thread.sleep(20*1000);

        } catch (InterruptedException | ExecutionException | IOException ignored) {
        } finally {
            rpcMessageHandlerPool.shutdown();
            blockHandlerPool.shutdown();
            range23ComputePool.shutdown();


//			globalAverageMeter.updatePrepareTotalTime();
//			globalAverageMeter.startStage2Query();
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
                    ArrayList<Range3Result> currentKeyResults = new ArrayList<>();
                    for (Entry<Integer, SuccessRate> entry :
                            minuteSuccessRate.entrySet()) {
                        int minuteTimeStamp = entry.getKey();
                        String dateTimeStamp = DateUtils.minuteTimeStampToDate(minuteTimeStamp);
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
    	stringBuilder.setLength(0);
        String range2Key = stringBuilder.append(caller).append("-").append(responder).append(time).toString();
        ArrayList<String> result = computedRange2Result.get(range2Key);
//        globalAverageMeter.updateStage2Query();
        return result == null ? new ArrayList<>() : result;
    }


    @Override
    public String checkResponder(String responder, String start, String end) throws Exception {
//        globalAverageMeter.getStatistic();

        ArrayList<Range3Result> results = computedRange3Result.get(responder);
        if (results == null) {
            return "-1.00%";
        }
        double rate = 0.0d;
        int count = 0;
        String result = ".00%";
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
        if (resultDouble - 0.0d >= 1e-4) {
            result = resultString + "%";
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
        synchronized (lockObject) {
            boolean isBeyondTwoBatch = false;
            writeRPCMessageHandler.setTargetBuffer(curBlock);
            writeRPCMessageHandler.setStartIndex(messageStartIdx);
            if (curHandledBlockIdx == MaxBlockSize - 1 && messageStartIdx >= curBlock.capacity()) {//特例1：线程没有必要继续处理数据了
//				System.out.println("Done!");
                return;
            }
            int nextStartIdx = messageStartIdx + MESSAGE_BATCH_SIZE;
            if (nextStartIdx >= curBlock.capacity() && curHandledBlockIdx == MaxBlockSize - 1) {//特例2：该batch是最后一个block数据的最后一块
                writeRPCMessageHandler.setiSBeyondTwoBlock(true);//置位，目的是让prepare中能跳出循环
                writeRPCMessageHandler.setAppendBuffer(null);
                writeRPCMessageHandler.setLength(curBlock.capacity() - messageStartIdx);
                readyedMessageHandlers.add(writeRPCMessageHandler);
                messageStartIdx = curBlock.capacity();
                return;
            }

            if (nextStartIdx >= BLOCK_SIZE) {
                isBeyondTwoBatch = true;
                nextStartIdx %= BLOCK_SIZE;

                curBlock = blocks[1];
                blocks[0] = blocks[1];
                blocks[1] = null;
            }
            while (curBlock.get(nextStartIdx) != (byte) '\n') {
                nextStartIdx++;
                if (nextStartIdx >= BLOCK_SIZE) {
                    isBeyondTwoBatch = true;
                    nextStartIdx = 0;
                    curBlock = blocks[1];
                    blocks[0] = blocks[1];
                    blocks[1] = null;

                }
            }
            // next 回车位置，自增后为下一个batch的开始
            nextStartIdx++;
            if (isBeyondTwoBatch) {
                writeRPCMessageHandler.setiSBeyondTwoBlock(true);
                writeRPCMessageHandler.setAppendBuffer(curBlock);
                writeRPCMessageHandler.setLength(nextStartIdx + (int) BLOCK_SIZE - messageStartIdx);
                curHandledBlockIdx++;
            } else {
                writeRPCMessageHandler.setiSBeyondTwoBlock(false);
                writeRPCMessageHandler.setAppendBuffer(null);//目的是尽快让GC回收区域
                writeRPCMessageHandler.setLength(nextStartIdx - messageStartIdx);
            }
            messageStartIdx = nextStartIdx;
            try {
                readyedMessageHandlers.put(writeRPCMessageHandler);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void setNextBlock(MappedByteBuffer block) {
        blocks[1] = block;
    }

    @Deprecated
    public void writeMinuteRPCMEssgaeToFile(int Minute) {
//    	ConcurrentHashMap<String, ConcurrentLinkedQueue<FileRPCMessage>> minuteMap = range2MessageMap.get(Minute);
//    	if(minuteMap == null) {
//    		new WriteMessageToFileThread(writeToFileHandlerPool, minuteMap, files, Minute).start();
//    	}
    }
}
