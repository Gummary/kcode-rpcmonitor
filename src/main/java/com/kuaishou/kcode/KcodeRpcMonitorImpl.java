package com.kuaishou.kcode;

import com.kuaishou.kcode.handler.BuildRPCMessageHandler;
import com.kuaishou.kcode.handler.DirectMemoryBlockHandler;
import com.kuaishou.kcode.model.GlobalAverageMeter;
import com.kuaishou.kcode.model.Range2Result;
import com.kuaishou.kcode.model.SuccessRate;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.math.RoundingMode;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.text.DecimalFormat;
import java.text.ParseException;
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
	public static final int MESSAGE_BATCH_SIZE = 5 * 1024 * 1024;
	private static final int LOAD_BLOCK_THRESHOLD = 400 * 1024 * 1024;
	private static final int CORE_THREAD_NUM = 7;
	private static final int MAX_THREAD_NUM = 5;
	private static final long TIME_OUT = 80;    
	private static final ExecutorService rpcMessageHandlerPool = Executors.newFixedThreadPool(CORE_THREAD_NUM);//new ThreadPoolExecutor(CORE_THREAD_NUM, MAX_THREAD_NUM, TIME_OUT, TimeUnit.SECONDS, new SynchronousQueue<>());
	private static final ExecutorService blockHandlerPool = Executors.newSingleThreadExecutor();
//	private static final ExecutorService writeToFileHandlerPool = Executors.newCachedThreadPool();
	public RandomAccessFile rpcDataFile;
	public FileChannel rpcDataFileChannel;
	private ConcurrentHashMap<Integer, ConcurrentHashMap<String, ConcurrentHashMap<String, Range2Result>>> range2MessageMap = new ConcurrentHashMap<Integer, ConcurrentHashMap<String,ConcurrentHashMap<String,Range2Result>>>();
	private ConcurrentHashMap<String, RandomAccessFile> files = new ConcurrentHashMap<String, RandomAccessFile>();
	private ConcurrentHashMap<String, ConcurrentHashMap<Integer, SuccessRate>> range3Result;
	private DirectMemoryBlockHandler directMemoryBlockHandler;
	private BuildRPCMessageHandler[] writeRPCMessageHandlers = new BuildRPCMessageHandler[CORE_THREAD_NUM];
	private MappedByteBuffer[] blocks = new MappedByteBuffer[2];
	private BlockingQueue<BuildRPCMessageHandler> readyedMessageHandlers = new LinkedBlockingQueue<BuildRPCMessageHandler>();
	private int MaxBlockSize = 0;//总共要读的块数
	private int curBlockIdx = 0;//当前读到的block数
	private int curHandledBlockIdx = 0;
	private int messageStartIdx = 0;//下一个MessageHandler从哪个位置开始处理
	MappedByteBuffer curBlock = null;
	private Object lockObject = new Object();//更新下一个任务时的锁
	private Object range2lockObject = new Object(); 
	private Object range3lockObject = new Object();

	private int cachedMinuteTimeStamp = -1;
	private ConcurrentHashMap<String, ConcurrentHashMap<String, Range2Result>> cachedFunctionMap = null;

	private static GlobalAverageMeter globalAverageMeter = new GlobalAverageMeter();
	//利用线程池优化2阶段
	private static ExecutorService range2ComputePool  = Executors.newFixedThreadPool(CORE_THREAD_NUM);
	private static AtomicInteger computeIdx = new AtomicInteger();
	private static int[] keyList;
	private static ConcurrentHashMap<String, ArrayList<String>> computedRange2Result = new ConcurrentHashMap<String, ArrayList<String>>();

	// 不要修改访问级别
    public KcodeRpcMonitorImpl() {

    	range3Result = new ConcurrentHashMap<String, ConcurrentHashMap<Integer, SuccessRate>>();
    	for(int i = 0; i < writeRPCMessageHandlers.length; i++) {
    		writeRPCMessageHandlers[i] = new BuildRPCMessageHandler(this, range2MessageMap, range3Result, range2lockObject, range3lockObject);
    	}
    }



    @Override
	public void prepare(String path) {
    	globalAverageMeter.startPrepareTotalTime();
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
			MaxBlockSize = (int)(fileSize / BLOCK_SIZE);
			//存在剩余 -> block数 + 1
			MaxBlockSize = fileSize % BLOCK_SIZE == 0 ? MaxBlockSize : MaxBlockSize + 1;
			//long curTimestamp = System.currentTimeMillis();
			blocks[0] = future.get();
			//System.out.println(System.currentTimeMillis() - curTimestamp);//TEST
			
			curBlock = blocks[0];//当前MessageHandler处理的块的位置
			for (int i = 0; i < writeRPCMessageHandlers.length; i++) {
				//TODO 是否需要判断当前block可读长度 > 这个Block要工作的长度？
				getCurrentIdxAndUpdateIt(writeRPCMessageHandlers[i]);
				
			}
			
			while(curBlockIdx < MaxBlockSize) {
				BuildRPCMessageHandler writeRPCMessageHandler = readyedMessageHandlers.take();
				if(writeRPCMessageHandler.isBeyondTwoBlock()) {
					curBlockIdx++;
					needReadNext = true;
				}
				rpcMessageHandlerPool.execute(writeRPCMessageHandler);
				//异步任务：当目前block处理字节数大于阈值时，读取下一个block
				//System.out.println(writeRPCMessageHandler.printInfo());
				if(writeRPCMessageHandler.getStartIndex() > LOAD_BLOCK_THRESHOLD && needReadNext 
						&& !writeRPCMessageHandler.isBeyondTwoBlock()) { //需要加载下一个block的数据
					needReadNext = false;
//					System.out.println(String.format("current ready to read blockIdx:%d, total block:%d ", curBlockIdx + 1, MaxBlockSize));
					long startPosition = (curBlockIdx + 1L) * BLOCK_SIZE;
					if(startPosition >= fileSize) {
						continue;
					}
					directMemoryBlockHandler.setStartPosition(startPosition);
					if(fileSize - startPosition > BLOCK_SIZE) {
						directMemoryBlockHandler.setLength(BLOCK_SIZE);
					}else {
						directMemoryBlockHandler.setLength(fileSize - startPosition);
					}
					blockHandlerPool.submit(directMemoryBlockHandler);
				}
			}
			rpcMessageHandlerPool.shutdown();
			rpcMessageHandlerPool.awaitTermination(10000, TimeUnit.MILLISECONDS);
			keyList = new int[range2MessageMap.size()];
			Enumeration<Integer> enumeration = range2MessageMap.keys();
			int i = 0;
			while(enumeration.hasMoreElements()) {
				keyList[i++] = enumeration.nextElement();
//				System.out.println(keyList[i-1]);
			}
			for(i = 0; i < CORE_THREAD_NUM; i++) {
				range2ComputePool.execute(new Runnable() {
					
					@Override
					public void run() {
						int workIndex = computeIdx.getAndIncrement();
						DecimalFormat format = new DecimalFormat("#.00");
				    	format.setRoundingMode(RoundingMode.DOWN);
						while(workIndex < keyList.length) {
							int workMinuteStamp = keyList[workIndex];
							ConcurrentHashMap<String, ConcurrentHashMap<String, Range2Result>> functionMap = range2MessageMap.get(workMinuteStamp);
							Iterator<Entry<String, ConcurrentHashMap<String, Range2Result>>> iterator = functionMap.entrySet().iterator();
							while(iterator.hasNext()) {
								Entry<String, ConcurrentHashMap<String, Range2Result>> node = iterator.next();
								String key = node.getKey();
								ConcurrentHashMap<String, Range2Result> valueMap = node.getValue();
								Iterator<Entry<String, Range2Result>> resultIterator = valueMap.entrySet().iterator();
								ArrayList<String> resultList = new ArrayList<String>();
								while(resultIterator.hasNext()) {
									Range2Result resultNnode = resultIterator.next().getValue();
//									System.out.println(String.format("mainIP:%s,calledIP:%s", node.mainIP, node.calledIP));
									StringBuilder builder = new StringBuilder();
									 
									 
									builder.append(resultNnode.mainIP).append(',')
										.append(resultNnode.calledIP).append(',')
										.append(resultNnode.computeSuccessRate(format)).append(',')
										.append(resultNnode.computeP99());
									resultList.add(builder.toString());
								}
								computedRange2Result.put(workMinuteStamp+key, resultList);
							}	
							workIndex = computeIdx.getAndIncrement();
						}
						
					}
				});
			}
		} catch (InterruptedException | ExecutionException | IOException e) {
//			System.out.println(e.getMessage());
		} finally {
			rpcMessageHandlerPool.shutdown();
			blockHandlerPool.shutdown();

			globalAverageMeter.updatePrepareTotalTime();
			globalAverageMeter.startStage2Query();
		}
    }

    @Override
	public List<String> checkPair(String caller, String responder, String time) {

    	SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
    	ArrayList<String> result = new ArrayList<String>();
    	DecimalFormat format = new DecimalFormat("#.00");
    	format.setRoundingMode(RoundingMode.DOWN);
    	String range2Key = new StringBuilder().append(caller).append('-').append(responder).toString();
    	try {
			//优化：先判断是否已经计算过结果了
    		int minuteTimeStamp = (int)(simpleDateFormat.parse(time).getTime() / 60000);
			ConcurrentHashMap<String, ConcurrentHashMap<String, Range2Result>> functionMap;
			String computedKey = minuteTimeStamp + range2Key;
			if(computedRange2Result.contains(computedKey)) {
				return computedRange2Result.get(computedKey);
			}
			
			
			if(minuteTimeStamp != cachedMinuteTimeStamp) {
				functionMap = range2MessageMap.get(minuteTimeStamp);
				cachedMinuteTimeStamp = minuteTimeStamp;
				cachedFunctionMap = functionMap;
			} else {
				functionMap = cachedFunctionMap;
			}


			if(functionMap != null) {
				
				ConcurrentHashMap<String, Range2Result> ipMaps = functionMap.get(range2Key);
				if(ipMaps != null) {
					Iterator<Entry<String, Range2Result>> iterator = ipMaps.entrySet().iterator();
					while(iterator.hasNext()) {
						Range2Result node = iterator.next().getValue();
//						System.out.println(String.format("mainIP:%s,calledIP:%s", node.mainIP, node.calledIP));
						StringBuilder builder = new StringBuilder();
						 
						 
						builder.append(node.mainIP).append(',')
							.append(node.calledIP).append(',')
							.append(node.computeSuccessRate(format)).append(',')
							.append(node.computeP99());
						result.add(builder.toString());
					}
				}
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}finally {
			range2ComputePool.shutdownNow();
		}
    	globalAverageMeter.updateStage2Query();
    	return result;
    }

    @Override
	public String checkResponder(String responder, String start, String end) throws Exception {

    	globalAverageMeter.getStatistic();

    	SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
    	DecimalFormat decimalFormat = new DecimalFormat("#.00");
    	String result = ".00%";
    	try {
			int startTimeStamp = (int)(simpleDateFormat.parse(start).getTime() / 60000);
			int endTimeStamp = (int)(simpleDateFormat.parse(end).getTime() / 60000);
			ConcurrentHashMap<Integer, SuccessRate> successRateMap = range3Result.get(responder);
			if(successRateMap == null) {
			    return "-1.00%";
			}
			double rate = 0.0d;
			int count = 0;
			for (int i = startTimeStamp; i <= endTimeStamp; i++) {
				SuccessRate successRate = successRateMap.get(i);
				if(successRate != null){
					rate += (double)successRate.success.get() / successRate.total.get();
					count++;
				}
			}
			double resultDouble = rate * 100 / count;
			String resultString = decimalFormat.format(resultDouble);
			if(resultDouble - 0.0d >= 1e-4) {
				result = resultString + "%";
			}
			
		} catch (ParseException e) {
			e.printStackTrace();
		}
        return result;
    }
    
    /**
     * 当前WriteRPCMessageHandler获得自己的读取任务
     * 设置该writeRPCMessageHandler在当前block中处理的数据范围
     * 如果这个读取任务超过两个Block，那么更新blocks数组的设置（1 -> 0后1 -> null）及WriteRPCMessageHandler的标志位true
     * @param writeRPCMessageHandler
     */
    public void getCurrentIdxAndUpdateIt(BuildRPCMessageHandler writeRPCMessageHandler) { 
    	synchronized (lockObject) {
    		boolean isBeyondTwoBatch = false;
    		writeRPCMessageHandler.setTargetBuffer(curBlock);
    		writeRPCMessageHandler.setStartIndex(messageStartIdx);
    		if(curHandledBlockIdx == MaxBlockSize - 1 && messageStartIdx >= curBlock.capacity()) {//特例1：线程没有必要继续处理数据了
//				System.out.println("Done!");
    			return ;
    		}
    		int nextStartIdx = messageStartIdx + MESSAGE_BATCH_SIZE;
    		if(nextStartIdx >= curBlock.capacity() && curHandledBlockIdx == MaxBlockSize - 1) {//特例2：该batch是最后一个block数据的最后一块
    			writeRPCMessageHandler.setiSBeyondTwoBlock(true);//置位，目的是让prepare中能跳出循环
    			writeRPCMessageHandler.setAppendBuffer(null);
    			writeRPCMessageHandler.setLength(curBlock.capacity() - messageStartIdx);
    			readyedMessageHandlers.add(writeRPCMessageHandler);
    			messageStartIdx = curBlock.capacity();
    			return ;
    		}
    		
    		if(nextStartIdx >= BLOCK_SIZE) {
    			isBeyondTwoBatch = true;
    			nextStartIdx %= BLOCK_SIZE;
    			
    			curBlock = blocks[1];
    			blocks[0] = blocks[1];
    			blocks[1] = null;
    		}
    		while(curBlock.get(nextStartIdx) != (byte)'\n') {
    			nextStartIdx++;
    			if(nextStartIdx >= BLOCK_SIZE) {
    				isBeyondTwoBatch = true;
    				nextStartIdx = 0;
        			curBlock = blocks[1];
        			blocks[0] = blocks[1];
        			blocks[1] = null;
    			
    			}
    		}
    		// next 回车位置，自增后为下一个batch的开始
    		nextStartIdx++;
    		if(isBeyondTwoBatch) {
    			writeRPCMessageHandler.setiSBeyondTwoBlock(true);
    			writeRPCMessageHandler.setAppendBuffer(curBlock);
    			writeRPCMessageHandler.setLength(nextStartIdx + (int)BLOCK_SIZE - messageStartIdx);
    			curHandledBlockIdx++;
    		}else {
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
