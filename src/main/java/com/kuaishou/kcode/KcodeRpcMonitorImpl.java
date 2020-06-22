package com.kuaishou.kcode;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


import com.kuaishou.kcode.handler.DirectMemoryBlockHandler;
import com.kuaishou.kcode.handler.BuildRPCMessageHandler;
import com.kuaishou.kcode.model.FileRPCMessage;
import com.kuaishou.kcode.model.SuccessRate;

/**
 * @author kcode
 * Created on 2020-06-01
 * 实际提交时请维持包名和类名不变
 */

public class KcodeRpcMonitorImpl implements KcodeRpcMonitor {
	public static final int BLOCK_SIZE = 500 * 1024 * 1024; // 每次读的block大小，多个线程同时处理该block
	public static final int MESSAGE_BATCH_SIZE = 5 * 1024 * 1024;   // 每个线程处理的数据量
	private static final int LOAD_BLOCK_THRESHOLD = 4 * 1024 * 1024;
	private static final int CORE_THREAD_NUM = 5;
	private static final int MAX_THREAD_NUM = 5;
	private static final long TIME_OUT = 80;    
	private static final ExecutorService rpcMessageHandlerPool = new ThreadPoolExecutor(
			CORE_THREAD_NUM, MAX_THREAD_NUM, TIME_OUT, TimeUnit.SECONDS, new SynchronousQueue<>());
	private static final ExecutorService blockHandlerPool = Executors.newSingleThreadExecutor();
	private static final ExecutorService writeToFileHandlerPool = Executors.newCachedThreadPool();
	public RandomAccessFile rpcDataFile;
	public FileChannel rpcDataFileChannel;
	private ConcurrentHashMap<Integer, ConcurrentHashMap<String, ConcurrentLinkedQueue<FileRPCMessage>>> range2MessageMap;
	private ConcurrentHashMap<String, SuccessRate> range3Result;
	private DirectMemoryBlockHandler directMemoryBlockHandler;
	private BuildRPCMessageHandler[] writeRPCMessageHandlers = new BuildRPCMessageHandler[CORE_THREAD_NUM];
	private MappedByteBuffer[] blocks = new MappedByteBuffer[2];
	private int MaxBlockSize = 0;//总共要读的块数
	private int curBlockIdx = 0;//当前读到的block数
	
	private int messageStartIdx = 0;//下一个MessageHandler从哪个位置开始处理
	MappedByteBuffer curBlock = null;
	private Object lockObject = new Object();//更新下一个任务时的锁
	
	private Object range2lockObject = new Object(); //
	private Object range3lockObject = new Object();
	
	private BlockingQueue<BuildRPCMessageHandler> readyedMessageHandlers = new LinkedBlockingQueue<BuildRPCMessageHandler>();
    // 不要修改访问级别
    public KcodeRpcMonitorImpl() {
    	range3Result = new ConcurrentHashMap<String, SuccessRate>();
    	for(int i = 0; i < writeRPCMessageHandlers.length; i++) {
    		writeRPCMessageHandlers[i] = new BuildRPCMessageHandler(this, range2MessageMap, range3Result, range2lockObject, range3lockObject);
    	}
    }

    public void prepare(String path) {
    	RandomAccessFile randomAccessFile;
		try {
			randomAccessFile = new RandomAccessFile(path, "r");
			directMemoryBlockHandler = new DirectMemoryBlockHandler(this, randomAccessFile.getChannel(), 0, BLOCK_SIZE);
			Future<MappedByteBuffer> future = blockHandlerPool.submit(directMemoryBlockHandler);
			this.rpcDataFile = randomAccessFile;
			this.rpcDataFileChannel = randomAccessFile.getChannel();
			long fileSize = randomAccessFile.length();
			//下取整
			MaxBlockSize = (int)(fileSize / BLOCK_SIZE);
			//存在剩余 -> block数 + 1
			MaxBlockSize = fileSize % BLOCK_SIZE == 0 ? MaxBlockSize : MaxBlockSize + 1;
			long curTimestamp = System.currentTimeMillis();
			blocks[0] = future.get();
			System.out.println(System.currentTimeMillis() - curTimestamp);//TEST
			
			curBlock = blocks[0];//当前MessageHandler处理的块的位置
			for (int i = 0; i < writeRPCMessageHandlers.length; i++) {
				//TODO 是否需要判断当前block可读长度 > 这个Block要工作的长度？
				getCurrentIdxAndUpdateIt(writeRPCMessageHandlers[i]);
			}
			while(curBlockIdx < MaxBlockSize) {
				BuildRPCMessageHandler writeRPCMessageHandler = readyedMessageHandlers.poll();
				if(writeRPCMessageHandler.isBeyondTwoBlock()) {
					curBlockIdx++;
				}
				rpcMessageHandlerPool.submit(writeRPCMessageHandler);
				//异步任务：当目前block处理字节数大于阈值时，读取下一个block
				if(writeRPCMessageHandler.getStartIndex() > LOAD_BLOCK_THRESHOLD) { //需要加载下一个block的数据
					directMemoryBlockHandler.setStartPosition(curBlockIdx * BLOCK_SIZE);
					if(curBlockIdx < MaxBlockSize - 1) {
						directMemoryBlockHandler.setLength(BLOCK_SIZE);
					}else {
						directMemoryBlockHandler.setLength((int)(fileSize - curBlockIdx * BLOCK_SIZE));
					}
					blockHandlerPool.submit(directMemoryBlockHandler);
				}
			}
		} catch (InterruptedException | ExecutionException | IOException e) {
			System.out.println(e.getMessage());
		}
    	
    }

    public List<String> checkPair(String caller, String responder, String time) {
        return new ArrayList<String>();
    }

    public String checkResponder(String responder, String start, String end) {
        return "0.00%";
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
    		if(messageStartIdx >= curBlock.capacity()) {//特例1：线程没有必要继续处理数据了
    			System.out.println("Done!");
    			return ;
    		}
    		int nextStartIdx = messageStartIdx + MESSAGE_BATCH_SIZE;
    		if(nextStartIdx >= curBlock.capacity() && curBlockIdx == MaxBlockSize - 1) {//特例2：该batch是最后一个block数据的最后一块
    			writeRPCMessageHandler.setiSBeyondTwoBlock(true);//置位，目的是让prepare中能跳出循环
    			writeRPCMessageHandler.setAppendBuffer(null);
    			writeRPCMessageHandler.setLength(curBlock.capacity() + 1 - messageStartIdx);
    			readyedMessageHandlers.add(writeRPCMessageHandler);
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
    		nextStartIdx++;
    		if(isBeyondTwoBatch) {
    			writeRPCMessageHandler.setiSBeyondTwoBlock(true);
    			writeRPCMessageHandler.setAppendBuffer(curBlock);
    			writeRPCMessageHandler.setLength(nextStartIdx + BLOCK_SIZE - messageStartIdx);
    		}else {
    			writeRPCMessageHandler.setiSBeyondTwoBlock(false);
    			writeRPCMessageHandler.setAppendBuffer(null);//目的是尽快让GC回收区域
    			writeRPCMessageHandler.setLength(nextStartIdx - messageStartIdx);
    		}
    		messageStartIdx = nextStartIdx;
    		readyedMessageHandlers.add(writeRPCMessageHandler);
		}
    	
    }
    public void setNextBlock(MappedByteBuffer block) {
    	blocks[1] = block;
    }
    
    public void writeMinuteRPCMEssgaeToFile(int Minute) {
    	
    }
}
