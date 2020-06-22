package com.kuaishou.kcode.handler;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.kuaishou.kcode.KcodeRpcMonitorImpl;
import com.kuaishou.kcode.model.FileRPCMessage;
import com.kuaishou.kcode.model.Range2Result;
import com.kuaishou.kcode.model.SuccessRate;

public class BuildRPCMessageHandler implements Runnable{
	
	
	private KcodeRpcMonitorImpl kcode;
	private MappedByteBuffer targetBuffer;
	private int startIndex;
	private int length;
	private ConcurrentHashMap<String, ConcurrentHashMap<Integer, SuccessRate>> range3Result;
	private ConcurrentHashMap<Integer, ConcurrentHashMap<String, ConcurrentHashMap<String, Range2Result>>> range2MessageMap;
	private Object range2lockObject;
	private Object range3lockObject;
	
	
	/**
	 * 以下为处理横跨两个Block的时候用到的变量
	 */
	//当读到的信息正好位于两个MemoryBlock中间时，iSBeyondTwoBlock = true
	private boolean iSBeyondTwoBlock = false;
	private MappedByteBuffer appendBuffer;
	/**
	 * 因为一个batch中数据基本为同一个分钟时间戳，所以做个缓存
	 */
	private int cachedMinute = -1;
	private ConcurrentHashMap<String, ConcurrentHashMap<String, Range2Result>> cachedMap;
			
	
	public BuildRPCMessageHandler(KcodeRpcMonitorImpl kcode, 
			ConcurrentHashMap<Integer, ConcurrentHashMap<String, ConcurrentHashMap<String, Range2Result>>> range2MessageMap,
			ConcurrentHashMap<String, ConcurrentHashMap<Integer, SuccessRate>> range3Result,
			Object range2lockObject, Object range3lockObject){
		this.kcode = kcode;
		this.range2MessageMap = range2MessageMap;
		this.range3Result = range3Result;
		this.range2lockObject = range2lockObject;
		this.range3lockObject = range3lockObject;
	}
	
	@Override
	public void run() {
		int[] splitIdxList = new int[6];//一条消息有6块
		int countSpiltIdx = 0;
		byte curByte;
		int messageStart = startIndex;
		//读的时候一定用 startIndex + length > targetBuffer.cap 确定处理的内容是否跨了两个block 
		//（这样做是为了在【特殊情况】最后一个batch虽然没有appendBuffer但是iSBeyondTwoBlock置位了的情况下也能正常处理数据）
		if(startIndex + length > targetBuffer.capacity()) {
			//读第一段
			int blockSize = targetBuffer.capacity();
			for(int i = startIndex; i < blockSize; i++) {
				curByte = targetBuffer.get(i);
				if(curByte == ',') {
					splitIdxList[countSpiltIdx] = i;
					countSpiltIdx++;
				}
				if(curByte == '\n') {
					buildMessage(targetBuffer, messageStart, splitIdxList);
					messageStart = i + 1; 
					countSpiltIdx = 0;
				}
			}
			//处理中间
			if(messageStart == blockSize) {//恰好读完
				messageStart = 0;
			}else {
				int findLF = 0;
				while(appendBuffer.get(findLF) != '\n') {
					findLF++;
				}
				int lengthInTargetBuffer = blockSize - messageStart;
				int bufferSize = lengthInTargetBuffer + findLF;
				
				ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
				for(int i = 0; i < bufferSize;i++) {
					if(i < lengthInTargetBuffer) {
						curByte = targetBuffer.get(messageStart + i);
					}else {
						curByte = appendBuffer.get(i - lengthInTargetBuffer);
					}
					buffer.put(curByte);
					if(curByte == ',') {
						splitIdxList[countSpiltIdx] = i;
						countSpiltIdx++;
					}
					if(curByte == '\n') {
						buildMessage(targetBuffer, messageStart, splitIdxList);
						countSpiltIdx = 0;
					}
					
				}
				messageStart = findLF + 1;
			}
			//读第二段
			for(int i = messageStart; i < startIndex + length - blockSize; i++) {
				curByte = appendBuffer.get(i);
				if(curByte == ',') {
					splitIdxList[countSpiltIdx] = i;
					countSpiltIdx++;
				}
				if(curByte == '\n') {
					buildMessage(targetBuffer, messageStart, splitIdxList);
					messageStart = i + 1; 
					countSpiltIdx = 0;
				}
			}
		}else {
			int endIndex = startIndex + length;
			for(int i = startIndex; i < endIndex; i++) {
				curByte = targetBuffer.get(i);
				if(curByte == ',') {
					splitIdxList[countSpiltIdx] = i;
					countSpiltIdx++;
				}
				if(curByte == '\n') {
					buildMessage(targetBuffer, messageStart, splitIdxList);
					messageStart = i + 1; 
					countSpiltIdx = 0;
				}
			}
		}
		
		//回调并更新
		kcode.getCurrentIdxAndUpdateIt(this);
	}
	
	
	
	private void buildMessage(ByteBuffer buffer, int messageStartIdx, int[] splitIdxList) {
		String mainService =  buildString(buffer, messageStartIdx, splitIdxList[0]);
		String mainIP = buildString(buffer, splitIdxList[0] + 1, splitIdxList[1]);
		String calledService = buildString(buffer, splitIdxList[1] + 1, splitIdxList[2]);
		String calledIP = buildString(buffer, splitIdxList[2] + 1, splitIdxList[3]);
		int isSuccess = buildBoolean(buffer, splitIdxList[3] + 1);
		int useTime = buildInt(buffer, splitIdxList[4] + 1, splitIdxList[5]);
		int secondTimeStamp = buildMinuteTimeStamp(buffer, splitIdxList[5] + 1);
		//String range3Key = new StringBuilder().append(calledService).append("-").append(secondTimeStamp).toString();
		
		//二阶段统计
		if(cachedMinute != secondTimeStamp) {
			cachedMinute = secondTimeStamp;
			cachedMap = range2MessageMap.get(secondTimeStamp);
			if(cachedMap == null) {
				synchronized (range2lockObject) {
					if(!range2MessageMap.containsKey(secondTimeStamp)) {
						cachedMap = new ConcurrentHashMap<String, ConcurrentHashMap<String, Range2Result>>();
						range2MessageMap.put(secondTimeStamp, cachedMap);
					}else {
						cachedMap = range2MessageMap.get(secondTimeStamp);
					}
				}
				//TODO 此时为读到新的一分钟时间戳的数据
			}
		}
		String range2Key = new StringBuilder().append(mainService).append('-').append(calledService).toString();
		ConcurrentHashMap<String, Range2Result> ipResult = cachedMap.get(range2Key);
		if(ipResult == null) {
			synchronized (range2lockObject) {
				if(!cachedMap.containsKey(range2Key)) {
					ipResult = new ConcurrentHashMap<String, Range2Result>();
					cachedMap.put(range2Key, ipResult);
				}else {
					ipResult = cachedMap.get(range2Key);
				}
			}
		}
		String range2IPKey = new StringBuilder().append(mainIP).append('-').append(calledIP).toString();
		Range2Result result = ipResult.get(range2IPKey);
		if(result == null) {
			synchronized (range2lockObject) {
				if(!ipResult.containsKey(range2IPKey)) {
					result = new Range2Result(mainIP, calledIP);
					ipResult.put(range2IPKey, result);
				}else {
					result = ipResult.get(range2IPKey);
				}
			}
		}
		result.fillMessage(isSuccess, useTime);
		
		//三阶段统计
		ConcurrentHashMap<Integer, SuccessRate> successRateMap = range3Result.get(calledService);
		if(successRateMap == null) {
			synchronized (range3lockObject) {
				if(!range3Result.containsKey(calledService)) {
					successRateMap = new ConcurrentHashMap<Integer, SuccessRate>();
					range3Result.put(calledService, successRateMap);
				}else {
					successRateMap = range3Result.get(calledService);
				}
			}
		}
		SuccessRate successRate = successRateMap.get(secondTimeStamp);
		if(successRate == null) {
			synchronized (range3lockObject) {
				if(!successRateMap.containsKey(secondTimeStamp)) {
					successRate = new SuccessRate();
					successRateMap.put(secondTimeStamp, successRate);
				}else {
					successRate = successRateMap.get(secondTimeStamp);
				}
			}
		}
		if(isSuccess > 0) {
			successRate.success.incrementAndGet();
		}
		successRate.total.incrementAndGet();
	}
	
	private String buildString(ByteBuffer buffer, int startIdx, int endIndex) {
		StringBuilder builder = new StringBuilder();
		for(int i = startIdx; i < endIndex; i++) {
			builder.append(buffer.get(i));
		}
		return builder.toString();
	}
	
	private int buildInt(ByteBuffer buffer, int startIdx, int endIndex) {
		int buildInt = 0;
		for(int i = startIdx; i < endIndex; i++) {
			buildInt *= 10;
			buildInt += buffer.get(i) - '0';
			
		}
		return buildInt;
	}
	
	private int buildBoolean(ByteBuffer buffer, int startIdx) {
		byte curByte = buffer.get(startIdx);
		if(curByte == 't') {
			return 1;
		}else {
			return 0;
		}
	}
	/**
	 * 返回秒级时间戳除以60的结果
	 * @param startIdx
	 * @return
	 */
	private int buildMinuteTimeStamp(ByteBuffer buffer, int startIdx) {
		int buildTimeStamp = 0;
		for(int i = startIdx; i < startIdx + 9; i++) {
			buildTimeStamp *= 10;
			buildTimeStamp += buffer.get(i) - '0';
		}
		return buildTimeStamp / 6;
	}
	
	
	
	public int getStartIndex() {
		return startIndex;
	}

	public void setStartIndex(int startIndex) {
		this.startIndex = startIndex;
	}

	public int getLength() {
		return length;
	}

	public void setLength(int length) {
		this.length = length;
	}

	public void setTargetBuffer(MappedByteBuffer targetBuffer) {
		this.targetBuffer = targetBuffer;
	}

	public boolean isBeyondTwoBlock() {
		return iSBeyondTwoBlock;
	}

	public void setiSBeyondTwoBlock(boolean iSBeyondTwoBlock) {
		this.iSBeyondTwoBlock = iSBeyondTwoBlock;
	}

	public void setAppendBuffer(MappedByteBuffer appendBuffer) {
		this.appendBuffer = appendBuffer;
	}

	
	
	
	
}
