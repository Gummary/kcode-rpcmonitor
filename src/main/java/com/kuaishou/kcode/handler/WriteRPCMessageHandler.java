package com.kuaishou.kcode.handler;

import java.nio.MappedByteBuffer;

import com.kuaishou.kcode.KcodeRpcMonitorImpl;

public class WriteRPCMessageHandler implements Runnable{
	
	
	private KcodeRpcMonitorImpl kcode;
	private MappedByteBuffer targetBuffer;
	private int startIndex;
	private int length;
	/**
	 * 以下为处理横跨两个Block的时候用到的变量
	 */
	//当读到的信息正好位于两个MemoryBlock中间时，iSBeyondTwoBlock = true
	private boolean iSBeyondTwoBlock = false;
	private MappedByteBuffer appendBuffer;
	
	
	public WriteRPCMessageHandler(KcodeRpcMonitorImpl kcode){
		this.kcode = kcode;
	}
	
	@Override
	public void run() {
		//执行逻辑
		//读的时候一定用 startIndex + length > targetBuffer.cap 确定处理的内容是否跨了两个block 
		//（这样做是为了在最后一个batch虽然没有appendBuffer但是iSBeyondTwoBlock置位了的情况下也能正常处理数据）
		
		//回调并更新
		kcode.getCurrentIdxAndUpdateIt(this);
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
