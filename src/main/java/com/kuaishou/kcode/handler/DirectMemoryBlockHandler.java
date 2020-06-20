package com.kuaishou.kcode.handler;



import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.Callable;

import com.kuaishou.kcode.KcodeRpcMonitorImpl;


public class DirectMemoryBlockHandler implements Callable<MappedByteBuffer>{
	private KcodeRpcMonitorImpl kcode;
	private FileChannel fileChannel; 
	private int startPosition; 
	private int length; 

	public DirectMemoryBlockHandler(KcodeRpcMonitorImpl kcode, FileChannel fileChannel, int startPosition, int length) {
		super();
		this.kcode = kcode;
		this.fileChannel = fileChannel;
		this.startPosition = startPosition;
		this.length = length;
	}

	@Override
	public MappedByteBuffer call() throws Exception {
		MappedByteBuffer block = fileChannel.map(FileChannel.MapMode.READ_ONLY, startPosition, length);
		kcode.setNextBlock(block);
		return block;
	}


	public void setStartPosition(int startPosition) {
		this.startPosition = startPosition;
	}

	public void setLength(int length) {
		this.length = length;
	}
	
}
