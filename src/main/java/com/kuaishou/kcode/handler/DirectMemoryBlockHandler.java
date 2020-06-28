package com.kuaishou.kcode.handler;



import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.Callable;

import com.kuaishou.kcode.KcodeRpcMonitorImpl;


public class DirectMemoryBlockHandler implements Callable<MappedByteBuffer>{
	private KcodeRpcMonitorImpl kcode;
	private FileChannel fileChannel; 
	private long startPosition; 
	private long length; 

	public DirectMemoryBlockHandler(KcodeRpcMonitorImpl kcode, FileChannel fileChannel, long startPosition, long length) {
		super();
		this.kcode = kcode;
		this.fileChannel = fileChannel;
		this.startPosition = startPosition;
		this.length = length;
	}

	@Override
	public MappedByteBuffer call() throws Exception {
		long start = System.currentTimeMillis();
		MappedByteBuffer block = null;
//		System.out.println(String.format("start pos:%d, length:%d", startPosition, length));
		try {
			block = fileChannel.map(FileChannel.MapMode.READ_ONLY, startPosition, length);
//			kcode.setNextBlock(block);
		}catch (Exception e) {
			e.printStackTrace();
		}
		
//		System.out.println("use time:"+ (System.currentTimeMillis() - start));
		return block;
	}


	public void setStartPosition(long startPosition) {
		this.startPosition = startPosition;
	}

	public void setLength(long length) {
		this.length = length;
	}
	
}
