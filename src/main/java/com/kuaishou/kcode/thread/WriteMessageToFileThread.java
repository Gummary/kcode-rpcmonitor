package com.kuaishou.kcode.thread;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import com.kuaishou.kcode.model.FileRPCMessage;


@Deprecated
public class WriteMessageToFileThread extends Thread{
	public static final String FILE_ROOT_PATH = "/tmp/";
 	private ExecutorService pool;
	private ConcurrentHashMap<String, ConcurrentLinkedQueue<FileRPCMessage>> messages;
	private ConcurrentHashMap<String, RandomAccessFile> files;
	private int minuteTimeStamp;
	
	public WriteMessageToFileThread(ExecutorService pool,
			ConcurrentHashMap<String, ConcurrentLinkedQueue<FileRPCMessage>> messages,
			ConcurrentHashMap<String, RandomAccessFile> files,
			int minuteTimeStamp) {
		super();
		this.pool = pool;
		this.messages = messages;
		this.files = files;
		this.minuteTimeStamp = minuteTimeStamp;
	}


	@Override
	public void run() {
		Iterator<Entry<String, ConcurrentLinkedQueue<FileRPCMessage>>> it = messages.entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, ConcurrentLinkedQueue<FileRPCMessage>> node = it.next();
			String key = node.getKey();
			ConcurrentLinkedQueue<FileRPCMessage> value = node.getValue();
			String fileName = new StringBuilder().append(minuteTimeStamp).append('-').append(key).toString();
			RandomAccessFile file;
			FileChannel fileChannel = null;
			try {
				file = new RandomAccessFile(new File(FILE_ROOT_PATH + fileName), "rw");
				fileChannel = file.getChannel();
				files.put(fileName, file);
				value.stream().sorted();
				StringBuilder builder = new StringBuilder();
				for (FileRPCMessage fileRPCMessage : value) {
					builder.append(fileRPCMessage.message).append('\n');
				}
				ByteBuffer buf = ByteBuffer.allocate(builder.capacity());
				while(buf.hasRemaining()) {
					fileChannel.write(buf);
				}
				
			} catch (IOException e) {
				e.printStackTrace();
			}finally {
				try {
					if(fileChannel != null)
						fileChannel.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			
					
			
		}
	}
	
	
	

}
