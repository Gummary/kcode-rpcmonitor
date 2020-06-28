package com.kuaishou.kcode.model;

@Deprecated
public class FileRPCMessage implements Comparable<FileRPCMessage>{
	public int useTime;
	public String message;
	
	public FileRPCMessage(int useTime, String mainService, String mainIP, String calledIP, int isSuccess) {
		super();
		this.useTime = useTime;
		StringBuilder builder = new StringBuilder();
		builder.append(useTime).append(',').append(mainService).append(',')
			.append(mainIP).append('-').append(calledIP).append(',')
			.append(isSuccess);
		this.message = builder.toString();
	}

	

	@Override
	public int compareTo(FileRPCMessage o) {
		
		return this.useTime - o.useTime;
	}
	
	
	
}
