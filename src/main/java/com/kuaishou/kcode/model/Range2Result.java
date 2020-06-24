package com.kuaishou.kcode.model;


import java.text.DecimalFormat;
import java.util.Comparator;
import java.util.concurrent.PriorityBlockingQueue;

public class Range2Result {
	public SuccessRate successRate;
	public PriorityBlockingQueue<Integer> queue;
	public String mainIP;
	public String calledIP;
	
	
	
	public Range2Result(String mainIP, String calledIP) {
		super();
		this.mainIP = mainIP;
		this.calledIP = calledIP;
		this.queue = new PriorityBlockingQueue<Integer>(200, new Comparator<Integer>() {

			@Override
			public int compare(Integer o1, Integer o2) {
				return o2 - o1;
			}
		});
		this.successRate = new SuccessRate();
	}



	public void fillMessage(int isSuccess, int useTime) {
		queue.add(useTime);
		if(isSuccess > 0) {
			successRate.success.incrementAndGet();
		}
		successRate.total.incrementAndGet();
	}
	
	public String computeSuccessRate(DecimalFormat format) {
		return successRate.computeSuccessRate(format);
	}
	public int computeP99() {
		int index = queue.size() - (int) Math.ceil((double)queue.size()*0.99)-1;
		int count = 0;
		int ans = -1;
		System.out.println(String.format("size:%d,count:%d,index:%s", queue.size(), count, index));
		while(count <= index) {
			ans = queue.poll();
			count++;
		}

		return ans;
	}




}
