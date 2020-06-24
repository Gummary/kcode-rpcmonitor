package com.kuaishou.kcode.model;


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
		this.queue = new PriorityBlockingQueue<Integer>();
		this.successRate = new SuccessRate();
	}



	public void fillMessage(int isSuccess, int useTime) {
		queue.add(useTime);
		if(isSuccess > 0) {
			successRate.success.incrementAndGet();
		}
		successRate.total.incrementAndGet();
	}
	
	public String computeSuccessRate() {
		return successRate.computeSuccessRate();
	}
	public int computeP99() {
		int index = queue.size() - (int) Math.ceil((double)queue.size()*0.99)-1;
		int count = 0;
		int ans = -1;
		while(count < index) {
			ans = queue.poll();
			count++;
		}
		return ans;
	}



	@Override
	public String toString() {
		return "Range2Result [successRate=" + successRate + ", mainIP=" + mainIP + ", calledIP="
				+ calledIP + "]";
	}
}
