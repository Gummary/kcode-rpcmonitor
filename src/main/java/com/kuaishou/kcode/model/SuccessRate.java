package com.kuaishou.kcode.model;

import java.util.concurrent.atomic.AtomicInteger;

public class SuccessRate {
	public AtomicInteger success = new AtomicInteger();
	public AtomicInteger total = new AtomicInteger();
	
	
	public String computeSuccessRate() {
		System.out.println(success.get()+","+total.get());
		double rate = (double)success.get() / total.get();
		rate = (int)(rate * 100) / 100;
		String resultRate = ".00%";
		if(rate - 0.0d >= 10e-2) {
			resultRate = rate + "%";
		}
		System.out.println(resultRate);
		return resultRate;
	}


	@Override
	public String toString() {
		return "SuccessRate [success=" + success.get() + ", total=" + total.get() + "]";
	}
	
}
