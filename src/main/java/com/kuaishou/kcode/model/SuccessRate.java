package com.kuaishou.kcode.model;

import java.text.DecimalFormat;
import java.util.concurrent.atomic.AtomicInteger;

public class SuccessRate {
	public AtomicInteger success = new AtomicInteger();
	public AtomicInteger total = new AtomicInteger();
	
	
	public String computeSuccessRate(DecimalFormat format) {
//		System.out.println(success.get()+","+total.get());
		double rate = (double)success.get() / total.get();
		String resultRate = ".00%";
		if(rate - 0.0d >= 1e-2) {
			resultRate = format.format(rate * 100) + "%";
		}
//		System.out.println(resultRate);
		return resultRate;
	}


}
