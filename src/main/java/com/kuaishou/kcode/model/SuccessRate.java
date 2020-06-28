package com.kuaishou.kcode.model;

import java.text.DecimalFormat;
import java.util.concurrent.atomic.AtomicInteger;

public class SuccessRate {
	public AtomicInteger success = new AtomicInteger();
	public AtomicInteger total = new AtomicInteger();

	public double getRawSuccessrate() {
		return (double)success.get() / total.get();
	}
	
	public String computeSuccessRate(DecimalFormat format) {
//		System.out.println(success.get()+","+total.get());
        double rate = getRawSuccessrate();
		String resultRate = ".00%";
		if(rate - 0.0d >= 1e-4) {
			resultRate = format.format(rate * 100) + "%";
		}
//		System.out.println(resultRate);
		return resultRate;
	}

	public void mergeSuccessate(SuccessRate other) {
		success.addAndGet(other.success.get());
		total.addAndGet(other.total.get());
	}

}
