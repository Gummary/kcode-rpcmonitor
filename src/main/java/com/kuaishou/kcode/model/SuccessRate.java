package com.kuaishou.kcode.model;

import java.util.concurrent.atomic.AtomicInteger;

public class SuccessRate {
	public AtomicInteger success = new AtomicInteger();
	public AtomicInteger total = new AtomicInteger();
	
	
	public double computeSuccessRate() {
		return (double)success.get() / total.get();
	}
}
