package com.kuaishou.kcode.model;

import java.nio.MappedByteBuffer;

public class ReadTask {

    public int start;
    public int end;
    public String remindBuffer;

    public MappedByteBuffer buffer;

    public ReadTask(int start, int end, String remindBuffer, MappedByteBuffer buffer) {
        this.start = start;
        this.end = end;
        this.remindBuffer = remindBuffer;
        this.buffer = buffer;
    }
}
