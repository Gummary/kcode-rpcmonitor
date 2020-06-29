package com.kuaishou.kcode.utils;

import java.nio.MappedByteBuffer;

public class BufferParser {
    private int offset;
    private final MappedByteBuffer buffer;

    public BufferParser(int start, MappedByteBuffer buffer)  {
        offset = start;
        this.buffer = buffer;
    }

    public int parseInt() {
        int result = 0;
        byte b;
        while ((b = buffer.get(offset)) >= '0' && b <= '9') {
            result *= 10;
            result += b - '0';
            offset ++;
        }
        // skip ","
        offset++;
        return result;
    }


    public long parseLong() {
        long result = 0;
        byte b;
        while ((b = buffer.get(offset)) >= '0' && b <= '9') {
            result *= 10;
            result += b - '0';
            offset ++;
        }
        // skip "\n"
        offset++;
        return result;
    }

    public boolean parseBoolean() {
        byte b = buffer.get(offset);
        if(b == 't') {
            // skip ure,
            offset += 5;
            return true;
        } else {
            // skip alse,
            offset += 6;
            return false;
        }
    }

    public String parseString() {
        StringBuilder builder = new StringBuilder();
        byte b;
        // 每条数据最后的值为时间戳，所以没有字符串位于最后的情况
        while((b=buffer.get(offset)) != ',') {
            builder.append((char)b);
            offset++;
        }
        // skip ","
        offset++;
        return builder.toString();
    }

    public int parseMinuteTimeStamp() {
        int buildTimeStamp = 0;
        for (int i = offset; i < offset + 9; i++) {
            buildTimeStamp *= 10;
            buildTimeStamp += buffer.get(i) - '0';
        }
        // skip millsecond and \n
        offset += 14;
        return buildTimeStamp / 6;
    }

    public long parseIP() {
        return parseLong() << 24 | parseInt() << 16 | parseInt() << 8 | parseInt();
    }

    public int getOffset() {
        return offset;
    }
}

