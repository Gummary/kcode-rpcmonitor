package com.kuaishou.kcode.utils;

public class DateUtils {
    private final static short[][] date_time_days =
            {
                    {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31},// 平年
                    {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31},// 润年
            };


    public static String minuteTimeStampToDate(int timestamp) {
        timestamp += 8 * 60;
        int month = 0;
        int day = 0;
        int year = 1970;
        int minute = timestamp % 60;
        timestamp /= 60;
        int hour = timestamp % 24;
        timestamp /= 24;

        // 1970 1971 平年 1972 闰年
        year += 3;
        timestamp -= 365 * 2 + 366;

        int years = timestamp / (365 * 4 + 1) * 4;
        timestamp %= 365 * 4 + 1;
        year += years;
        // 在余下的4年里计算月份和天份
        for (int i = 0; i < 3; ++i) {
            if (timestamp >= 365) {
                ++year;
                timestamp -= 365;
            } else {
                for (int j = 0; j < 12; ++j) {
                    month = j + 1;
                    if (timestamp >= date_time_days[0][j]) {
                        timestamp -= date_time_days[0][j];
                    } else {
                        day = timestamp + 1;
                        return String.format("%d-%02d-%02d %02d:%02d", year, month, day, hour, minute);
                    }
                }
            }
        }
        for (int j = 0; j < 12; ++j) {
            month = j + 1;
            if (timestamp >= date_time_days[1][j]) {
                timestamp -= date_time_days[1][j];
            } else {
                day = timestamp + 1;
                return String.format("%d-%02d-%02d %02d:%02d", year, month, day, hour, minute);
            }
        }
        return String.format("%d-%02d-%02d %02d:%02d", year, month, day, hour, minute);
    }

    public static int DateToMinuteTimeStamp(String date) {
        int syear = Integer.parseInt(date.substring(0, 4)) - 1;
        int smonth = Integer.parseInt(date.substring(5, 7)) - 1;
        int sday = Integer.parseInt(date.substring(8, 10)) - 1;
        int shour = Integer.parseInt(date.substring(11, 13));
        int sminute = Integer.parseInt(date.substring(14, 16));

        int minuteTimeStamp = sminute + shour * 60 + sday * 60 * 24;
        int isfour = ((syear + 1) % 4 == 0 ? 1 : 0);
        for (int i = 0; i < smonth; i++) {
            minuteTimeStamp += date_time_days[isfour][i] * (24 * 60);
        }

        minuteTimeStamp += (365 * 24 * 60 * 3) + 24 * 60; // 1970 1971 1972
        minuteTimeStamp += (((syear - 1972) / 4) * (365 * 4 + 1) + ((syear - 1972) % 4) * 365) * 24 * 60;

        // 东8区
        return minuteTimeStamp - 8*60;

    }
}
