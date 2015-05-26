package com.order.util;

import org.apache.log4j.Logger;

import java.util.Calendar;

/**
 * Created by LiMingji on 2015/5/26.
 */
public class TimeParaser {
    private static Logger log =Logger.getLogger(TimeParaser.class);
    /**
     * @param recordTime 输入样例 20150505165523
     * @return 从recordTime到1970年的毫秒数
     */
    public static long splitTime(String recordTime) {
        Calendar calendar = Calendar.getInstance();
        try {
            int year = Integer.parseInt(recordTime.substring(0, 4));
            int month = Integer.parseInt(recordTime.substring(4, 6));
            int date = Integer.parseInt(recordTime.substring(6, 8));
            int hour = Integer.parseInt(recordTime.substring(8, 10));
            int minute = Integer.parseInt(recordTime.substring(10, 12));
            int seconds = Integer.parseInt(recordTime.substring(12, 14));

            calendar.set(year, month, date, hour, minute, seconds);
            return calendar.getTimeInMillis();

        } catch (Exception e) {
            log.error("时间输入格式有问题: " + e);
        }
        return -1L;
    }
}
