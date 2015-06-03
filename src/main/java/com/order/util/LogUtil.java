package com.order.util;

import com.order.bolt.StatisticsBolt;
import org.apache.log4j.Logger;

/**
 * Created by LiMingji on 2015/6/3.
 */
public class LogUtil {

    private static Logger log = Logger.getLogger(LogUtil.class);

    public static void printLog(Object obj, String msg, Boolean isObey) {
        if (StatisticsBolt.isDebug) {
            String tmp = isObey ? " obey " : " disobey ";
            log.info(tmp + msg+"  "+ obj.toString());
        }
    }
}
