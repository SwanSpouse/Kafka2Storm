package com.order.databean.cleaner;

import com.order.bolt.Redis.StatisticsRedisBolt;
import com.order.bolt.StatisticsBolt;
import com.order.constant.Constant;
import com.order.databean.TimeCacheStructures.Pair;
import com.order.databean.UserInfo;
import org.apache.log4j.Logger;

import java.util.Iterator;

/**
 * Created by LiMingji on 2015/6/20.
 */
public class UserInfoCleaner extends Thread {

    private static Logger log = Logger.getLogger(UserInfoCleaner.class);
    private StatisticsRedisBolt bolt = null;

    public UserInfoCleaner(StatisticsRedisBolt bolt) {
        this.bolt = bolt;
    }

    @Override
    public void run() {
        super.run();
        while (true) {
            try {
                Thread.sleep(Constant.ONE_MINUTE * 1000L);
                Iterator<Pair<String, UserInfo>> it = bolt.userInfos.keySet().iterator();
                while (it.hasNext()) {
                    Pair<String,UserInfo> currentPair = it.next();
                    UserInfo currentUser = currentPair.getValue();
                    if (currentUser == null) {
                        it.remove();
                        continue;
                    }
                    if (currentUser.clear()) {
                        it.remove();
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
