package com.order.databean.cleaner;

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
    private StatisticsBolt bolt = null;

    public UserInfoCleaner(StatisticsBolt bolt) {
        this.bolt = bolt;
    }

    @Override
    public void run() {
        super.run();
        log.info("UserInfoCleaner Clean Thread created, id: " + this.getId());
        while (true) {
            try {
                Thread.sleep(Constant.ONE_MINUTE * 1000L);
                log.info("UserInfoCleaner Clean Thread begin clean data, id: " + this.getId()
                		+ ", userInfos.size: " + bolt.userInfos.keySet().size());
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
                log.info("SessionInfoCleaner Clean Thread end clean data, id: " + this.getId()
                		+ ", sessionInfos.size: " + bolt.sessionInfos.keySet().size());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
