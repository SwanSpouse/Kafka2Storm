package com.order.databean.cleaner;

import com.order.bolt.StatisticsRedisBolt;
import com.order.constant.Constant;
import com.order.databean.SessionInfo;
import com.order.databean.TimeCacheStructures.Pair;
import org.apache.log4j.Logger;

import java.util.Iterator;

/**
 * Created by LiMingji on 2015/6/20.
 */
public class SessionInfoCleaner extends Thread {

    private static Logger log = Logger.getLogger(UserInfoCleaner.class);
    private StatisticsRedisBolt bolt = null;

    public SessionInfoCleaner(StatisticsRedisBolt bolt) {
    	this.bolt = bolt;
    }
    
    @Override
    public void run() {
        super.run();
        while (true) {
            try {
                Thread.sleep(Constant.ONE_MINUTE * 1000L);
                Iterator<Pair<String, SessionInfo>> it = bolt.sessionInfos.keySet().iterator();
                while (it.hasNext()) {
                    Pair<String,SessionInfo> currentPair = it.next();
                    SessionInfo currentUser = currentPair.getValue();
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
