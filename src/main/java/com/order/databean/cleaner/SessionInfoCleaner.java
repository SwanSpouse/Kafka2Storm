package com.order.databean.cleaner;

import com.order.bolt.StatisticsBolt;
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
    private StatisticsBolt bolt = null;

    public SessionInfoCleaner(StatisticsBolt bolt) {
    	this.bolt = bolt;
    }
    
    @Override
    public void run() {
        super.run();
        log.info("SessionInfoCleaner Clean Thread created, id: " + this.getId());
        while (true) {
            try {
                Thread.sleep(Constant.ONE_MINUTE * 1000L);
                log.info("SessionInfoCleaner Clean Thread begin clean data, id: " + this.getId()
                		+ ", sessionInfos.size: " + bolt.sessionInfos.keySet().size());

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
                log.info("SessionInfoCleaner Clean Thread end clean data, id: " + this.getId()
                		+ ", sessionInfos.size: " + bolt.sessionInfos.keySet().size());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
