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

    @Override
    public void run() {
        super.run();
        while (true) {
            try {
                Thread.sleep(Constant.ONE_MINUTE * 1000L);
long currentTime = System.currentTimeMillis();
int sizeBefore = StatisticsBolt.sessionInfos.size(currentTime);
                Iterator<Pair<String, SessionInfo>> it = StatisticsBolt.sessionInfos.keySet().iterator();
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
long afterClearTime = System.currentTimeMillis();
int sizeAfter = StatisticsBolt.sessionInfos.size(afterClearTime);
log.info("清理sessionInfos耗时： " + (afterClearTime - currentTime)  + " 清理了 " + (sizeAfter - sizeBefore));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
