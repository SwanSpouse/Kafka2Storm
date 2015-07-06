package com.order.databean.cleaner;

import com.order.constant.Constant;
import com.order.databean.SessionInfo;
import com.order.databean.TimeCacheStructures.Pair;
import com.order.databean.TimeCacheStructures.RealTimeCacheList;
import org.apache.log4j.Logger;

import java.util.Iterator;

/**
 * Created by LiMingji on 2015/6/20.
 */
public class SessionInfoCleaner extends Thread {

    private RealTimeCacheList<Pair<String, SessionInfo>> sessionInfos;

    private static Logger log = Logger.getLogger(UserInfoCleaner.class);

    public SessionInfoCleaner(RealTimeCacheList<Pair<String, SessionInfo>> sessionInfos) {
        this.sessionInfos = sessionInfos;
    }

    @Override
    public void run() {
        super.run();
        while (true) {
            try {
                Thread.sleep(Constant.ONE_MINUTE * 1000L);
long currentTime = System.currentTimeMillis();
int sizeBefore = sessionInfos.size(currentTime);
                Iterator<Pair<String, SessionInfo>> it = sessionInfos.keySet().iterator();
                while (it.hasNext()) {
                    Pair<String,SessionInfo> currentPair = it.next();
                    SessionInfo currentUser = currentPair.getValue();
                    if (currentUser == null) {
                        sessionInfos.remove(currentPair);
                        continue;
                    }
                    if (currentUser.clear()) {
                        sessionInfos.remove(currentPair);
                    }
                }
long afterClearTime = System.currentTimeMillis();
int sizeAfter = sessionInfos.size(afterClearTime);
log.info("清理sessionInfos耗时： " + (afterClearTime - currentTime) / 1000 + " 清理了 " + (sizeAfter - sizeBefore));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
