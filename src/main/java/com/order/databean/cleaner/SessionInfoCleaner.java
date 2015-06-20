package com.order.databean.cleaner;

import com.order.constant.Constant;
import com.order.databean.SessionInfo;
import com.order.databean.TimeCacheStructures.Pair;
import com.order.databean.TimeCacheStructures.RealTimeCacheList;
import com.order.util.LogUtil;

import java.util.Iterator;

/**
 * Created by LiMingji on 2015/6/20.
 */
public class SessionInfoCleaner extends Thread {

    private RealTimeCacheList<Pair<String, SessionInfo>> sessionInfos;

    public SessionInfoCleaner(RealTimeCacheList<Pair<String, SessionInfo>> sessionInfos) {
        this.sessionInfos = sessionInfos;
    }

    @Override
    public void run() {
        super.run();
        while (true) {
            try {
                Thread.sleep(Constant.ONE_MINUTE * 1000L);
                LogUtil.printLog(this.getClass() + "开始清理SessionInfo数据");
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
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
