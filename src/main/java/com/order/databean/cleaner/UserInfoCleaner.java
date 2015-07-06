package com.order.databean.cleaner;

import com.order.constant.Constant;
import com.order.databean.TimeCacheStructures.Pair;
import com.order.databean.TimeCacheStructures.RealTimeCacheList;
import com.order.databean.UserInfo;
import org.apache.log4j.Logger;

import java.util.Iterator;

/**
 * Created by LiMingji on 2015/6/20.
 */
public class UserInfoCleaner extends Thread {

    private RealTimeCacheList<Pair<String, UserInfo>> userInfos;

    private static Logger log = Logger.getLogger(UserInfoCleaner.class);

    public UserInfoCleaner(RealTimeCacheList<Pair<String, UserInfo>> userInfos) {
        this.userInfos = userInfos;
    }

    @Override
    public void run() {
        super.run();
        while (true) {
            try {
                Thread.sleep(Constant.ONE_MINUTE * 1000L);
long currentTime = System.currentTimeMillis();
int sizeBefore = userInfos.size(currentTime);
                Iterator<Pair<String, UserInfo>> it = userInfos.keySet().iterator();
                while (it.hasNext()) {
                    Pair<String,UserInfo> currentPair = it.next();
                    UserInfo currentUser = currentPair.getValue();
                    if (currentUser == null) {
                        userInfos.remove(currentPair);
                        continue;
                    }
                    if (currentUser.clear()) {
                        userInfos.remove(currentPair);
                    }
                }
long afterClearTime = System.currentTimeMillis();
int sizeAfter = userInfos.size(afterClearTime);
log.info("清理userInofs耗时： " + (afterClearTime - currentTime) / 1000 + " 清理了 " + (sizeAfter - sizeBefore));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
