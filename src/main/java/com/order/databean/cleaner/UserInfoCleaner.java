package com.order.databean.cleaner;

import com.order.constant.Constant;
import com.order.databean.TimeCacheStructures.Pair;
import com.order.databean.TimeCacheStructures.RealTimeCacheList;
import com.order.databean.UserInfo;
import com.order.util.LogUtil;

import java.util.Iterator;

/**
 * Created by LiMingji on 2015/6/20.
 */
public class UserInfoCleaner extends Thread {

    private RealTimeCacheList<Pair<String, UserInfo>> userInfos;

    public UserInfoCleaner(RealTimeCacheList<Pair<String, UserInfo>> userInfos) {
        this.userInfos = userInfos;
    }

    @Override
    public void run() {
        super.run();
        while (true) {
            try {
                Thread.sleep(Constant.ONE_MINUTE * 1000L);
                LogUtil.printLog(this.getClass()+"开始清理UserInfo数据");
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
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
