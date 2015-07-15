package com.order.db.RedisBoltDBHelper.DBRedisHelper;

import com.order.Redis.RedisClient;
import com.order.util.RuleUtil;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * Created by LiMingji on 15/7/15.
 */
public class OrderInRedisHelper implements Serializable {

    private static final long serialVersionUID = 1L;

    private static Logger log = Logger.getLogger(DBTotalFeeRedisHelper.class);

    private RedisClient redisClient = null;

    public OrderInRedisHelper() {
        redisClient = new RedisClient();
    }

    public void putOrderInRedis(String msisdn, Long recordTime, double realInfoFee, String channelCode,
                                String provinceId, String contentId, String contentType, String rules) {

        //解析 ONE|THREE|ELEVEN 这样的Rules。变成101000000010这样的形式存储到Redis中。
        HashSet<Integer> ruleSet = new HashSet<Integer>();
        String[] ruleArr = rules.split("\\|");
        for (int i = 0; i < ruleArr.length; i++) {
            if (ruleArr[i].trim().equals("")) {
                continue;
            }
            int ruleId = RuleUtil.getRuleNumFromString(ruleArr[i]);
            ruleSet.add(ruleId);
        }
        String rulesInRedis = "";
        for (int i = 1; i <= 12; i++) {
            if (ruleSet.contains(i)) {
                rulesInRedis += 1;
            } else {
                rulesInRedis += 0;
            }
        }
        //拼出Redis中存储的Key和Value
        String redisOrderKey = channelCode + "|" + msisdn + "|" + recordTime;
        String redisOrderValue = realInfoFee + "|" + provinceId + "|" + contentId + "|" + contentType + "|" + rulesInRedis;
        redisClient.putOrderInRedis(redisOrderKey, redisOrderValue);
    }

    public List<String> traceOrderFromRedis(String channelCode, String msisdn, Long traceBackTime, int ruleId) {
        String orderKeyInRedis = channelCode + "|" + msisdn;
        return redisClient.getOrderInRedis(orderKeyInRedis, traceBackTime, ruleId);
    }
}
