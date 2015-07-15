package com.order.db.RedisBoltDBHelper.DBRedisHelper;

import com.order.Redis.RedisClient;
import com.order.constant.Rules;
import com.order.util.RuleUtil;
import com.order.util.TimeParaser;
import org.apache.log4j.Logger;

/**
 * Created by LiMingji on 15/7/13.
 */
public class DBTotalFeeRedisHelper {

    private static final long serialVersionUID = 1L;
    private static Logger log = Logger.getLogger(DBTotalFeeRedisHelper.class);

    private RedisClient redisClient = null;

    public DBTotalFeeRedisHelper() {
        redisClient = new RedisClient();
    }

    public void insertFeeToRedis(Long time, String provinceId, String channelCode, String contentId, String contentType,
                                 Double realInfoFee, String rules) {

        String currentTime = TimeParaser.formatTimeInDay(time);
        // 将总费用插入Redis，并设置今晚过期
        String totalFeeKey = currentTime + "|" + provinceId + "|"
                + channelCode + "|" + contentId + "|" + contentType;
        redisClient.insertFeeToRedis(totalFeeKey, realInfoFee);

        String[] ruleArr = rules.split("\\|");
        for (int i = 0; i < ruleArr.length; i++) {
            if (ruleArr[i].trim().equals("")) {
                continue;
            }
            String abnormalFeeKey = totalFeeKey + "|" + RuleUtil.getRuleNumFromString(ruleArr[i]);
            redisClient.insertFeeToRedis(abnormalFeeKey, realInfoFee);
        }
    }

    public double getTotalFeeFromRedis(String key, Double realInfofFee) {
        double totalFee = redisClient.getFee(key);
        if (totalFee == -1.0) {
            redisClient.insertFeeToRedis(key, realInfofFee);
            return 0;
        }
        return totalFee;
    }

    public double getAbnFeeFromRedis(String key) {
        double abnFee = redisClient.getFee(key);
        if (abnFee == -1.0) {
            return 0;
        }
        return abnFee;
    }
}
