package com.order.db.RedisBoltDBHelper.DBRedisHelper;

import com.order.Redis.RedisClient;
import com.order.constant.Rules;
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
            String abnormalFeeKey = totalFeeKey + "|" + this.getRuleNumFromString(ruleArr[i]);
            redisClient.insertFeeToRedis(abnormalFeeKey, realInfoFee);
        }
    }

    public double getTotalFeeFromRedis(Long time, String provinceId, String channelCode, String contentId, String contentType,
                                  Double realInfoFee) {
        String currentTime = TimeParaser.formatTimeInDay(time);
        // 将总费用插入Redis，并设置今晚过期
        String totalFeeKey = currentTime + "|" + provinceId + "|"
                + channelCode + "|" + contentId + "|" + contentType ;

        double abnFee = redisClient.getAbnFee(totalFeeKey);
        if (abnFee == -1.0) {
            redisClient.insertFeeToRedis(totalFeeKey, realInfoFee);
        }
        return abnFee;
    }

    public double getAbnFeeFromRedis(Long time, String provinceId, String channelCode, String contentId, String contentType,
                                     Double realInfoFee, String rules) {
        String currentTime = TimeParaser.formatTimeInDay(time);
        String totalFeeKey = currentTime + "|" + provinceId + "|"
                + channelCode + "|" + contentId + "|" + contentType ;
        String abnormallFeeKey = totalFeeKey + "|" + rules;
        double abnFee = redisClient.getAbnFee(abnormallFeeKey);
        if (abnFee == -1.0) {
            redisClient.insertFeeToRedis(totalFeeKey, realInfoFee);
            redisClient.insertFeeToRedis(abnormallFeeKey, realInfoFee);
        }
        return abnFee;
    }


    /* 获取异常规则对应的数字编号 */
    public int getRuleNumFromString(String rule) {
        if (rule.equals(Rules.ONE.name())) {
            return 1;
        } else if (rule.equals(Rules.TWO.name())) {
            return 2;
        } else if (rule.equals(Rules.THREE.name())) {
            return 3;
        } else if (rule.equals(Rules.FOUR.name())) {
            return 4;
        } else if (rule.equals(Rules.FIVE.name())) {
            return 5;
        } else if (rule.equals(Rules.SIX.name())) {
            return 6;
        } else if (rule.equals(Rules.SEVEN.name())) {
            return 7;
        } else if (rule.equals(Rules.EIGHT.name())) {
            return 8;
        } else if (rule.equals(Rules.NINE.name())) {
            return 9;
        } else if (rule.equals(Rules.TEN.name())) {
            return 10;
        } else if (rule.equals(Rules.ELEVEN.name())) {
            return 11;
        } else if (rule.equals(Rules.TWELVE.name())) {
            return 12;
        }
        return 0;
    }
}
