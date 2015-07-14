package com.order.Redis;


/**
 * Created by LiMingji on 15/7/10.
 */
public class RedisTest {
    public static void main(String[] args) {
//        new RedisClient().show();
        RedisClient redisClient = new RedisClient();

        redisClient.insertFeeToRedis("limingji", 3);
        redisClient.insertFeeToRedis("limingji1", 3);
        redisClient.insertFeeToRedis("limingji2", 3);
        redisClient.insertFeeToRedis("limingji2", 3);
        redisClient.insertFeeToRedis("limingji3", 3);
        redisClient.insertFeeToRedis("limingji4", 3);
        System.out.println("ok");
    }
}

