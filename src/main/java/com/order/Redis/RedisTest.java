package com.order.Redis;

/**
 * Created by LiMingji on 15/7/10.
 */
public class RedisTest {
    public static void main(String[] args) {
//        new RedisClient().show();
        RedisClient redisClient = new RedisClient();

        redisClient.KeyOperate();
        redisClient.StringOperate();
        redisClient.ListOperate();
        redisClient.SetOperate();
        redisClient.SortedSetOperate();
        redisClient.HashOperate();
    }
}
