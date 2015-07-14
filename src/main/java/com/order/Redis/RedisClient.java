package com.order.Redis;

import com.order.util.TimeParaser;
import redis.clients.jedis.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Created by LiMingji on 15/7/10.
 */

public class RedisClient implements Serializable{
    private static final long serialVersionUID = 1L;

    private Jedis jedis;//非切片额客户端连接
    private JedisPool jedisPool;//非切片连接池

    public RedisClient() {
        initialPool();
        jedis = jedisPool.getResource();
    }

    /**
     * 初始化非切片池
     */
    private void initialPool() {
        // 池基本配置
        JedisPoolConfig config = new JedisPoolConfig();
//        config.setMaxActive(20);
        config.setMaxIdle(5);
//        config.setMaxWait(1000l);
        config.setTestOnBorrow(false);
        jedisPool = new JedisPool(config, "10.212.239.157", 6379);
    }

    //将总费用异常费用插入Redis
    public void insertFeeToRedis(String key, double fee) {
        if (!jedis.exists(key)) {
            jedis.set(key, fee + "");
        } else {
            double newFee = Double.parseDouble(jedis.get(key));
            jedis.set(key, fee + newFee + "");
        }
        jedis.expireAt(key, TimeParaser.getTomorrowZeroOclockMillis());
    }

    //根据Key从Redis取数据
    public double getAbnFee(String key) {
        if (jedis.exists(key)) {
            return Double.parseDouble(jedis.get(key));
        } else {
            return -1.0;
        }
    }
}
