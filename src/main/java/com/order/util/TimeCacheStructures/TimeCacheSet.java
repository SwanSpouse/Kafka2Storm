package com.order.util.TimeCacheStructures;

import org.apache.log4j.Logger;

import java.util.LinkedList;

/**
 * 同TimeCacheList。 只是其中不能存储相同元素
 *
 * Created by LiMingji on 2015/5/22.
 */
public class TimeCacheSet<T> extends TimeCacheList<T> {

    private static Logger log = Logger.getLogger(TimeCacheSet.class);

    public TimeCacheSet(int expirationSecs, int numBuckets, ExpiredCallback expiredCallback) {
        super(expirationSecs, numBuckets, expiredCallback);
    }

    public TimeCacheSet(int expirationSecs, ExpiredCallback expiredCallback) {
        this(expirationSecs, DEFAULT_NUM_BUCKETS, expiredCallback);
    }

    public TimeCacheSet(int expirationSecs, int numBuckets) {
        this(expirationSecs, numBuckets, null);
    }

    public TimeCacheSet(int expirationSecs) {
        this(expirationSecs, DEFAULT_NUM_BUCKETS);
    }

    @Override
    public void put(T value) {
        synchronized (LOCK) {
            this.remove(value);
            LinkedList<T> bucket = buckets.getLast();
            bucket.addLast(value);
        }
    }
}
