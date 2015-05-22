package com.order.util.TimeCacheStructures;

import org.apache.log4j.Logger;

import java.util.LinkedList;
import java.util.List;

/**
 * 参照 TimeCacheMap 实现的TimeCacheList。
 *
 * TODO: 这个还不是严格的实时，如果数据到达时间和插入的时间间隔比较长。延迟的时间会变长。
 * Created by LiMingji on 2015/5/21.
 */
public class TimeCacheList<T> {
    protected static final int DEFAULT_NUM_BUCKETS = 3;

    private static Logger log = Logger.getLogger(TimeCacheList.class);

    public static interface ExpiredCallback<T> {
        public void expire(T value);
    }

    protected LinkedList<LinkedList<T>> buckets;

    protected final Object LOCK = new Object();
    protected Thread cleaner;
    protected ExpiredCallback expiredCallback;

    public TimeCacheList(int expirationSecs, int numBuckets, final ExpiredCallback<T> expiredCallback) {
        if (numBuckets < 2) {
            throw new IllegalArgumentException("numBuckets must be >= 2");
        }
        buckets = new LinkedList<LinkedList<T>>();
        for (int i = 0; i < numBuckets; i++) {
            buckets.add(new LinkedList<T>());
        }

        this.expiredCallback = expiredCallback;
        final long expirationMillis = expirationSecs * 1000L;
        final long sleepTime = expirationMillis / (numBuckets - 1);

        cleaner = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while (true) {
                        List<T> dead = null;
                        cleaner.sleep(sleepTime);
                        synchronized (LOCK) {
                            dead = buckets.removeFirst();
                            buckets.addLast(new LinkedList<T>());
                        }
                        if (expiredCallback != null) {
                            for (T value : dead) {
                                expiredCallback.expire(value);
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    log.error("InterruptedException : " + e);
                }
            }
        });
        cleaner.setDaemon(true);
        cleaner.start();
    }

    public TimeCacheList(int expirationSecs, ExpiredCallback<T> expiredCallback) {
        this(expirationSecs, DEFAULT_NUM_BUCKETS, expiredCallback);
    }

    public TimeCacheList(int expirationSecs, int numBuckets) {
        this(expirationSecs, numBuckets, null);
    }

    public TimeCacheList(int expirationSecs) {
        this(expirationSecs, DEFAULT_NUM_BUCKETS);
    }

    public boolean contains(T value) {
        synchronized (LOCK) {
            for (List list : buckets) {
                if (list.contains(value)) {
                    return true;
                }
            }
        }
        return false;
    }

    public void put(T value) {
        synchronized (LOCK) {
            LinkedList<T> bucket = buckets.getLast();
            bucket.addLast(value);
        }
    }

    public void remove(T value) {
        synchronized (LOCK) {
            for (List bucket : buckets) {
                if (bucket.contains(value)) {
                    bucket.remove(value);
                }
            }
        }
    }

    public T getLast() {
        synchronized (LOCK) {
            LinkedList<T> bucket = buckets.getLast();
            if (!bucket.isEmpty()) {
                return bucket.getLast();
            } else {
                return null;
            }
        }
    }

    public int size() {
        synchronized (LOCK) {
            int size = 0;
            for (List bucket : buckets) {
                size += bucket.size();
            }
            return size;
        }
    }

    public void cleanup() {
        cleaner.interrupt();
    }
}