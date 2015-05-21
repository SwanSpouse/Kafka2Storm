package com.order.util;

import backtype.storm.utils.Time;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Expires keys that have not been updated in the configured number of seconds.
 * The algorithm used will take between expirationSecs and
 * expirationSecs * (1 + 1 / (numBuckets-1)) to actually expire the message.
 * <p/>
 * get, put, remove, containsKey, and size take O(numBuckets) time to run.
 * <p/>
 * The advantage of this design is that the expiration thread only locks the object
 * for O(1) time, meaning the object is essentially always available for gets/puts.
 * <p/>
 * reference: https://github.com/HQebupt/TimeCacheMap
 */

public class TimeCacheMap<K, V> {
    //this default ensures things expire at most 50% past the expiration time
    private static final int DEFAULT_NUM_BUCKETS = 3;

    public static interface ExpiredCallback<K, V> {
        public void expire(K key, V val);
    }

    private LinkedList<HashMap<K, V>> buckets;

    private final Object LOCK = new Object();
    private Thread cleaner;
    private ExpiredCallback expiredCallback;

    public TimeCacheMap(int expirationSecs, int numBuckets, ExpiredCallback<K, V> callback) {
        if (numBuckets < 2) {
            throw new IllegalArgumentException("numBuckets must be >= 2");
        }
        buckets = new LinkedList<HashMap<K, V>>();
        for (int i = 0; i < numBuckets; i++) {
            buckets.add(new HashMap<K, V>());
        }


        expiredCallback = callback;
        final long expirationMillis = expirationSecs * 1000L;
        final long sleepTime = expirationMillis / (numBuckets - 1);
        cleaner = new Thread(new Runnable() {
            public void run() {
                try {
                    while (true) {
                        Map<K, V> dead = null;
//                        Time.sleep(sleepTime);
                        cleaner.sleep(sleepTime);
                        synchronized (LOCK) {
                            dead = buckets.removeLast();
                            buckets.addFirst(new HashMap<K, V>());
                        }
                        if (expiredCallback != null) {
                            for (Entry<K, V> entry : dead.entrySet()) {
                                expiredCallback.expire(entry.getKey(), entry.getValue());
                            }
                        }
                    }
                } catch (InterruptedException ex) {

                }
            }
        });
        cleaner.setDaemon(true);
        cleaner.start();
    }

    public TimeCacheMap(int expirationSecs, ExpiredCallback<K, V> callback) {
        this(expirationSecs, DEFAULT_NUM_BUCKETS, callback);
    }

    public TimeCacheMap(int expirationSecs) {
        this(expirationSecs, DEFAULT_NUM_BUCKETS);
    }

    public TimeCacheMap(int expirationSecs, int numBuckets) {
        this(expirationSecs, numBuckets, null);
    }

    public boolean containsKey(K key) {
        synchronized (LOCK) {
            for (HashMap<K, V> bucket : buckets) {
                if (bucket.containsKey(key)) {
                    return true;
                }
            }
            return false;
        }
    }

    public V get(K key) {
        synchronized (LOCK) {
            for (HashMap<K, V> bucket : buckets) {
                if (bucket.containsKey(key)) {
                    return bucket.get(key);
                }
            }
            return null;
        }
    }

    public void put(K key, V value) {
        synchronized (LOCK) {
            Iterator<HashMap<K, V>> it = buckets.iterator();
            HashMap<K, V> bucket = it.next();
            bucket.put(key, value);
            while (it.hasNext()) {
                bucket = it.next();
                bucket.remove(key);
            }
        }
    }

    public Object remove(K key) {
        synchronized (LOCK) {
            for (HashMap<K, V> bucket : buckets) {
                if (bucket.containsKey(key)) {
                    return bucket.remove(key);
                }
            }
            return null;
        }
    }

    public int size() {
        synchronized (LOCK) {
            int size = 0;
            for (HashMap<K, V> bucket : buckets) {
                size += bucket.size();
            }
            return size;
        }
    }

    public void cleanup() {
        cleaner.interrupt();
    }
}