package com.order.databean.TimeCacheStructures;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * Created by LiMingji on 2015/6/20.
 */
public class CachedList<T> implements Serializable {
    private static final long serialVersionUID = 1L;

    private static int mapSize = 100;

    private ConcurrentHashMap<T, LinkedList<Long>> list = new ConcurrentHashMap<T, LinkedList<Long>>();
    protected int expirationSecs = 0;

    public CachedList(int expirationSecs) {
        if (list == null) {
            list = new ConcurrentHashMap<T, LinkedList<Long>>(this.mapSize);
        }
        this.expirationSecs = expirationSecs;
    }

    /**
     * @param currentTime
     * @return
     */
    public int size(long currentTime) {
        this.removeTimeOutData(currentTime);
        return list.size();
    }

    /**
     * 对list中过期的数据进行清理。
     * @param lastUpdateTime 过期时间阈值
     */
    public void removeTimeOutData(long lastUpdateTime) {
        long timeThreshold = lastUpdateTime - expirationSecs * 1000L;
        for (T key : list.keySet()) {
            LinkedList<Long> clickTimes = list.get(key);
            if (clickTimes == null || clickTimes.size() == 0) {
                list.remove(key);
                continue;
            }
            //锁住
            synchronized (new Object()) {
                Iterator<Long> it = clickTimes.iterator();
                while (it.hasNext()) {
                    long currentTime = it.next();
                    if (currentTime <= timeThreshold) {
                        it.remove();
                    }
                }
            }
            if (clickTimes.size() == 0) {
                list.remove(key);
            }
        }
    }

    /**
     * 获取某个ID下的List大小。
     * @param key id
     * @return
     */
    public int sizeById(T key) {
        if (!list.containsKey(key) || list.get(key) == null) {
            return 0;
        }
        return list.get(key).size();
    }

    /**
     * 获取某个ID下未过期的数据。
     * @param key
     * @param currentTime
     * @param thresholdInSeconds
     * @return
     */
    public int sizeWithTimeThreshold(T key, Long currentTime, int thresholdInSeconds) {
        long timeThreshold = currentTime - thresholdInSeconds * 1000L;
        if (!list.containsKey(key) || list.get(key) == null) {
            return 0;
        }
        Iterator<Long> it = list.get(key).iterator();
        int countSize = 0;
        while (it.hasNext()) {
            Long currentClickTime = it.next();
            if (currentClickTime > timeThreshold) {
                countSize += 1;
            }
        }
        return countSize;
    }

    public void put(T key, Long currentTime) {
        if (currentTime == null) {
            currentTime = System.currentTimeMillis();
        }
        if (!list.containsKey(key) || list.get(key) == null) {
            LinkedList<Long> clickTimes = new LinkedList<Long>();
            clickTimes.add(currentTime);
            list.put(key, clickTimes);
        } else {
            LinkedList<Long> clickTimes = list.get(key);
            clickTimes.add(currentTime);
            list.put(key, clickTimes);
        }
    }

    public Pair get(T value) {
        if (!(value instanceof Pair)) {
            return null;
        }
        if ( list.containsKey(value)) {
            for (T currentValue : list.keySet()) {
                Pair currentPair = (Pair) currentValue;
                if (currentPair.equals(value)) {
                    return currentPair;
                }
            }
        }
        return null;
    }

    public boolean contains(T key) {
        if (key == null) {
            return false;
        }
        return list.containsKey(key);
    }

    @Override
    public String toString() {
        if (list == null || list.size() == 0) {
            return " EMPTY ";
        } else {
            return list.toString();
        }
    }
}
