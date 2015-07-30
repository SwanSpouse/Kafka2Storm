package com.order.databean.TimeCacheStructures;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * Created by LiMingji on 2015/6/20.
 */
public class CachedList<T> implements Serializable {
    private static final long serialVersionUID = 1L;

    private static int mapSize = 100;

    private ConcurrentHashMap<T, ArrayList<Long>> list = new ConcurrentHashMap<T, ArrayList<Long>>();
    protected int expirationSecs = 0;

    public CachedList(int expirationSecs) {
        if (list == null) {
            list = new ConcurrentHashMap<T, ArrayList<Long>>(this.mapSize);
        }
        this.expirationSecs = expirationSecs;
    }

    /**
     * 获取整个List中Key的个数。key的个数取决于key 对应的list中元素不为零的个数。
     * @param currentTime
     * @param timeOutSeconds  过期时间。单位s
     * @return
     */
    public int size(long currentTime, int timeOutSeconds) {
        //第一步清除过期数据
        this.removeTimeOutData(currentTime);
        if (timeOutSeconds == -1) {
            timeOutSeconds = expirationSecs;
        }
        int count = 0;
        for (T key : list.keySet()) {
            count += sizeById(key, currentTime, timeOutSeconds) == 0 ? 0 : 1;
        }
        return count;
    }

    /**
     * 获取某个ID(KEY)下的，符合时间要求的List大小。
     * @param key    id
     * @param endTime   key的时间。消息达到时间。
     * @param timeOutSeconds 数据过期时间。单位s
     * @return
     */
    public int sizeById(T key, long endTime, int timeOutSeconds) {
        //第一步清除过期数据
        this.removeTimeOutData(endTime);
        if (!list.containsKey(key)) {
            return 0;
        }
        if (list.get(key) == null || list.get(key).size() == 0) {
            list.remove(key);
            return 0;
        }
        long startTime = endTime - timeOutSeconds * 1000l;
        ArrayList<Long> clickTimes = list.get(key);
        int count = 0;
        for (int i = 0; i < clickTimes.size(); i++) {
            if (clickTimes.get(i) >= startTime && clickTimes.get(i) <= endTime) {
                count += 1;
            }
        }
        return count;
    }

    /**
     * 获取某个ID下未过期的数据。
     * @param key
     * @param endTime            当前时间(long)。
     * @param thresholdInSeconds 数据过期时间(s)。
     * @return
     */
    public int sizeWithTimeThreshold(T key, Long endTime, int thresholdInSeconds) {
        //第一步清除过期数据
        this.removeTimeOutData(endTime);
        long startTime = endTime - thresholdInSeconds * 1000L;
        if (!list.containsKey(key) || list.get(key) == null) {
            return 0;
        }
        Iterator<Long> it = list.get(key).iterator();
        int countSize = 0;
        while (it.hasNext()) {
            Long currentClickTime = it.next();
            if (currentClickTime >= startTime && currentClickTime <= endTime) {
                countSize += 1;
            }
        }
        return countSize;
    }

    /**
     * 对list中过期的数据进行清理。
     * @param lastUpdateTime 过期时间阈值
     */
    private void removeTimeOutData(long lastUpdateTime) {
        long timeThreshold = lastUpdateTime - expirationSecs * 1000L;
        Iterator<T> itKey = list.keySet().iterator();
        while (itKey.hasNext()) {
            T key = itKey.next();
            ArrayList<Long> clickTimes = list.get(key);
            if (clickTimes == null || clickTimes.size() == 0) {
                itKey.remove();
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
                clickTimes.clear();
                itKey.remove();
            }
        }
    }

    /**
     * 将数据iD和数据到达时间放到表里。
     * @param key    ID
     * @param currentTime 数据到达时间。
     */
    public void put(T key, Long currentTime) {
        if (currentTime == null) {
            currentTime = System.currentTimeMillis();
        }
        if (!list.containsKey(key) || list.get(key) == null) {
            ArrayList<Long> clickTimes = new ArrayList<Long>();
            clickTimes.add(currentTime);
            list.put(key, clickTimes);
        } else {
            ArrayList<Long> clickTimes = list.get(key);
            clickTimes.add(currentTime);
            list.put(key, clickTimes);
        }
    }

    /**
     * 用在BookOrderList和SessionInfos UserInfos中。
     * @param value 通过Key来获取一个Key-Value对。
     * @return
     */
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

    @Override
    public String toString() {
        if (list == null || list.size() == 0) {
            return " EMPTY ";
        } else {
            return list.toString();
        }
    }
}
