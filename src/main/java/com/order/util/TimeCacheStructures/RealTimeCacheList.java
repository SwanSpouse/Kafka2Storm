package com.order.util.TimeCacheStructures;

import org.apache.log4j.Logger;

import java.util.*;

/**
 * Created by LiMingji on 2015/5/22.
 */
public class RealTimeCacheList<T> {

    private static Logger log = Logger.getLogger(RealTimeCacheList.class);

    public static interface TimeOutCallback<T> {
        public void expire(T value, LinkedList<Long> pvTimes);
    }

    private Map<T, LinkedList<Long>> oldList;
    private Map<T, LinkedList<Long>> currentList;

    protected final static Object LOCK = new Object();
    protected Thread cleaner = null;
    protected TimeOutCallback timeOutCallback = null;
    protected int expiratonSecs = 0;

    public RealTimeCacheList(int expirationSecs, final TimeOutCallback timeOutCallback) {
        oldList = new LinkedHashMap<T, LinkedList<Long>>();
        currentList = new LinkedHashMap<T, LinkedList<Long>>();

        this.timeOutCallback = timeOutCallback;
        this.expiratonSecs = expirationSecs;
        final long sleepTime = expirationSecs * 1000L;

        cleaner = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        cleaner.sleep(sleepTime);
                        if (timeOutCallback != null) {
                            Iterator<T> iterator = oldList.keySet().iterator();
                            while (iterator.hasNext()) {
                                T key = iterator.next();
                                timeOutCallback.expire(key, oldList.get(key));
                            }
                        }
                        Iterator<T> it = currentList.keySet().iterator();
                        while (it.hasNext()) {
                            T value = it.next();
                            removeExpiredData(value, System.currentTimeMillis());
                        }
                        oldList.clear();
                        oldList.putAll(currentList);
                        currentList.clear();
                    } catch (InterruptedException e) {
                        log.error(e);
                    }
                }
            }
        });
        cleaner.setDaemon(true);
        cleaner.start();
    }

    public int size() {
        synchronized (LOCK) {
            return currentList.size() + oldList.size();
        }
    }

    public void put(T value) {
        this.put(value, new Date());
    }

    public void put(T value, Date date) {
        synchronized (LOCK) {
            long currentTime;
            if (date == null) {
                currentTime = date.getTime();
            } else {
                currentTime = System.currentTimeMillis();
            }

            //clear expired data when insert data
            removeExpiredData(value, currentTime);

            if (oldList.containsKey(value)) {
                LinkedList<Long> clickTimes = oldList.get(value);
                LinkedList<Long> newClickTimes = new LinkedList<Long>();
                newClickTimes.addAll(clickTimes);
                newClickTimes.add(currentTime);
                currentList.put(value, newClickTimes);
                oldList.remove(value);
            } else {
                LinkedList<Long> clickTimes = new LinkedList<Long>();
                clickTimes.add(currentTime);
                currentList.put(value, clickTimes);
            }
        }
    }

    //对某个id下的过期数据进行清楚。
    private void removeExpiredData(T value, long currentTime) {
        if ( !oldList.containsKey(value)) {
            return ;
        }
        long timeOutThreshold = currentTime - expiratonSecs * 1000L;
        synchronized (LOCK) {
            Iterator<Long> it = oldList.get(value).iterator();
            while (it.hasNext()) {
                Long time = it.next();
                if (time < timeOutThreshold) {
                    it.remove();
                } else {
                    return;
                }
            }
        }
    }

    public void clear() {
        cleaner.interrupt();
    }
}