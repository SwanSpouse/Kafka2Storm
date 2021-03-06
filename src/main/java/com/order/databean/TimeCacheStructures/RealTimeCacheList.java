package com.order.databean.TimeCacheStructures;

import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * reference: https://github.com/HQebupt/TimeCacheMap
 * <p/>
 * 实时过期List :
 * expirationSecs: List中数据过期时间。
 * TimeOutCallBack: 对过期的数据如何进行处理。（可以选择进行持久化操作。）
 * <p/>
 * 清理操作：
 * 1. 定时清理：每隔expirationSecs进行一次清理。
 * 2. 数据插入时清理：插入对于同一key下的过期数据进行清理。
 * <p/>
 * 说明：
 * List中key唯一。相同的key会根据访问时间合并到key所对应的List中。
 * <p/>
 * Created by LiMingji on 2015/5/22.
 */
public class RealTimeCacheList<T> implements Serializable {

    private static final long serialVersionUID = 1L;
    private static Logger log = Logger.getLogger(RealTimeCacheList.class);

    private static int mapSize = 300; // 30000

    public static interface TimeOutCallback<T> {
        public void expire(T value, LinkedList<Long> pvTimes);
    }

    private ConcurrentHashMap<T, LinkedList<Long>> oldList;
    private ConcurrentHashMap<T, LinkedList<Long>> currentList;

    protected transient Thread cleaner = null;
    protected TimeOutCallback timeOutCallback = null;
    protected int expiratonSecs = 0;

    public RealTimeCacheList(int expiratonSecs) {
        this(expiratonSecs, null);
    }

    public RealTimeCacheList(int expirationSecs, final TimeOutCallback timeOutCallback) {
        oldList = new ConcurrentHashMap<T, LinkedList<Long>>(this.mapSize);
        currentList = new ConcurrentHashMap<T, LinkedList<Long>>(this.mapSize);

        this.timeOutCallback = timeOutCallback;
        this.expiratonSecs = expirationSecs;
        createCleanThread();
    }

    private void createCleanThread() {	
		if (cleaner == null) {
	        final long sleepTime = expiratonSecs * 1000L;
	        cleaner = new Thread(new Runnable() {
	            @Override
	            public void run() {
	            	log.info("RealTimeCacheList Clean Thread created, id: " + cleaner.getId());
	                while (true) {
	                    try {
	                        cleaner.sleep(sleepTime);
//	                        if (timeOutCallback != null) {
//	                            Iterator<T> iterator = oldList.keySet().iterator();
//	                            while (iterator.hasNext()) {
//	                                T key = iterator.next();
//	                                timeOutCallback.expire(key, oldList.get(key));
//	                            }
//	                        }
	                    	log.info("RealTimeCacheList Clean Thread begin clean data, id: " + cleaner.getId()
	                    			+ ", oldList.size: " + oldList.size() + ", currentList.size():" + currentList.size());
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
    }
    
    public void put(T value) {
    	createCleanThread();
        this.put(value, (new Date()).getTime());
    }

    public void put(T value, Long date) {
        long currentTime;
        if (date != null) {
            currentTime = date;
        } else {
            currentTime = System.currentTimeMillis();
        }

        if (oldList.containsKey(value)) {
            LinkedList<Long> clickTimes = oldList.get(value);
            LinkedList<Long> newClickTimes = new LinkedList<Long>();
            newClickTimes.addAll(clickTimes);
            newClickTimes.add(currentTime);
            currentList.put(value, newClickTimes);
            oldList.remove(value);
        } else {
            LinkedList<Long> clickTimes;
            if (currentList.containsKey(value)) {
                clickTimes = currentList.get(value);
            } else {
                clickTimes = new LinkedList<Long>();
            }
            clickTimes.add(currentTime);
            currentList.put(value, clickTimes);
        }
    }

    public boolean contains(T value) {
        if (value == null) {
            return false;
        }
        return oldList.containsKey(value) || currentList.containsKey(value);
    }

    public Pair get(T value) {
        if (!(value instanceof Pair)) {
            return null;
        }
        if (oldList.containsKey(value)) {
            for (T currentValue : oldList.keySet()) {
                Pair currentPair = (Pair) currentValue;
                if (currentPair.equals(value)) {
                    return currentPair;
                }
            }
        }
        if (currentList.containsKey(value)) {
            for (T currentValue : currentList.keySet()) {
                Pair currentPair = (Pair) currentValue;
                if (currentPair.equals(value)) {
                    return currentPair;
                }
            }
        }
        return null;
    }

    public Set<T> keySet() {
        HashSet<T> set = new HashSet<T>();
        for (T key : oldList.keySet()) {
            set.add(key);
        }
        for (T key : currentList.keySet()) {
            set.add(key);
        }
        return set;
    }

    public void remove(T key) {
        if (oldList.containsKey(key)) {
            oldList.remove(key);
        }
        if (currentList.containsKey(key)) {
            currentList.remove(key);
        }
    }

    @Override
    public String toString() {
        if (oldList.size() == 0 && currentList.size() == 0) {
            return " EMPTY ";
        }
        String str = "";
        for (T key : oldList.keySet()) {
            str += " { " + key + " : " + oldList.get(key) + " " + currentList.get(key) + " } ";
        }
        return str;
    }
}