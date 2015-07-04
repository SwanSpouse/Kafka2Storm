package com.order.databean.TimeCacheStructures;

import com.order.constant.Constant;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Set;

/**
 * 图书订购列表。Key为图书ID。ID为各个渠道下图书的订购次数。
 *
 * Created by LiMingji on 2015/5/28.
 */
public class BookOrderList implements Serializable{
    private static final long serialVersionUID = 1L;

    private HashMap<String, CachedList<Integer>> map = null;

    private static Object LOCK = null;
    public BookOrderList() {
        LOCK = new Object();
        map = new HashMap<String, CachedList<Integer>>();
    }

    @Override
    public String toString() {
        String str = "";
        for (String key : map.keySet()) {
            str += "key " + key + " " + map.get(key) + "\n";
        }
        return str;
    }

    public void put(String bookId, int orderType, Long currentTime) {
        if (LOCK == null) {
            LOCK = new Object();
        }
        synchronized (LOCK) {
            if (map.containsKey(bookId)) {
                CachedList<Integer> orderTypeList = map.get(bookId);
                orderTypeList.put(orderType, currentTime);
            } else {
                CachedList<Integer> orderTypeList = null;
                if (orderType == 4) {
                    orderTypeList = new CachedList<Integer>(Constant.THREE_MINUTES);
                } else {
                    orderTypeList = new CachedList<Integer>(Constant.FIVE_MINUTES);
                }
                orderTypeList.put(orderType, currentTime);
                map.put(bookId, orderTypeList);
            }
        }
    }

    /**
     * 根据过期时间移除过期数据
     * @param currentTime 当前时间
     */
    public void removeTimeOutData(long currentTime) {
        if (LOCK == null) {
            LOCK = new Object();
        }
        if (map == null) {
            map = new HashMap<String, CachedList<Integer>>();
            return ;
        }
        synchronized (LOCK) {
            for (String key : map.keySet()) {
                CachedList<Integer> list = map.get(key);
                //这个size()方法自带清理功能。
                if (list.size(currentTime) == 0) {
                    map.remove(key);
                }
            }
        }
    }

    //获取所有订购的图书本数
    public int sizeOfOrderBooks(long lastUpdateTime) {
        if (LOCK == null) {
            LOCK = new Object();
        }
        removeTimeOutData(lastUpdateTime);
        synchronized (LOCK) {
            return map.size();
        }
    }

    //特定图书的订购次数
    public int sizeOfBookOrderTimes(String id) {
        if (LOCK == null) {
            LOCK = new Object();
        }
        synchronized (LOCK) {
            if (!map.containsKey(id)) {
                return 0;
            }
            CachedList<Integer> orderList = map.get(id);
            return orderList.size(System.currentTimeMillis());
        }
    }

    //特定orderType下的图书订购次数
    public int sizeOfBookOrderTimesWithOrderType(String id, int orderType) {
        if (LOCK == null) {
            LOCK = new Object();
        }
        synchronized (LOCK) {
            if (!map.containsKey(id)) {
                return 0;
            }
            CachedList<Integer> orderList = map.get(id);
            return orderList.sizeById(orderType);
        }
    }

    public Set<String> keySet() {
        if (LOCK == null) {
            LOCK = new Object();
        }
        synchronized (LOCK) {
            return map.keySet();
        }
    }
}
