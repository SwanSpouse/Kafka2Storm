package com.order.databean.TimeCacheStructures;

import com.order.constant.Constant;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 图书订购列表。Key为图书ID。ID为各个渠道下图书的订购次数。
 * <p/>
 * Created by LiMingji on 2015/5/28.
 */
public class BookOrderList implements Serializable {
    private static final long serialVersionUID = 1L;

    private ConcurrentHashMap<String, CachedList<Integer>> map = null;

    public BookOrderList() {
        map = new ConcurrentHashMap<String, CachedList<Integer>>();
    }

    @Override
    public String toString() {
        String str = "";
        for (String key : map.keySet()) {
            str += "key " + key + " " + map.get(key) + "\n";
        }
        return str;
    }

    /**
     * 将Book的订购记录插入到表里。
     *
     * @param bookId 图书ID
     * @param orderType 订购类型
     * @param currentTime 订购时间。
     */
    public void put(String bookId, int orderType, Long currentTime) {
        if (map.containsKey(bookId)) {
            CachedList<Integer> orderTypeList = map.get(bookId);
            orderTypeList.put(orderType, currentTime);
        } else {
            CachedList<Integer> orderTypeList;
            if (orderType == 4) {
                orderTypeList = new CachedList<Integer>(Constant.ONE_HOUR);
            } else {
                orderTypeList = new CachedList<Integer>(Constant.ONE_HOUR);
            }
            orderTypeList.put(orderType, currentTime);
            map.put(bookId, orderTypeList);
        }
    }

    /**
     * 根据过期时间移除过期数据
     * @param currentTime 当前时间
     */
    public void removeTimeOutData(long currentTime) {
        Iterator<String> itKey = map.keySet().iterator();
        while (itKey.hasNext()) {
            String key = itKey.next();
            CachedList<Integer> list = map.get(key);
            //这个size()方法自带清理功能。
            if (list.size(currentTime, -1) == 0) {
                itKey.remove();
            }
        }
        
        //for (String key : map.keySet()) {
        //    CachedList<Integer> list = map.get(key);
        //    //这个size()方法自带清理功能。
        //    if (list.size(currentTime, -1) == 0) {
        //        map.remove(key);
        //    }
        //}
    }

    /**
     * 获取所有订购的图书本数
     * 获取某个用户订购的所有图书的本书。
     *
     * function: 用作判定用户是否过期的标志之一。
     */
    public int sizeOfOrderBooks(long lastUpdateTime) {
        removeTimeOutData(lastUpdateTime);
        return map.size();
    }

    /**
     *  特定orderType下的图书订购次数
     *  id为图书id
     *  orderType 为订购类型。
     */
    public int sizeOfBookOrderTimesWithOrderType(String id, int orderType, long currentTime, int timeOutSeconds) {
        if (!map.containsKey(id)) {
            return 0;
        }
        CachedList<Integer> orderList = map.get(id);
        return orderList.sizeById(orderType, currentTime, timeOutSeconds);
    }

    /**
     * 获得用户订购的所有图书的ID
     * @return
     */
    public Set<String> keySet() {
        return map.keySet();
    }
}
