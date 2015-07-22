package com.order.util;

import java.io.Serializable;

/**
 * 用作OrderSplit的缓存
 * Created by LiMingji on 15/7/22.
 */
public class OrderItem implements Serializable {
    private static final long serialVersionUID = 1L;

    private String msisdn;
    private Long recordTime;
    private String terminal;
    private String platform;
    private String orderType;
    private String productID;
    private String bookID;
    private String chapterID;
    private String channelCode;
    private String cost;
    private String provinceId;
    private String wapIp;
    private String sessionId;
    private String promotionid;

    public OrderItem(String msisdn, Long recordTime, String terminal, String platform,
                     String orderType, String productID, String bookID, String chapterID,
                     String channelCode, String cost, String provinceId, String wapIp,
                     String sessionId, String promotionid) {
        this.msisdn = msisdn;
        this.recordTime = recordTime;
        this.terminal = terminal;
        this.platform = platform;
        this.orderType = orderType;
        this.productID = productID;
        this.bookID = bookID;
        this.chapterID = chapterID;
        this.channelCode = channelCode;
        this.cost = cost;
        this.provinceId = provinceId;
        this.wapIp = wapIp;
        this.sessionId = sessionId;
        this.promotionid = promotionid;
    }

    public String getMsisdn() {
        return msisdn;
    }

    public Long getRecordTime() {
        return recordTime;
    }

    public String getTerminal() {
        return terminal;
    }

    public String getPlatform() {
        return platform;
    }

    public String getOrderType() {
        return orderType;
    }

    public String getProductID() {
        return productID;
    }

    public String getBookID() {
        return bookID;
    }

    public String getChapterID() {
        return chapterID;
    }

    public String getChannelCode() {
        return channelCode;
    }

    public String getCost() {
        return cost;
    }

    public String getProvinceId() {
        return provinceId;
    }

    public String getWapIp() {
        return wapIp;
    }

    public String getSessionId() {
        return sessionId;
    }

    public String getPromotionid() {
        return promotionid;
    }
}














