package com.order.bolt.Redis;

import java.io.Serializable;

/**
 * Created by LiMingji on 15/7/14.
 */
public class RealTimeOutputDBItem implements Serializable {
    private static final long serialVersionUID = 1L;

    private String recordTime;
    private String provinceId;
    private String channelCode;
    private String contentId;
    private String contentType;
    private String rule;
    private Double realInfoFee;

    public String getChannelCode() {
        return channelCode;
    }

    public RealTimeOutputDBItem setChannelCode(String channelCode) {
        this.channelCode = channelCode;
        return this;
    }

    public String getRecordTime() {
        return recordTime;
    }

    public RealTimeOutputDBItem setRecordTime(String recordTime) {
        this.recordTime = recordTime;
        return this;
    }

    public String getProvinceId() {
        return provinceId;
    }

    public RealTimeOutputDBItem setProvinceId(String provinceId) {
        this.provinceId = provinceId;
        return this;
    }

    public String getContentId() {
        return contentId;
    }

    public RealTimeOutputDBItem setContentId(String contentId) {
        this.contentId = contentId;
        return this;
    }

    public String getContentType() {
        return contentType;
    }

    public RealTimeOutputDBItem setContentType(String contentType) {
        this.contentType = contentType;
        return this;
    }

    public String getRule() {
        return rule;
    }

    public RealTimeOutputDBItem setRule(String rule) {
        this.rule = rule;
        return this;
    }

    public Double getRealInfoFee() {
        return realInfoFee;
    }

    public RealTimeOutputDBItem setRealInfoFee(Double realInfoFee) {
        this.realInfoFee = realInfoFee;
        return this;
    }
}
