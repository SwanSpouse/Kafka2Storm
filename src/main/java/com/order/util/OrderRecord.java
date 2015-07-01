package com.order.util;

import java.io.Serializable;
import java.util.HashMap;

/* 订购记录，用于缓存和仓库输出表 */
public class OrderRecord implements Serializable {
	private static final long serialVersionUID = 1L;

	Long recordTime;
	String msisdn;
	String sessionId;
	String channelCode;
	String bookID;
	String productID;
	double realfee;
	String provinceId;
	int orderType;
	HashMap<Integer, Integer> rules;

	public Long getRecordTime() {
		return recordTime;
	}

	public void setRecordTime(Long recordTime) {
		this.recordTime = recordTime;
	}

	public String getMsisdn() {
		return msisdn;
	}

	public void setMsisdn(String msisdn) {
		this.msisdn = msisdn;
	}

	public String getSessionId() {
		return sessionId;
	}

	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}

	public String getChannelCode() {
		return channelCode;
	}

	public void setChannelCode(String channelCode) {
		this.channelCode = channelCode;
	}

	public String getBookID() {
		return bookID;
	}

	public void setBookID(String bookID) {
		this.bookID = bookID;
	}

	public String getProductID() {
		return productID;
	}

	public void setProductID(String productID) {
		this.productID = productID;
	}

	public double getRealfee() {
		return realfee;
	}

	public String getProvinceId() {
		return provinceId;
	}

	public void setProvinceId(String provinceId) {
		this.provinceId = provinceId;
	}

	public int getOrderType() {
		return orderType;
	}

	public void setOrderType(int orderType) {
		this.orderType = orderType;
	}

	public void setRealfee(double realfee) {
		this.realfee = realfee;
	}

	public HashMap<Integer, Integer> getRules() {
		return rules;
	}

	public void setRules(HashMap<Integer, Integer> rules) {
		this.rules = rules;
	}

	public OrderRecord() {
		rules = new HashMap<Integer, Integer>();
	}

	@Override
	public boolean equals(Object obj) {
		if ((recordTime.equals(((OrderRecord) obj).recordTime))
				&& (msisdn.equals(((OrderRecord) obj).msisdn))
				&& (sessionId.equals(((OrderRecord) obj).sessionId))
				&& (channelCode.equals(((OrderRecord) obj).channelCode))
				&& (bookID.equals(((OrderRecord) obj).bookID))
				&& (productID.equals(((OrderRecord) obj).productID))
				&& (provinceId.equals(((OrderRecord) obj).provinceId))
				&& (orderType == ((OrderRecord) obj).orderType)) {
			return true;
		} else {
			return false;
		}
	}
	
	public String toString() {
		String mainString =  String.valueOf(recordTime) + " " + TimeParaser.formatTimeInSeconds(recordTime) + " " + msisdn 
				+ " " + sessionId + " " + channelCode + " " + bookID + " " + productID 
				+ " " + provinceId + " " + orderType;
		for (int i = 1; i < 13; i++) {
			mainString += " " + String.valueOf(rules.get(i));
		}
		return mainString;
	}
	
}