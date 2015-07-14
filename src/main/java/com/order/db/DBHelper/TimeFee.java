package com.order.db.DBHelper;

public class TimeFee {
	private double fee;
	private long updateTime;

	public double getFee() {
		return fee;
	}
	public void setFee(double fee) {
		this.fee = fee;
	}
	public long getUpdateTime() {
		return updateTime;
	}
	public void setUpdateTime(long updateTime) {
		this.updateTime = updateTime;
	}
	
	public TimeFee(double fee, long updateTime) {
		super();
		this.fee = fee;
		this.updateTime = updateTime;
	}
}
