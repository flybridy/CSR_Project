package com.fleety.common.redis;

import java.util.Date;

import com.fleety.util.pool.db.redis.RedisTableBean;

public class BusinessLastBusyBean extends RedisTableBean {
	private String carNo;
	private Date lastBusyTime;
	private Date trackDate;

	public Date getTrackDate() {
		return trackDate;
	}

	public void setTrackDate(Date trackDate) {
		this.trackDate = trackDate;
	}

	public String getCarNo() {
		return carNo;
	}

	public void setCarNo(String carNo) {
		this.carNo = carNo;
	}

	public Date getLastBusyTime() {
		return lastBusyTime;
	}

	public void setLastBusyTime(Date lastBusyTime) {
		this.lastBusyTime = lastBusyTime;
	}
}
