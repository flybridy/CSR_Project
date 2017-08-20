package com.fleety.analysis.operation.task;

import java.util.Date;

import com.fleety.util.pool.db.redis.RedisTableBean;

public class DestStopLevelRecordBean extends RedisTableBean {
	private long startTime = 0;
	private long endTime = 0;
	private long recordTime = 0;
	
	public long getStartTime() {
		return startTime;
	}
	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}
	public long getEndTime() {
		return endTime;
	}
	public void setEndTime(long endTime) {
		this.endTime = endTime;
	}
	public long getRecordTime() {
		return recordTime;
	}
	public void setRecordTime(long recordTime) {
		this.recordTime = recordTime;
	}

	
}
