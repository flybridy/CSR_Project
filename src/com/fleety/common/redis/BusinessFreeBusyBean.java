package com.fleety.common.redis;

import java.util.Date;

import com.fleety.util.pool.db.redis.RedisTableBean;

public class BusinessFreeBusyBean extends RedisTableBean {

	private int status;
	public int gpsStatus=0;//0定位1不定位	
	private Date lastResportDate; // 汇报日期
	private Date lastSystemDate = new Date();
	
	private int free2Busy=0;
	private int busy2Free=0;
	
	public int getStatus() {
		return status;
	}
	public void setStatus(int status) {
		this.status = status;
	}
	public int getGpsStatus() {
		return gpsStatus;
	}
	public void setGpsStatus(int gpsStatus) {
		this.gpsStatus = gpsStatus;
	}
	public Date getLastResportDate() {
		return lastResportDate;
	}
	public void setLastResportDate(Date lastResportDate) {
		this.lastResportDate = lastResportDate;
	}
	public Date getLastSystemDate() {
		return lastSystemDate;
	}
	public void setLastSystemDate(Date lastSystemDate) {
		this.lastSystemDate = lastSystemDate;
	}
	public int getFree2Busy() {
		return free2Busy;
	}
	public void setFree2Busy(int free2Busy) {
		this.free2Busy = free2Busy;
	}
	public int getBusy2Free() {
		return busy2Free;
	}
	public void setBusy2Free(int busy2Free) {
		this.busy2Free = busy2Free;
	}
		
}
