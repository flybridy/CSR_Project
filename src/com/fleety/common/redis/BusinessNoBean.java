package com.fleety.common.redis;

import java.util.Date;

import com.fleety.util.pool.db.redis.RedisTableBean;

public class BusinessNoBean extends RedisTableBean {
	
	private int pid;
	private int fid;
	private int mdtid;
	private int carType;
	private int comId;
	private String companyName;
	private int trackNum;
	private String driverInfo ;
	private int busStatus;
	
	private Date lastResportDate; // »ã±¨ÈÕÆÚ
	private Date lastSystemDate = new Date();
	private Date trackStartTime;
	private Date trackEndTime;
	public int getMdtid() {
		return mdtid;
	}
	public void setMdtid(int mdtid) {
		this.mdtid = mdtid;
	}
	public int getCarType() {
		return carType;
	}
	public void setCarType(int carType) {
		this.carType = carType;
	}
	public int getComId() {
		return comId;
	}
	public void setComId(int comId) {
		this.comId = comId;
	}
	public String getCompanyName() {
		return companyName;
	}
	public void setCompanyName(String companyName) {
		this.companyName = companyName;
	}
	public int getTrackNum() {
		return trackNum;
	}
	public void setTrackNum(int trackNum) {
		this.trackNum = trackNum;
	}
	public String getDriverInfo() {
		return driverInfo;
	}
	public void setDriverInfo(String driverInfo) {
		this.driverInfo = driverInfo;
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
	public int getPid() {
		return pid;
	}
	public void setPid(int pid) {
		this.pid = pid;
	}
	public int getFid() {
		return fid;
	}
	public void setFid(int fid) {
		this.fid = fid;
	}
	public int getBusStatus() {
		return busStatus;
	}
	public void setBusStatus(int busStatus) {
		this.busStatus = busStatus;
	}
	public Date getTrackStartTime() {
		return trackStartTime;
	}
	public void setTrackStartTime(Date trackStartTime) {
		this.trackStartTime = trackStartTime;
	}
	public Date getTrackEndTime() {
		return trackEndTime;
	}
	public void setTrackEndTime(Date trackEndTime) {
		this.trackEndTime = trackEndTime;
	}
	
	
		
}
