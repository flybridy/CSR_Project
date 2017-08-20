package com.fleety.analysis.realtime;

import java.util.Date;

import com.fleety.util.pool.db.redis.RedisTableBean;

public class DestLoginStatusInfoBean extends RedisTableBean{
	private int mdtId=0;
	private String destNo="";
	private String serviceNo="";
	private String driverName="";
	private String gender="0";//1ÄÐ0Å®
	private String driverTel="";		
	private Date driverLoginTime=null;
	private Date driverLogoutTime=null;
	private int actionFlag=0;//1µÇÂ½0Ç©ÍË
	public int getMdtId() {
		return mdtId;
	}
	public void setMdtId(int mdtId) {
		this.mdtId = mdtId;
	}
	public String getDestNo() {
		return destNo;
	}
	public void setDestNo(String destNo) {
		this.destNo = destNo;
	}
	public String getServiceNo() {
		return serviceNo;
	}
	public void setServiceNo(String serviceNo) {
		this.serviceNo = serviceNo;
	}
	public String getDriverName() {
		return driverName;
	}
	public void setDriverName(String driverName) {
		this.driverName = driverName;
	}
	public String getGender() {
		return gender;
	}
	public void setGender(String gender) {
		this.gender = gender;
	}
	public String getDriverTel() {
		return driverTel;
	}
	public void setDriverTel(String driverTel) {
		this.driverTel = driverTel;
	}
	public Date getDriverLoginTime() {
		return driverLoginTime;
	}
	public void setDriverLoginTime(Date driverLoginTime) {
		this.driverLoginTime = driverLoginTime;
	}
	public Date getDriverLogoutTime() {
		return driverLogoutTime;
	}
	public void setDriverLogoutTime(Date driverLogoutTime) {
		this.driverLogoutTime = driverLogoutTime;
	}
	public int getActionFlag() {
		return actionFlag;
	}
	public void setActionFlag(int actionFlag) {
		this.actionFlag = actionFlag;
	}
	
}
