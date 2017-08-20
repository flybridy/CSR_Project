package com.fleety.analysis.operation.task;


import java.util.Date;

import com.fleety.util.pool.db.redis.RedisTableBean;

public class DriverLoginOutBean  extends RedisTableBean {
	
	private String    serviceNo;
	private String    loginMdtId;
	private String    logoutMdtId;
	private Date      loginDate;
	private Date      logoutDate;
	private String    driverInfo;
	public String getServiceNo() {
		return serviceNo;
	}
	public void setServiceNo(String serviceNo) {
		this.serviceNo = serviceNo;
	}
	public String getLoginMdtId() {
		return loginMdtId;
	}
	public void setLoginMdtId(String loginMdtId) {
		this.loginMdtId = loginMdtId;
	}
	public String getLogoutMdtId() {
		return logoutMdtId;
	}
	public void setLogoutMdtId(String logoutMdtId) {
		this.logoutMdtId = logoutMdtId;
	}
	public Date getLoginDate() {
		return loginDate;
	}
	public void setLoginDate(Date loginDate) {
		this.loginDate = loginDate;
	}
	public Date getLogoutDate() {
		return logoutDate;
	}
	public void setLogoutDate(Date logoutDate) {
		this.logoutDate = logoutDate;
	}
	public String getDriverInfo() {
		return driverInfo;
	}
	public void setDriverInfo(String driverInfo) {
		this.driverInfo = driverInfo;
	}
}
