package com.fleety.analysis.operation.task;

import java.util.Date;

import com.fleety.util.pool.db.redis.RedisTableBean;

public class ActuralWorkDurationTimeBean extends RedisTableBean{
//	public String plateNo;
	private int    companyId;
	private String companyName;
	private float  workDuration;
	private Date   analysisTime;
	private Date   startTime;
	private Date   endTime;
	public int getCompanyId() {
		return companyId;
	}
	public void setCompanyId(int companyId) {
		this.companyId = companyId;
	}
	public String getCompanyName() {
		return companyName;
	}
	public void setCompanyName(String companyName) {
		this.companyName = companyName;
	}
	public float getWorkDuration() {
		return workDuration;
	}
	public void setWorkDuration(float workDuration) {
		this.workDuration = workDuration;
	}
	public Date getAnalysisTime() {
		return analysisTime;
	}
	public void setAnalysisTime(Date analysisTime) {
		this.analysisTime = analysisTime;
	}
	public Date getStartTime() {
		return startTime;
	}
	public void setStartTime(Date startTime) {
		this.startTime = startTime;
	}
	public Date getEndTime() {
		return endTime;
	}
	public void setEndTime(Date endTime) {
		this.endTime = endTime;
	}
	
}
