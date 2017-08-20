package com.fleety.analysis.realtime;

import java.util.Date;

import com.fleety.util.pool.db.redis.RedisTableBean;

public class DriverBusinessRealTimeBean extends RedisTableBean {
	private String driverId;
	private String driverName;
	private int companyId;
	private String companyName;
	private int driverGrade;
	private float durationTime;
	private int workTimes;
	private float totalDistance;
	private float workDistance;
	private float freeDistance;
	private int waitingHour;
	private int waitingMinute;
	private int waitingSecond;
	private float totalIncome;
	private float workIncome;
	private float fuelIncome;
	private int telcallTimes;
	private int telcallFinishTimes;
	private int serviceEvaluateTimes;
	private int satisfisfyTimes;
	private int unsatisfyTimes;
	private int highlySatisfisfyTimes;
	private int unJudgeTimes;
	private Date analysisTime;
	private String workDate;
	private Date startTime;
	private Date endTime;
	private String plateNo;
	private int workTimeSeconds;

	public String getDriverId() {
		return driverId;
	}

	public void setDriverId(String driverId) {
		this.driverId = driverId;
	}

	public String getDriverName() {
		return driverName;
	}

	public void setDriverName(String driverName) {
		this.driverName = driverName;
	}

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

	public int getDriverGrade() {
		return driverGrade;
	}

	public void setDriverGrade(int driverGrade) {
		this.driverGrade = driverGrade;
	}

	public float getDurationTime() {
		return durationTime;
	}

	public void setDurationTime(float durationTime) {
		this.durationTime = durationTime;
	}

	public int getWorkTimes() {
		return workTimes;
	}

	public void setWorkTimes(int workTimes) {
		this.workTimes = workTimes;
	}

	public float getTotalDistance() {
		return totalDistance;
	}

	public void setTotalDistance(float totalDistance) {
		this.totalDistance = totalDistance;
	}

	public float getWorkDistance() {
		return workDistance;
	}

	public void setWorkDistance(float workDistance) {
		this.workDistance = workDistance;
	}

	public float getFreeDistance() {
		return freeDistance;
	}

	public void setFreeDistance(float freeDistance) {
		this.freeDistance = freeDistance;
	}

	public int getWaitingHour() {
		return waitingHour;
	}

	public void setWaitingHour(int waitingHour) {
		this.waitingHour = waitingHour;
	}

	public int getWaitingMinute() {
		return waitingMinute;
	}

	public void setWaitingMinute(int waitingMinute) {
		this.waitingMinute = waitingMinute;
	}

	public int getWaitingSecond() {
		return waitingSecond;
	}

	public void setWaitingSecond(int waitingSecond) {
		this.waitingSecond = waitingSecond;
	}

	public float getTotalIncome() {
		return totalIncome;
	}

	public void setTotalIncome(float totalIncome) {
		this.totalIncome = totalIncome;
	}

	public float getWorkIncome() {
		return workIncome;
	}

	public void setWorkIncome(float workIncome) {
		this.workIncome = workIncome;
	}

	public float getFuelIncome() {
		return fuelIncome;
	}

	public void setFuelIncome(float fuelIncome) {
		this.fuelIncome = fuelIncome;
	}

	public int getTelcallTimes() {
		return telcallTimes;
	}

	public void setTelcallTimes(int telcallTimes) {
		this.telcallTimes = telcallTimes;
	}

	public int getTelcallFinishTimes() {
		return telcallFinishTimes;
	}

	public void setTelcallFinishTimes(int telcallFinishTimes) {
		this.telcallFinishTimes = telcallFinishTimes;
	}

	public int getServiceEvaluateTimes() {
		return serviceEvaluateTimes;
	}

	public void setServiceEvaluateTimes(int serviceEvaluateTimes) {
		this.serviceEvaluateTimes = serviceEvaluateTimes;
	}

	public int getSatisfisfyTimes() {
		return satisfisfyTimes;
	}

	public void setSatisfisfyTimes(int satisfisfyTimes) {
		this.satisfisfyTimes = satisfisfyTimes;
	}

	public int getUnsatisfyTimes() {
		return unsatisfyTimes;
	}

	public void setUnsatisfyTimes(int unsatisfyTimes) {
		this.unsatisfyTimes = unsatisfyTimes;
	}

	public int getHighlySatisfisfyTimes() {
		return highlySatisfisfyTimes;
	}

	public void setHighlySatisfisfyTimes(int highlySatisfisfyTimes) {
		this.highlySatisfisfyTimes = highlySatisfisfyTimes;
	}

	public int getUnJudgeTimes() {
		return unJudgeTimes;
	}

	public void setUnJudgeTimes(int unJudgeTimes) {
		this.unJudgeTimes = unJudgeTimes;
	}

	public Date getAnalysisTime() {
		return analysisTime;
	}

	public void setAnalysisTime(Date analysisTime) {
		this.analysisTime = analysisTime;
	}

	public String getWorkDate() {
		return workDate;
	}

	public void setWorkDate(String workDate) {
		this.workDate = workDate;
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

	public String getPlateNo() {
		return plateNo;
	}

	public void setPlateNo(String plateNo) {
		this.plateNo = plateNo;
	}

	public int getWorkTimeSeconds() {
		return workTimeSeconds;
	}

	public void setWorkTimeSeconds(int workTimeSeconds) {
		this.workTimeSeconds = workTimeSeconds;
	}
}
