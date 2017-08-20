package com.fleety.analysis.realtime;

import com.fleety.util.pool.db.redis.RedisTableBean;

public class BusinessRealTimeBean extends RedisTableBean {
	private String carNo; // ����
	private int typeId;
	private int companyId;
	private String companyName;
	private String aliasName;
	private int workTimes; // �ܲ��
	private int dispatchTimes;// ������
	private double totalDistance;// �����
	private double workDistance;// Ӫ�����
	private double freeDistance;// ��ʻ���
	private int waitHour; // �Ⱥ�ʱ����Сʱ��
	private int waitMinute;// �Ⱥ�ʱ�����֣�
	private int waitSecond;// �Ⱥ�ʱ�����룩
	private double income; // Ӫ��
	private double dispatchInCome;// ���Ӫ��

	public String getCarNo() {
		return carNo;
	}

	public void setCarNo(String carNo) {
		this.carNo = carNo;
	}

	public int getWorkTimes() {
		return workTimes;
	}

	public void setWorkTimes(int workTimes) {
		this.workTimes = workTimes;
	}

	public int getDispatchTimes() {
		return dispatchTimes;
	}

	public void setDispatchTimes(int dispatchTimes) {
		this.dispatchTimes = dispatchTimes;
	}

	public double getTotalDistance() {
		return totalDistance;
	}

	public void setTotalDistance(double totalDistance) {
		this.totalDistance = totalDistance;
	}

	public double getWorkDistance() {
		return workDistance;
	}

	public void setWorkDistance(double workDistance) {
		this.workDistance = workDistance;
	}

	public double getFreeDistance() {
		return freeDistance;
	}

	public void setFreeDistance(double freeDistance) {
		this.freeDistance = freeDistance;
	}

	public int getWaitHour() {
		return waitHour;
	}

	public void setWaitHour(int waitHour) {
		this.waitHour = waitHour;
	}

	public int getWaitMinute() {
		return waitMinute;
	}

	public void setWaitMinute(int waitMinute) {
		this.waitMinute = waitMinute;
	}

	public int getWaitSecond() {
		return waitSecond;
	}

	public void setWaitSecond(int waitSecond) {
		this.waitSecond = waitSecond;
	}

	public double getIncome() {
		return income;
	}

	public void setIncome(double income) {
		this.income = income;
	}

	public double getDispatchInCome() {
		return dispatchInCome;
	}

	public void setDispatchInCome(double dispatchInCome) {
		this.dispatchInCome = dispatchInCome;
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

	public String getAliasName() {
		return aliasName;
	}

	public void setAliasName(String aliasName) {
		this.aliasName = aliasName;
	}

	public int getTypeId() {
		return typeId;
	}

	public void setTypeId(int typeId) {
		this.typeId = typeId;
	}

}
