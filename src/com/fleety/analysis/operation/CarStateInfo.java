package com.fleety.analysis.operation;

public class CarStateInfo {

	private int teamId;//����id
	private int Knum = 0;//�ճ���
	private int Znum = 0;//�س���
	private int onlineNum = 0;//������
	private int offlineNum = 0;//������
	private int guzhangNum = 0;//������
	private int carNum = 0;//������
	
	public int getCarNum() {
		return carNum;
	}
	public void setCarNum(int carNum) {
		this.carNum = carNum;
	}
	public int getTeamId() {
		return teamId;
	}
	public void setTeamId(int teamId) {
		this.teamId = teamId;
	}
	public int getKnum() {
		return Knum;
	}
	public void setKnum(int knum) {
		Knum = knum;
	}
	public int getZnum() {
		return Znum;
	}
	public void setZnum(int znum) {
		Znum = znum;
	}
	public int getOnlineNum() {
		return onlineNum;
	}
	public void setOnlineNum(int onlineNum) {
		this.onlineNum = onlineNum;
	}
	public int getOfflineNum() {
		return offlineNum;
	}
	public void setOfflineNum(int offlineNum) {
		this.offlineNum = offlineNum;
	}
	public int getGuzhangNum() {
		return guzhangNum;
	}
	public void setGuzhangNum(int guzhangNum) {
		this.guzhangNum = guzhangNum;
	}
	
	
}
