package com.fleety.analysis.operation;

public class CarStateInfo {

	private int teamId;//车队id
	private int Knum = 0;//空车数
	private int Znum = 0;//重车数
	private int onlineNum = 0;//在线数
	private int offlineNum = 0;//离线数
	private int guzhangNum = 0;//故障数
	private int carNum = 0;//车辆数
	
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
