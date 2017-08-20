package com.fleety.job.video;

public class CarInfo  {
	
	private String parentid;
	private String carno;
	private String status;
	private String channels;
	
	public CarInfo() {
		super();
	}
	public CarInfo(String parentid, String carno, String status, String channels) {
		super();
		this.parentid = parentid;
		this.carno = carno;
		this.status = status;
		this.channels = channels;
	}
	public String getParentid() {
		return parentid;
	}
	public void setParentid(String parentid) {
		this.parentid = parentid;
	}
	public String getCarno() {
		return carno;
	}
	public void setCarno(String carno) {
		this.carno = carno;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public String getChannels() {
		return channels;
	}
	public void setChannels(String channels) {
		this.channels = channels;
	}
	
	


}
