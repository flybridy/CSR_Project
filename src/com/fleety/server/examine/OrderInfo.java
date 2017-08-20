package com.fleety.server.examine;

import java.util.Date;

public class OrderInfo{
	private int orderId;
	private String phone;
	private int user_id;
	private String destNo;
	private String driver_id;
	private Date carWantedTime;//用车时间
	
	
	public int getOrderId() {
		return orderId;
	}
	public void setOrderId(int orderId) {
		this.orderId = orderId;
	}
	public String getPhone() {
		return phone;
	}
	public void setPhone(String phone) {
		this.phone = phone;
	}
	public int getUser_id() {
		return user_id;
	}
	public void setUser_id(int userId) {
		user_id = userId;
	}
	public String getDestNo() {
		return destNo;
	}
	public void setDestNo(String destNo) {
		this.destNo = destNo;
	}
	public Date getCarWantedTime() {
		return carWantedTime;
	}
	public void setCarWantedTime(Date carWantedTime) {
		this.carWantedTime = carWantedTime;
	}
	public String getDriver_id() {
		return driver_id;
	}
	public void setDriver_id(String driverId) {
		driver_id = driverId;
	}
	
	
}
