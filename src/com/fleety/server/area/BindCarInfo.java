package com.fleety.server.area;

public class BindCarInfo {
	private int plan_id;
	private String car_no;
	
	public BindCarInfo(int planId, String carNo) {
		super();
		plan_id = planId;
		car_no = carNo;
	}
	public int getPlan_id() {
		return plan_id;
	}
	public void setPlan_id(int planId) {
		plan_id = planId;
	}
	public String getCar_no() {
		return car_no;
	}
	public void setCar_no(String carNo) {
		car_no = carNo;
	}
}
