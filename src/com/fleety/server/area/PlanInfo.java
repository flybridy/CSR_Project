package com.fleety.server.area;

import java.util.Date;
import java.util.HashMap;
import java.util.List;

public class PlanInfo {
	private int plan_id;
	private String plan_name;
	private Date start_time;
	private Date end_time;
	private String start_time1;
	private String end_time1;
	private int area_id;
	private int status;
	private HashMap<String,BindCarInfo> bInfos;
	
	public PlanInfo(){
		
	}
	public PlanInfo(int planId, String planName, Date startTime, Date endTime,
			int areaId, int status,HashMap<String,BindCarInfo> bInfos) {
		plan_id = planId;
		plan_name = planName;
		start_time = startTime;
		end_time = endTime;
		area_id = areaId;
		this.status = status;
		this.bInfos = bInfos;
	}
	public int getPlan_id() {
		return plan_id;
	}
	public void setPlan_id(int planId) {
		plan_id = planId;
	}
	public String getPlan_name() {
		return plan_name;
	}
	public void setPlan_name(String planName) {
		plan_name = planName;
	}
	public Date getStart_time() {
		return start_time;
	}
	public void setStart_time(Date startTime) {
		start_time = startTime;
	}
	public Date getEnd_time() {
		return end_time;
	}
	public void setEnd_time(Date endTime) {
		end_time = endTime;
	}
	public int getArea_id() {
		return area_id;
	}
	public void setArea_id(int areaId) {
		area_id = areaId;
	}
	public int getStatus() {
		return status;
	}
	public void setStatus(int status) {
		this.status = status;
	}
	public HashMap<String,BindCarInfo> getbInfos() {
		return bInfos;
	}
	public void setbInfos(HashMap<String,BindCarInfo> bInfos) {
		this.bInfos = bInfos;
	}
	public String getStart_time1() {
		return start_time1;
	}
	public void setStart_time1(String startTime1) {
		start_time1 = startTime1;
	}
	public String getEnd_time1() {
		return end_time1;
	}
	public void setEnd_time1(String endTime1) {
		end_time1 = endTime1;
	}
	
}
