package com.fleety.analysis.realtime;

import com.fleety.util.pool.db.redis.RedisTableBean;

public class StopCarMessageBean extends RedisTableBean {
	private int company_id = 0;// 企业id
	private int area_id = 0;// 区域id
	private String car_id=null;
	private int car_type=0;//车型
	private String com_name=null;
	private String service_no=null;
	public String getService_no() {
		return service_no;
	}
	public void setService_no(String service_no) {
		this.service_no = service_no;
	}
	private int index = 0;// 当前信息一小时实时数据的次序（0-6）  
	public int getIndex() {
		return index;
	}
	public void setIndex(int index) {
		this.index = index;
	}
	public int getCompany_id() {
		return company_id;
	}
	public String getCom_name() {
		return com_name;
	}
	public void setCom_name(String com_name) {
		this.com_name = com_name;
	}
	public void setCompany_id(int company_id) {
		this.company_id = company_id;
	}
	public int getArea_id() {
		return area_id;
	}
	public void setArea_id(int area_id) {
		this.area_id = area_id;
	}
	public String getCar_id() {
		return car_id;
	}
	public void setCar_id(String car_id) {
		this.car_id = car_id;
	}
	public int getCar_type() {
		return car_type;
	}
	public void setCar_type(int car_type) {
		this.car_type = car_type;
	}

}
