package com.fleety.analysis.realtime;

import com.fleety.util.pool.db.redis.RedisTableBean;

public class AreaCompanyCarBean extends RedisTableBean{
	private int area_id;     //����id
	private int company_id;  //��ҵid
	private String car_no;  //����
	private int status;     //����״̬
	private double la;      //��������
	private double lo;      //����γ��
	private int car_type;   //����
	private String service_no;//��ʻԱ
	private int index = 0;// ��ǰ��ϢһСʱʵʱ���ݵĴ���0-6��   
	
	public String getService_no() {
		return service_no;
	}
	public void setService_no(String service_no) {
		this.service_no = service_no;
	}
	public int getArea_id() {
		return area_id;
	}
	public void setArea_id(int area_id) {
		this.area_id = area_id;
	}
	public int getCompany_id() {
		return company_id;
	}
	public void setCompany_id(int company_id) {
		this.company_id = company_id;
	}
	public String getCar_no() {
		return car_no;
	}
	public void setCar_no(String car_no) {
		this.car_no = car_no;
	}
	public int getStatus() {
		return status;
	}
	public void setStatus(int status) {
		this.status = status;
	}
	public double getLa() {
		return la;
	}
	public void setLa(double la) {
		this.la = la;
	}
	public double getLo() {
		return lo;
	}
	public void setLo(double lo) {
		this.lo = lo;
	}
	public int getCar_type() {
		return car_type;
	}
	public void setCar_type(int car_type) {
		this.car_type = car_type;
	}
	public int getIndex() {
		return index;
	}
	public void setIndex(int index) {
		this.index = index;
	}
	
}
