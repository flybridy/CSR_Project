package com.fleety.job.video;

import java.io.Serializable;

import com.fleety.util.pool.db.redis.RedisTableBean;
/*
 * ������
 * create by mike.li on 2014-02-27
 */
public class Org extends RedisTableBean implements Serializable {
    private String name;    //��������,����UID�洢����ID
    private int  fid;       //������Id
    private String orgCode1; //ҵ��id(�����������Ӧ��ϵ)
    private String tempVal ; //��ʱ�洢
    private int orderBy;       // �������������������ʹ�� add by mike.li on 2015-10-14
    
    
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public int getFid() {
		return fid;
	}
	public void setFid(int fid) {
		this.fid = fid;
	}
	public String getOrgCode1() {
		return orgCode1;
	}
	public void setOrgCode1(String orgCode) {
		this.orgCode1 = orgCode;
	}
	public String getTempVal() {
		return tempVal;
	}
	public void setTempVal(String tempVal) {
		this.tempVal = tempVal;
	}
	public int getOrderBy() {
		return orderBy;
	}
	public void setOrderBy(int orderBy) {
		this.orderBy = orderBy;
	}
	
}
