package com.fleety.analysis.realtime;

import com.fleety.util.pool.db.redis.RedisTableBean;
/*
 * ������
 * create by mike.li on 2014-02-27
 */
public class Vehicle extends RedisTableBean{
	private int mdtId;			//������Ӧ���豸��
	private int oid;           //����Id,����UIdΪ���ƺ���
	private int mid;           //����Id
	private String dname;      //˾������ 
	private String dtel;       //˾���绰  �绰���������0��ͷ
	private String clitel;     //�ն˵绰
	private int vtype;         //����Id
	private String status;     //����״̬������ϡ����ޡ�ͣ��
	
	
	public int getMdtId() {
		return mdtId;
	}
	public void setMdtId(int mdtId) {
		this.mdtId = mdtId;
	}
	public int getOid() {
		return oid;
	}
	public void setOid(int oid) {
		this.oid = oid;
	}
	public int getMid() {
		return mid;
	}
	public void setMid(int mid) {
		this.mid = mid;
	}
	public String getDname() {
		return dname;
	}
	public void setDname(String dname) {
		this.dname = dname;
	}
	public String getDtel() {
		return dtel;
	}
	public void setDtel(String dtel) {
		this.dtel = dtel;
	}
	public String getClitel() {
		return clitel;
	}
	public void setClitel(String clitel) {
		this.clitel = clitel;
	}
	public int getVtype() {
		return vtype;
	}
	public void setVtype(int vtype) {
		this.vtype = vtype;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}	

}
