package com.fleety.job.video;

import com.fleety.util.pool.db.redis.RedisTableBean;
/*
 * 车辆表
 * create by mike.li on 2014-02-27
 */
public class Vehicle extends RedisTableBean{
	private int mdtId;			//车辆对应的设备号
	private int oid;           //机构Id,父类UId为车牌号码
	private int mid;           //车型Id
	private String dname;      //司机名称 
	private String dtel;       //司机电话  电话号码可能有0开头
	private String clitel;     //终端电话
	private int vtype;         //车种Id
	private String status;     //车辆状态，如故障、保修、停运
	private String videoCode; //视频编码。 设备编码，摄影头 (EMC590,0;EMC590,1;EMC590,2;EMC590,3);
	
	
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

	public String getVideoCode() {
		return videoCode;
	}
	public void setVideoCode(String videoCode) {
		this.videoCode = videoCode;
	}

}
