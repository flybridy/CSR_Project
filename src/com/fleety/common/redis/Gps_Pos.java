package com.fleety.common.redis;

import java.util.Date;
import com.fleety.util.pool.db.redis.RedisTableBean;

public class Gps_Pos extends RedisTableBean {
	private int state;
	private int lo; // 经度 标准经度乘10的7次方存储
	private int la; // 纬度 标准纬度乘10的7次方存储
	private short dit; // 当前方向,单位度
	private String pos; // 当前位置
	private short spd; // 当前速度 公里/小时
	private Date dt; // 汇报日期
	public int acc = -1;//acc状态0关1开
	private Date sysDate = new Date();
	public int gpsStatus=0;//0定位1不定位	
	
	public int getState() {
		return state;
	}

	public void setState(int state) {
		this.state = state;
	}

	public int getLo() {
		return lo;
	}

	public void setLo(int lo) {
		this.lo = lo;
	}

	public int getLa() {
		return la;
	}

	public void setLa(int la) {
		this.la = la;
	}

	public short getDit() {
		return dit;
	}

	public void setDit(short dit) {
		this.dit = dit;
	}

	public String getPos() {
		return pos;
	}

	public void setPos(String pos) {
		this.pos = pos;
	}

	public short getSpd() {
		return spd;
	}

	public void setSpd(short spd) {
		this.spd = spd;
	}

	public Date getDt() {
		return dt;
	}

	public void setDt(Date dt) {
		this.dt = dt;
	}

	public int getAcc() {
		return acc;
	}

	public void setAcc(int acc) {
		this.acc = acc;
	}

	public Date getSysDate() {
		return sysDate;
	}

	public void setSysDate(Date sysDate) {
		this.sysDate = sysDate;
	}
	
	public int getGpsStatus() {
		return gpsStatus;
	}

	public void setGpsStatus(int gpsStatus) {
		this.gpsStatus = gpsStatus;
	}
}
