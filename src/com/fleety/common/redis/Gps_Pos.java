package com.fleety.common.redis;

import java.util.Date;
import com.fleety.util.pool.db.redis.RedisTableBean;

public class Gps_Pos extends RedisTableBean {
	private int state;
	private int lo; // ���� ��׼���ȳ�10��7�η��洢
	private int la; // γ�� ��׼γ�ȳ�10��7�η��洢
	private short dit; // ��ǰ����,��λ��
	private String pos; // ��ǰλ��
	private short spd; // ��ǰ�ٶ� ����/Сʱ
	private Date dt; // �㱨����
	public int acc = -1;//acc״̬0��1��
	private Date sysDate = new Date();
	public int gpsStatus=0;//0��λ1����λ	
	
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
