package com.fleety.analysis.realtime;

import com.fleety.util.pool.db.redis.RedisTableBean;

public class OrderCompanyRealTimeBean extends RedisTableBean {
	private String companyName;
	private int companyId;
	private int qtTotal;
	private int ztTotal;
	private int jtTotal;

	public String getCompanyName() {
		return companyName;
	}

	public void setCompanyName(String companyName) {
		this.companyName = companyName;
	}

	public int getCompanyId() {
		return companyId;
	}

	public void setCompanyId(int companyId) {
		this.companyId = companyId;
	}

	public int getQtTotal() {
		return qtTotal;
	}

	public void setQtTotal(int qtTotal) {
		this.qtTotal = qtTotal;
	}

	public int getZtTotal() {
		return ztTotal;
	}

	public void setZtTotal(int ztTotal) {
		this.ztTotal = ztTotal;
	}

	public int getJtTotal() {
		return jtTotal;
	}

	public void setJtTotal(int jtTotal) {
		this.jtTotal = jtTotal;
	}
}
