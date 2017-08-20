package com.fleety.analysis.order.redis;

import com.fleety.util.pool.db.redis.RedisTableBean;

public class OrderRealTimeStatBean extends RedisTableBean {
	private int companyId = 0;
	private int orderTotal = 0;
	private int yougongTotal = 0;
	private int finishTotal = 0;
	private int cancelTotal = 0;
	private int fangkongTotal = 0;

	public int getOrderTotal() {
		return orderTotal;
	}

	public void setOrderTotal(int orderTotal) {
		this.orderTotal = orderTotal;
	}

	public int getYougongTotal() {
		return yougongTotal;
	}

	public void setYougongTotal(int yougongTotal) {
		this.yougongTotal = yougongTotal;
	}

	public int getFinishTotal() {
		return finishTotal;
	}

	public void setFinishTotal(int finishTotal) {
		this.finishTotal = finishTotal;
	}

	public int getCancelTotal() {
		return cancelTotal;
	}

	public void setCancelTotal(int cancelTotal) {
		this.cancelTotal = cancelTotal;
	}

	public int getFangkongTotal() {
		return fangkongTotal;
	}

	public void setFangkongTotal(int fangkongTotal) {
		this.fangkongTotal = fangkongTotal;
	}

	public int getCompanyId() {
		return companyId;
	}

	public void setCompanyId(int companyId) {
		this.companyId = companyId;
	}

}
