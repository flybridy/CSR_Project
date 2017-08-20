package com.fleety.analysis.realtime;

import com.fleety.util.pool.db.redis.RedisTableBean;

public class GuzhangRealTimeBean extends RedisTableBean {
	private int companyId;
	private String companyName;
	private String aliasName;
	private int overSpeedTotal;
	private int screenTotal;
	private int screenHfTotal;
	private int jjqTotal;
	private int jjqHfTotal;
	private int sxtTotal;
	private int sxtHfTotal;

	public int getCompanyId() {
		return companyId;
	}

	public void setCompanyId(int companyId) {
		this.companyId = companyId;
	}

	public String getCompanyName() {
		return companyName;
	}

	public void setCompanyName(String companyName) {
		this.companyName = companyName;
	}

	public int getOverSpeedTotal() {
		return overSpeedTotal;
	}

	public void setOverSpeedTotal(int overSpeedTotal) {
		this.overSpeedTotal = overSpeedTotal;
	}

	public int getScreenTotal() {
		return screenTotal;
	}

	public void setScreenTotal(int screenTotal) {
		this.screenTotal = screenTotal;
	}

	public int getScreenHfTotal() {
		return screenHfTotal;
	}

	public void setScreenHfTotal(int screenHfTotal) {
		this.screenHfTotal = screenHfTotal;
	}

	public int getJjqTotal() {
		return jjqTotal;
	}

	public void setJjqTotal(int jjqTotal) {
		this.jjqTotal = jjqTotal;
	}

	public int getJjqHfTotal() {
		return jjqHfTotal;
	}

	public void setJjqHfTotal(int jjqHfTotal) {
		this.jjqHfTotal = jjqHfTotal;
	}

	public int getSxtTotal() {
		return sxtTotal;
	}

	public void setSxtTotal(int sxtTotal) {
		this.sxtTotal = sxtTotal;
	}

	public int getSxtHfTotal() {
		return sxtHfTotal;
	}

	public void setSxtHfTotal(int sxtHfTotal) {
		this.sxtHfTotal = sxtHfTotal;
	}

	public String getAliasName() {
		return aliasName;
	}

	public void setAliasName(String aliasName) {
		this.aliasName = aliasName;
	}

}
