package com.fleety.analysis.realtime;

import com.fleety.util.pool.db.redis.RedisTableBean;

public class GradeRealTimeBean extends RedisTableBean {
	private int comId;

	private int goodNum;

	private int veryGoodNum;

	private int badNum;

	private int unknownNum;

	public int getGoodNum() {
		return goodNum;
	}

	public void setGoodNum(int goodNum) {
		this.goodNum = goodNum;
	}

	public int getVeryGoodNum() {
		return veryGoodNum;
	}

	public void setVeryGoodNum(int veryGoodNum) {
		this.veryGoodNum = veryGoodNum;
	}

	public int getBadNum() {
		return badNum;
	}

	public void setBadNum(int badNum) {
		this.badNum = badNum;
	}

	public int getUnknownNum() {
		return unknownNum;
	}

	public void setUnknownNum(int unknownNum) {
		this.unknownNum = unknownNum;
	}

	public int getComId() {
		return comId;
	}

	public void setComId(int comId) {
		this.comId = comId;
	}

}
