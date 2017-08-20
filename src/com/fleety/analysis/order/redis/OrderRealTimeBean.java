package com.fleety.analysis.order.redis;

import com.fleety.util.pool.db.redis.RedisTableBean;

public class OrderRealTimeBean extends RedisTableBean {
	private int ivrTotalNum = 0;
	private int ivrSuccessNum = 0;
	private int ivrWugongNum = 0;
	private int ivrCancelNum = 0;
	private int ivrFangkongNum = 0;
	private int ivrYuyueNum = 0;

	private int mobileTotalNum = 0;
	private int mobileSuccessNum = 0;
	private int mobileWugongNum = 0;
	private int mobileCancelNum = 0;
	private int mobileFangkongNum = 0;
	private int mobileYuyueNum = 0;

	private int qqTotalNum = 0;
	private int qqSuccessNum = 0;
	private int qqWugongNum = 0;
	private int qqCancelNum = 0;
	private int qqFangkongNum = 0;
	private int qqYuyueNum = 0;

	private int wxTotalNum = 0;
	private int wxSuccessNum = 0;
	private int wxWugongNum = 0;
	private int wxCancelNum = 0;
	private int wxFangkongNum = 0;
	private int wxYuyueNum = 0;

	private int netTotalNum = 0;
	private int netSuccessNum = 0;
	private int netWugongNum = 0;
	private int netCancelNum = 0;
	private int netFangkongNum = 0;
	private int netYuyueNum = 0;
	
	public int getIvrTotalNum() {
		return ivrTotalNum;
	}

	public void setIvrTotalNum(int ivrTotalNum) {
		this.ivrTotalNum = ivrTotalNum;
	}

	public int getIvrSuccessNum() {
		return ivrSuccessNum;
	}

	public void setIvrSuccessNum(int ivrSuccessNum) {
		this.ivrSuccessNum = ivrSuccessNum;
	}

	public int getIvrWugongNum() {
		return ivrWugongNum;
	}

	public void setIvrWugongNum(int ivrWugongNum) {
		this.ivrWugongNum = ivrWugongNum;
	}

	public int getIvrCancelNum() {
		return ivrCancelNum;
	}

	public void setIvrCancelNum(int ivrCancelNum) {
		this.ivrCancelNum = ivrCancelNum;
	}

	public int getIvrFangkongNum() {
		return ivrFangkongNum;
	}

	public void setIvrFangkongNum(int ivrFangkongNum) {
		this.ivrFangkongNum = ivrFangkongNum;
	}

	public int getMobileTotalNum() {
		return mobileTotalNum;
	}

	public void setMobileTotalNum(int mobileTotalNum) {
		this.mobileTotalNum = mobileTotalNum;
	}

	public int getMobileSuccessNum() {
		return mobileSuccessNum;
	}

	public void setMobileSuccessNum(int mobileSuccessNum) {
		this.mobileSuccessNum = mobileSuccessNum;
	}

	public int getMobileWugongNum() {
		return mobileWugongNum;
	}

	public void setMobileWugongNum(int mobileWugongNum) {
		this.mobileWugongNum = mobileWugongNum;
	}

	public int getMobileCancelNum() {
		return mobileCancelNum;
	}

	public void setMobileCancelNum(int mobileCancelNum) {
		this.mobileCancelNum = mobileCancelNum;
	}

	public int getMobileFangkongNum() {
		return mobileFangkongNum;
	}

	public void setMobileFangkongNum(int mobileFangkongNum) {
		this.mobileFangkongNum = mobileFangkongNum;
	}

	public int getQqTotalNum() {
		return qqTotalNum;
	}

	public void setQqTotalNum(int qqTotalNum) {
		this.qqTotalNum = qqTotalNum;
	}

	public int getQqSuccessNum() {
		return qqSuccessNum;
	}

	public void setQqSuccessNum(int qqSuccessNum) {
		this.qqSuccessNum = qqSuccessNum;
	}

	public int getQqWugongNum() {
		return qqWugongNum;
	}

	public void setQqWugongNum(int qqWugongNum) {
		this.qqWugongNum = qqWugongNum;
	}

	public int getQqCancelNum() {
		return qqCancelNum;
	}

	public void setQqCancelNum(int qqCancelNum) {
		this.qqCancelNum = qqCancelNum;
	}

	public int getQqFangkongNum() {
		return qqFangkongNum;
	}

	public void setQqFangkongNum(int qqFangkongNum) {
		this.qqFangkongNum = qqFangkongNum;
	}

	public int getWxTotalNum() {
		return wxTotalNum;
	}

	public void setWxTotalNum(int wxTotalNum) {
		this.wxTotalNum = wxTotalNum;
	}

	public int getWxSuccessNum() {
		return wxSuccessNum;
	}

	public void setWxSuccessNum(int wxSuccessNum) {
		this.wxSuccessNum = wxSuccessNum;
	}

	public int getWxWugongNum() {
		return wxWugongNum;
	}

	public void setWxWugongNum(int wxWugongNum) {
		this.wxWugongNum = wxWugongNum;
	}

	public int getWxCancelNum() {
		return wxCancelNum;
	}

	public void setWxCancelNum(int wxCancelNum) {
		this.wxCancelNum = wxCancelNum;
	}

	public int getWxFangkongNum() {
		return wxFangkongNum;
	}

	public void setWxFangkongNum(int wxFangkongNum) {
		this.wxFangkongNum = wxFangkongNum;
	}

	public int getNetTotalNum() {
		return netTotalNum;
	}

	public void setNetTotalNum(int netTotalNum) {
		this.netTotalNum = netTotalNum;
	}

	public int getNetSuccessNum() {
		return netSuccessNum;
	}

	public void setNetSuccessNum(int netSuccessNum) {
		this.netSuccessNum = netSuccessNum;
	}

	public int getNetWugongNum() {
		return netWugongNum;
	}

	public void setNetWugongNum(int netWugongNum) {
		this.netWugongNum = netWugongNum;
	}

	public int getNetCancelNum() {
		return netCancelNum;
	}

	public void setNetCancelNum(int netCancelNum) {
		this.netCancelNum = netCancelNum;
	}

	public int getNetFangkongNum() {
		return netFangkongNum;
	}

	public void setNetFangkongNum(int netFangkongNum) {
		this.netFangkongNum = netFangkongNum;
	}

	public int getIvrYuyueNum() {
		return ivrYuyueNum;
	}

	public void setIvrYuyueNum(int ivrYuyueNum) {
		this.ivrYuyueNum = ivrYuyueNum;
	}

	public int getMobileYuyueNum() {
		return mobileYuyueNum;
	}

	public void setMobileYuyueNum(int mobileYuyueNum) {
		this.mobileYuyueNum = mobileYuyueNum;
	}

	public int getQqYuyueNum() {
		return qqYuyueNum;
	}

	public void setQqYuyueNum(int qqYuyueNum) {
		this.qqYuyueNum = qqYuyueNum;
	}

	public int getWxYuyueNum() {
		return wxYuyueNum;
	}

	public void setWxYuyueNum(int wxYuyueNum) {
		this.wxYuyueNum = wxYuyueNum;
	}

	public int getNetYuyueNum() {
		return netYuyueNum;
	}

	public void setNetYuyueNum(int netYuyueNum) {
		this.netYuyueNum = netYuyueNum;
	}
}
