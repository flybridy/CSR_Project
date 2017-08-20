package com.fleety.analysis.realtime;

import com.fleety.util.pool.db.redis.RedisTableBean;

public class CompanyOverRateRealTimeBean extends RedisTableBean {
	private int company_id = 0;// 企业id
	private long overload_num_red = 0;// 红的重车数
	private long empty_num_red = 0;// 红的空车数
	private long task_num_red = 0;// 红的任务车数
	private long other_num_red = 0;// 红的未知状态数
	private long overload_num_green = 0;// 绿的重车数
	private long empty_num_green = 0;// 绿的空车数
	private long task_num_green = 0;// 绿的任务车数
	private long other_num_green = 0;// 绿的未知状态数
	private long overload_num_electric = 0;// 电动重车数
	private long empty_num_electric = 0;// 电动空车数
	private long task_num_electric = 0;// 电动任务车数
	private long other_num_electric = 0;// 电动未知状态数
	private long overload_num_accessible = 0;// 无障碍重车数
	private long empty_num_accessible = 0;// 无障碍空车数
	private long task_num_accessible = 0;// 无障碍任务车数
	private long other_num_accessible = 0;// 无障碍未知状态数
	private long total_num = 0;// 总车数
	private int index = 0;// 当前信息一小时实时数据的次序（0-11）

	public int getCompany_id() {
		return company_id;
	}

	public void setCompany_id(int company_id) {
		this.company_id = company_id;
	}

	public long getOverload_num_red() {
		return overload_num_red;
	}

	public void setOverload_num_red(long overload_num_red) {
		this.overload_num_red = overload_num_red;
	}

	public long getEmpty_num_red() {
		return empty_num_red;
	}

	public void setEmpty_num_red(long empty_num_red) {
		this.empty_num_red = empty_num_red;
	}

	public long getTask_num_red() {
		return task_num_red;
	}

	public void setTask_num_red(long task_num_red) {
		this.task_num_red = task_num_red;
	}

	public long getOther_num_red() {
		return other_num_red;
	}

	public void setOther_num_red(long other_num_red) {
		this.other_num_red = other_num_red;
	}

	public long getOverload_num_green() {
		return overload_num_green;
	}

	public void setOverload_num_green(long overload_num_green) {
		this.overload_num_green = overload_num_green;
	}

	public long getEmpty_num_green() {
		return empty_num_green;
	}

	public void setEmpty_num_green(long empty_num_green) {
		this.empty_num_green = empty_num_green;
	}

	public long getTask_num_green() {
		return task_num_green;
	}

	public void setTask_num_green(long task_num_green) {
		this.task_num_green = task_num_green;
	}

	public long getOther_num_green() {
		return other_num_green;
	}

	public void setOther_num_green(long other_num_green) {
		this.other_num_green = other_num_green;
	}

	public long getOverload_num_electric() {
		return overload_num_electric;
	}

	public void setOverload_num_electric(long overload_num_electric) {
		this.overload_num_electric = overload_num_electric;
	}

	public long getEmpty_num_electric() {
		return empty_num_electric;
	}

	public void setEmpty_num_electric(long empty_num_electric) {
		this.empty_num_electric = empty_num_electric;
	}

	public long getTask_num_electric() {
		return task_num_electric;
	}

	public void setTask_num_electric(long task_num_electric) {
		this.task_num_electric = task_num_electric;
	}

	public long getOther_num_electric() {
		return other_num_electric;
	}

	public void setOther_num_electric(long other_num_electric) {
		this.other_num_electric = other_num_electric;
	}

	public long getOverload_num_accessible() {
		return overload_num_accessible;
	}

	public void setOverload_num_accessible(long overload_num_accessible) {
		this.overload_num_accessible = overload_num_accessible;
	}

	public long getEmpty_num_accessible() {
		return empty_num_accessible;
	}

	public void setEmpty_num_accessible(long empty_num_accessible) {
		this.empty_num_accessible = empty_num_accessible;
	}

	public long getTask_num_accessible() {
		return task_num_accessible;
	}

	public void setTask_num_accessible(long task_num_accessible) {
		this.task_num_accessible = task_num_accessible;
	}

	public long getOther_num_accessible() {
		return other_num_accessible;
	}

	public void setOther_num_accessible(long other_num_accessible) {
		this.other_num_accessible = other_num_accessible;
	}

	public long getTotal_num() {
		return total_num;
	}

	public void setTotal_num(long total_num) {
		this.total_num = total_num;
	}

	public int getIndex() {
		return index;
	}

	public void setIndex(int index) {
		this.index = index;
	}

}
