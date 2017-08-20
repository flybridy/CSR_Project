package com.fleety.analysis.cheatTask;

import java.util.Date;

/**
 * 标准数据数据结构类
 * @author Administrator
 *
 */
public class StandardInfo {
	//，营运时长（分钟），营运次数，最大的距离，最大的金额
	    private int car_type;//车型
		private double max_sum;//最大金额
		private double max_DISTANCE;//最大距离
		private double operator_time;//营运时长
		private Date record_date;//分析日期
		private int hour_type;//0,晚23点到24点及早0点到早6点;1,早6点到晚23点
		private double operator_nums;//营运次数
		public Date getRecord_date() {
			return record_date;
		}
		public void setRecord_date(Date record_date) {
			this.record_date = record_date;
		}
		public double getOperator_nums() {
			return operator_nums;
		}
		public void setOperator_nums(double operator_nums) {
			this.operator_nums = operator_nums;
		}
		public int getCar_type() {
			return car_type;
		}
		public void setCar_type(int car_type) {
			this.car_type = car_type;
		}
		public double getMax_sum() {
			return max_sum;
		}
		public void setMax_sum(double max_sum) {
			this.max_sum = max_sum;
		}
		public double getMax_DISTANCE() {
			return max_DISTANCE;
		}
		public void setMax_DISTANCE(double max_DISTANCE) {
			this.max_DISTANCE = max_DISTANCE;
		}
		public double getOperator_time() {
			return operator_time;
		}
		public void setOperator_time(double operator_time) {
			this.operator_time = operator_time;
		}
		public Date getRecord_time() {
			return record_date;
		}
		public void setRecord_time(Date record_time) {
			this.record_date = record_time;
		}
		public int getHour_type() {
			return hour_type;
		}
		public void setHour_type(int hour_type) {
			this.hour_type = hour_type;
		}
	    
}
