package com.fleety.analysis.cheatTask;

import java.util.Date;

/**
 * ��׼�������ݽṹ��
 * @author Administrator
 *
 */
public class StandardInfo {
	//��Ӫ��ʱ�������ӣ���Ӫ�˴��������ľ��룬���Ľ��
	    private int car_type;//����
		private double max_sum;//�����
		private double max_DISTANCE;//������
		private double operator_time;//Ӫ��ʱ��
		private Date record_date;//��������
		private int hour_type;//0,��23�㵽24�㼰��0�㵽��6��;1,��6�㵽��23��
		private double operator_nums;//Ӫ�˴���
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
