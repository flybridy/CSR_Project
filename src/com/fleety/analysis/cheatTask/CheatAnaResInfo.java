package com.fleety.analysis.cheatTask;

import java.util.Date;
/**
 * 分析结果数据结构类
 * @author Administrator
 *
 */
public class CheatAnaResInfo {	
		private long op_id;//营运数据ID  
		private String car_no;//牌号车
		private int car_type;//车型
		private String date_up;//上车时间
		private String date_down;//上车时间
		private double DISTANCE;//计价器距离
		private double sum;//计价器金额
		private double operator_time;//营运时长
		private int hour_type;//0,晚23点到24点及早0点到早6点;1,早6点到晚23点
		
		private double max__distance;//标准计价器距离
		private double max__sum;//标准计价器距离
		private double operator_nums;//采样次数	
		
		private int is_fit;//0,不合格;1,合格;
		public long getOp_id() {
			return op_id;
		}
		public void setOp_id(long op_id) {
			this.op_id = op_id;
		}
		
		public String getCar_no() {
			return car_no;
		}
		public void setCar_no(String car_no) {
			this.car_no = car_no;
		}
		public int getCar_type() {
			return car_type;
		}
		public void setCar_type(int car_type) {
			this.car_type = car_type;
		}
		
		public String getDate_up() {
			return date_up;
		}
		public void setDate_up(String date_up) {
			this.date_up = date_up;
		}
		public String getDate_down() {
			return date_down;
		}
		public void setDate_down(String date_down) {
			this.date_down = date_down;
		}
		public double getDISTANCE() {
			return DISTANCE;
		}
		public void setDISTANCE(double dISTANCE) {
			DISTANCE = dISTANCE;
		}
		public double getSum() {
			return sum;
		}
		public void setSum(double sum) {
			this.sum = sum;
		}
		public double getMax__distance() {
			return max__distance;
		}
		public void setMax__distance(double max__distance) {
			this.max__distance = max__distance;
		}
		public double getMax__sum() {
			return max__sum;
		}
		public void setMax__sum(double max__sum) {
			this.max__sum = max__sum;
		}
		public double getOperator_time() {
			return operator_time;
		}
		public void setOperator_time(double operator_time) {
			this.operator_time = operator_time;
		}
		public double getOperator_nums() {
			return operator_nums;
		}
		public void setOperator_nums(double operator_nums) {
			this.operator_nums = operator_nums;
		}
		public int getHour_type() {
			return hour_type;
		}
		public void setHour_type(int hour_type) {
			this.hour_type = hour_type;
		}
		public int getIs_fit() {
			return is_fit;
		}
		public void setIs_fit(int is_fit) {
			this.is_fit = is_fit;
		}
	
}
