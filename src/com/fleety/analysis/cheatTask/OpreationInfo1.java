package com.fleety.analysis.cheatTask;

import java.util.Date;

public class OpreationInfo1 {

	String car_no;
	Date date_up;
	Date date_down;
	Date record_time;
	
	double sum;
	double distance;
	double gps_distance;
	double gps_points;
	
	int operate_time;//营运时长
	double area_size;//面积
	double load_points=0;//重车点数
	double empty_points=0;//空车点数
	double task_points=0;//任务车点数
	double other_points=0;//未知状态点数
	double un_fit=0;//不合格点数
	@Override
	public String toString() {
		return "OpreationInfo1 [car_no=" + car_no + ", date_up=" + date_up
				+ ", date_down=" + date_down + ", record_time=" + record_time
				+ ", sum=" + sum + ", distance=" + distance + ", gps_distance="
				+ gps_distance + ", gps_points=" + gps_points
				+ ", operate_time=" + operate_time + ", area_size=" + area_size
				+ ", load_points=" + load_points + ", empty_points="
				+ empty_points + ", task_points=" + task_points
				+ ", other_points=" + other_points + ", un_fit=" + un_fit + "]";
	}
			
}
