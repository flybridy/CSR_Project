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
	
	int operate_time;//Ӫ��ʱ��
	double area_size;//���
	double load_points=0;//�س�����
	double empty_points=0;//�ճ�����
	double task_points=0;//���񳵵���
	double other_points=0;//δ֪״̬����
	double un_fit=0;//���ϸ����
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
