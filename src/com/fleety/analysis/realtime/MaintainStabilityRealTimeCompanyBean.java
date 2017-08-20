package com.fleety.analysis.realtime;

import com.fleety.util.pool.db.redis.RedisTableBean;

public class MaintainStabilityRealTimeCompanyBean extends RedisTableBean{
  private int company_id=0;//公司id 
  private int stopcarnum = 0;         //当前停驶车辆数
  private double businessnum = 0;      //当前5分钟在线车辆平均营运次数
  private double businessdistance = 0; //当前5分钟在线车辆平均营运距离
  private double businessmoney = 0;    //当前5分钟在线车辆平均营运金额
  private double speed = 0;            //当前5分钟平均速度
  private int stopcarnum_red = 0;         //当前停驶车辆数(红的)
  private double businessnum_red = 0;      //当前5分钟在线车辆平均营运次数(红的)
  private double businessdistance_red = 0; //当前5分钟在线车辆平均营运距离(红的)
  private double businessmoney_red = 0;    //当前5分钟在线车辆平均营运金额(红的)
  private double speed_red = 0;            //当前5分钟平均速度(红的)
  private int stopcarnum_green = 0;         //当前停驶车辆数(绿的)
  private double businessnum_green = 0;      //当前5分钟在线车辆平均营运次数(绿的)
  private double businessdistance_green = 0; //当前5分钟在线车辆平均营运距离(绿的)
  private double businessmoney_green = 0;    //当前5分钟在线车辆平均营运金额(绿的)
  private double speed_green = 0;            //当前5分钟平均速度(绿的)
  private int stopcarnum_electric = 0;         //当前停驶车辆数(电动)
  private double businessnum_electric = 0;      //当前5分钟在线车辆平均营运次数(电动)
  private double businessdistance_electric = 0; //当前5分钟在线车辆平均营运距离(电动)
  private double businessmoney_electric = 0;    //当前5分钟在线车辆平均营运金额(电动)
  private double speed_electric = 0;            //当前5分钟平均速度(电动)
  private int stopcarnum_accessible = 0;         //当前停驶车辆数(无障碍)
  private double businessnum_accessible = 0;      //当前5分钟在线车辆平均营运次数(无障碍)
  private double businessdistance_accessible = 0; //当前5分钟在线车辆平均营运距离(无障碍)
  private double businessmoney_accessible = 0;    //当前5分钟在线车辆平均营运金额(无障碍)
  private double speed_accessible = 0;            //当前5分钟平均速度(无障碍)

  public int getStopcarnum() {
	return stopcarnum;
}
public int getCompany_id() {
	return company_id;
}
public void setCompany_id(int company_id) {
	this.company_id = company_id;
}
public void setStopcarnum(int stopcarnum) {
	this.stopcarnum = stopcarnum;
}
public double getBusinessnum() {
	return businessnum;
}
public void setBusinessnum(double businessnum) {
	this.businessnum = businessnum;
}
public double getBusinessdistance() {
	return businessdistance;
}
public void setBusinessdistance(double businessdistance) {
	this.businessdistance = businessdistance;
}
public double getBusinessmoney() {
	return businessmoney;
}
public void setBusinessmoney(double businessmoney) {
	this.businessmoney = businessmoney;
}
public double getSpeed() {
	return speed;
}
public void setSpeed(double speed) {
	this.speed = speed;
}
public int getStopcarnum_red() {
	return stopcarnum_red;
}
public void setStopcarnum_red(int stopcarnum_red) {
	this.stopcarnum_red = stopcarnum_red;
}
public double getBusinessnum_red() {
	return businessnum_red;
}
public void setBusinessnum_red(double businessnum_red) {
	this.businessnum_red = businessnum_red;
}
public double getBusinessdistance_red() {
	return businessdistance_red;
}
public void setBusinessdistance_red(double businessdistance_red) {
	this.businessdistance_red = businessdistance_red;
}
public double getBusinessmoney_red() {
	return businessmoney_red;
}
public void setBusinessmoney_red(double businessmoney_red) {
	this.businessmoney_red = businessmoney_red;
}
public double getSpeed_red() {
	return speed_red;
}
public void setSpeed_red(double speed_red) {
	this.speed_red = speed_red;
}
public int getStopcarnum_green() {
	return stopcarnum_green;
}
public void setStopcarnum_green(int stopcarnum_green) {
	this.stopcarnum_green = stopcarnum_green;
}
public double getBusinessnum_green() {
	return businessnum_green;
}
public void setBusinessnum_green(double businessnum_green) {
	this.businessnum_green = businessnum_green;
}
public double getBusinessdistance_green() {
	return businessdistance_green;
}
public void setBusinessdistance_green(double businessdistance_green) {
	this.businessdistance_green = businessdistance_green;
}
public double getBusinessmoney_green() {
	return businessmoney_green;
}
public void setBusinessmoney_green(double businessmoney_green) {
	this.businessmoney_green = businessmoney_green;
}
public double getSpeed_green() {
	return speed_green;
}
public void setSpeed_green(double speed_green) {
	this.speed_green = speed_green;
}
public int getStopcarnum_electric() {
	return stopcarnum_electric;
}
public void setStopcarnum_electric(int stopcarnum_electric) {
	this.stopcarnum_electric = stopcarnum_electric;
}
public double getBusinessnum_electric() {
	return businessnum_electric;
}
public void setBusinessnum_electric(double businessnum_electric) {
	this.businessnum_electric = businessnum_electric;
}
public double getBusinessdistance_electric() {
	return businessdistance_electric;
}
public void setBusinessdistance_electric(double businessdistance_electric) {
	this.businessdistance_electric = businessdistance_electric;
}
public double getBusinessmoney_electric() {
	return businessmoney_electric;
}
public void setBusinessmoney_electric(double businessmoney_electric) {
	this.businessmoney_electric = businessmoney_electric;
}
public double getSpeed_electric() {
	return speed_electric;
}
public void setSpeed_electric(double speed_electric) {
	this.speed_electric = speed_electric;
}
public int getStopcarnum_accessible() {
	return stopcarnum_accessible;
}
public void setStopcarnum_accessible(int stopcarnum_accessible) {
	this.stopcarnum_accessible = stopcarnum_accessible;
}
public double getBusinessnum_accessible() {
	return businessnum_accessible;
}
public void setBusinessnum_accessible(double businessnum_accessible) {
	this.businessnum_accessible = businessnum_accessible;
}
public double getBusinessdistance_accessible() {
	return businessdistance_accessible;
}
public void setBusinessdistance_accessible(double businessdistance_accessible) {
	this.businessdistance_accessible = businessdistance_accessible;
}
public double getBusinessmoney_accessible() {
	return businessmoney_accessible;
}
public void setBusinessmoney_accessible(double businessmoney_accessible) {
	this.businessmoney_accessible = businessmoney_accessible;
}
public double getSpeed_accessible() {
	return speed_accessible;
}
public void setSpeed_accessible(double speed_accessible) {
	this.speed_accessible = speed_accessible;
}

  
}
