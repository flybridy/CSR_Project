package com.fleety.job.task;

public class DestLocationInfo {
	private String companyName;
	private String plateNo;
	private int mdtId;
	private double lo;
	private double la;
	private int speed;
	private int angle;
	private int status;//���س�0��1��
	private int gps;//gps��λ״̬0��λ1δ��λ
	private long gpsTime;
	private int acc = -1;//acc״̬0��1��
	
	private int state;
	private String lola;
	private String stateStr;
	private String statusStr;
	private String formateGpsTime;
	
	private int workTimes;
	private double workIncome;
	private double totalIncome;
	private double fuelIncome;
	private double totalDistance;
	
	
	public double getTotalDistance() {
		return totalDistance;
	}
	public void setTotalDistance(double totalDistance) {
		this.totalDistance = totalDistance;
	}
	public DestLocationInfo(){
		
	}
	public DestLocationInfo(int mdtId,double lo,double la,int speed,int angle,int status,int gps,long gpsTime){
		this.mdtId = mdtId;
		this.lo = lo;
		this.la = la;
		this.speed = speed;
		this.angle = angle;
		this.status = status;
		this.gps = gps;
		this.gpsTime = gpsTime;
	}
	public DestLocationInfo(int mdtId,double lo,double la,int speed,int angle,int status,int acc,int gps,long gpsTime){
		this.mdtId = mdtId;
		this.lo = lo;
		this.la = la;
		this.speed = speed;
		this.angle = angle;
		this.status = status;
		this.acc = acc;
		this.gps = gps;
		this.gpsTime = gpsTime;
	}
	public int getMdtId() {
		return mdtId;
	}
	public void setMdtId(int mdtId) {
		this.mdtId = mdtId;
	}
	public double getLo() {
		return lo;
	}
	public void setLo(double lo) {
		this.lo = lo;
	}
	public double getLa() {
		return la;
	}
	public void setLa(double la) {
		this.la = la;
	}
	public int getSpeed() {
		return speed;
	}
	public void setSpeed(int speed) {
		this.speed = speed;
	}
	public int getAngle() {
		return angle;
	}
	public void setAngle(int angle) {
		this.angle = angle;
	}
	public int getStatus() {
		return status;
	}
	public void setStatus(int status) {
		this.status = status;
	}
	public int getGps() {
		return gps;
	}
	public void setGps(int gps) {
		this.gps = gps;
	}
	public long getGpsTime() {
		return gpsTime;
	}
	public void setGpsTime(long gpsTime) {
		this.gpsTime = gpsTime;
	}
	public int getAcc() {
		return acc;
	}
	public void setAcc(int acc) {
		this.acc = acc;
	}
	public String getLola() {
		return lola;
	}
	public void setLola(String lola) {
		this.lola = lola;
	}
	public String getStateStr() {
		return stateStr;
	}
	public void setStateStr(String stateStr) {
		this.stateStr = stateStr;
	}
	public String getPlateNo() {
		return plateNo;
	}
	public void setPlateNo(String plateNo) {
		this.plateNo = plateNo;
	}
	public int getState() {
		return state;
	}
	public void setState(int state) {
		this.state = state;
	}
	public String getStatusStr() {
		return statusStr;
	}
	public void setStatusStr(String statusStr) {
		this.statusStr = statusStr;
	}
	public String getFormateGpsTime() {
		return formateGpsTime;
	}
	public void setFormateGpsTime(String formateGpsTime) {
		this.formateGpsTime = formateGpsTime;
	}
	public int getWorkTimes() {
		return workTimes;
	}
	public void setWorkTimes(int workTimes) {
		this.workTimes = workTimes;
	}
	public double getWorkIncome() {
		return workIncome;
	}
	public void setWorkIncome(double workIncome) {
		this.workIncome = workIncome;
	}
	public double getTotalIncome() {
		return totalIncome;
	}
	public void setTotalIncome(double totalIncome) {
		this.totalIncome = totalIncome;
	}
	public double getFuelIncome() {
		return fuelIncome;
	}
	public void setFuelIncome(double fuelIncome) {
		this.fuelIncome = fuelIncome;
	}
	public String getCompanyName() {
		return companyName;
	}
	public void setCompanyName(String companyName) {
		this.companyName = companyName;
	}
}
