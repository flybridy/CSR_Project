package com.fleety.analysis.track.task.overspeed_area;


import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

import com.fleety.base.InfoContainer;
import com.fleety.server.area.AreaInfo;
import com.fleety.track.TrackIO;

public class OverSpeedSchemeInfo 
{
	private int scheme_id;
	private String scheme_name;
	
	private String start_time;  //00-24
	private String end_time;
	private AreaInfo overSpeedAreaInfo;
	
	private double overSpeed;
	private int isUsed ; //0:Í£ÓÃ£»1£ºÆôÓÃ
	private int car_status;
	
	public HashMap statMap=new HashMap();


	public int getScheme_id() {
		return scheme_id;
	}

	public void setScheme_id(int scheme_id) {
		this.scheme_id = scheme_id;
	}

	public String getScheme_name() {
		return scheme_name;
	}

	public void setScheme_name(String scheme_name) {
		this.scheme_name = scheme_name;
	}

	public String getStart_time() {
		return start_time;
	}

	public void setStart_time(String start_time) {
		this.start_time = start_time;
	}

	public String getEnd_time() {
		return end_time;
	}

	public void setEnd_time(String end_time) {
		this.end_time = end_time;
	}

	


	public AreaInfo getOverSpeedAreaInfo() {
		return overSpeedAreaInfo;
	}

	public void setOverSpeedAreaInfo(AreaInfo overSpeedAreaInfo) {
		this.overSpeedAreaInfo = overSpeedAreaInfo;
	}

	public double getOverSpeed() {
		return overSpeed;
	}

	public void setOverSpeed(double overSpeed) {
		this.overSpeed = overSpeed;
	}

	public int getIsUsed() {
		return isUsed;
	}

	public void setIsUsed(int isUsed) {
		this.isUsed = isUsed;
	}

	public HashMap getStatMap() {
		return statMap;
	}

	public void setStatMap(HashMap statMap) {
		this.statMap = statMap;
	}

	public int getCar_status() {
		return car_status;
	}

	public void setCar_status(int car_status) {
		this.car_status = car_status;
	}

	public boolean isInScheme(InfoContainer info) {
		
		double lo = info.getDouble(TrackIO.DEST_LO_FLAG);
		double la = info.getDouble(TrackIO.DEST_LA_FLAG);
		if(!this.overSpeedAreaInfo.contains(lo, la)){
			return false;
		}
		
		Date gpsTime = info.getDate(TrackIO.DEST_TIME_FLAG);
		Calendar calendar=Calendar.getInstance();
		calendar.setTime(gpsTime);
		int hour = calendar.get(Calendar.HOUR_OF_DAY);
		int start = Integer.parseInt(start_time);
		int end = Integer.parseInt(end_time);
		if(hour>=start && hour<end){
				
		}else{
			return false;
		}
//		int speed = info.getInteger(TrackIO.DEST_SPEED_FLAG).intValue();
//		if(speed<=this.overSpeed){
//			return false;
//		}
		return true;
	}

}
