package com.fleety.analysis.track.task.capacity_area;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;

import com.fleety.base.InfoContainer;
import com.fleety.server.area.AreaInfo;
import com.fleety.track.TrackIO;

public class CapacitySchemeInfo {
	private int scheme_id;
	private String scheme_name;

	private AreaInfo areaInfo;
	private int order_num;
	private HashSet<String> destSet;

	private int status; // 0:Õ£”√£ª1£∫∆Ù”√
	private String start_time; // 00-23
	private String end_time; // 01-24

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

	public AreaInfo getAreaInfo() {
		return areaInfo;
	}

	public void setAreaInfo(AreaInfo areaInfo) {
		this.areaInfo = areaInfo;
	}

	public int getOrder_num() {
		return order_num;
	}

	public void setOrder_num(int order_num) {
		this.order_num = order_num;
	}

	public HashSet<String> getDestSet() {
		return destSet;
	}

	public void setDestSet(HashSet<String> destSet) {
		this.destSet = destSet;
	}

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
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

	public boolean isTrackInScheme(InfoContainer info) {

		double lo = info.getDouble(TrackIO.DEST_LO_FLAG);
		double la = info.getDouble(TrackIO.DEST_LA_FLAG);
		if (!this.areaInfo.contains(lo, la)) {
			return false;
		}

		Date gpsTime = info.getDate(TrackIO.DEST_TIME_FLAG);
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(gpsTime);
		int hour = calendar.get(Calendar.HOUR_OF_DAY);
		int start = Integer.parseInt(start_time);
		int end = Integer.parseInt(end_time);
		if (hour >= start && hour < end) {

		} else {
			return false;
		}
		return true;
	}

}
