package com.fleety.analysis.track.task.key_area;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;

import com.fleety.analysis.track.DestInfo;
import com.fleety.base.InfoContainer;
import com.fleety.track.TrackIO;

public class AreaStatCarInfo 
{
	int areaId ;
	String areaName;
	int preTime = 5;//单位分钟
	ArrayList trackInfo = new ArrayList();//所有在区域里面的轨迹点
	HashMap periodMap = new HashMap();//1个小时对应一个周期统计的map
	
	public AreaStatCarInfo()
	{
		this.init();
	}
	
	private void init()
	{
		PeriodStat periodStat = null;
		for(int i = 0; i <= 23; i ++)
		{
			periodStat = new PeriodStat();
			periodStat.period = i;
			periodStat.preTime = this.preTime;
			periodMap.put(i, periodStat);
		}
	}	
	
	public void stat(DestInfo destInfo)
	{
		InfoContainer info = null;
		int curStatus = 0, lastStatus = 0, hour = 0;
		Date gpsTime = null;
		PeriodStat periodStat = null;
		
		//把所有的在区域里面的轨迹点分配到每个小时周期里面去
		Calendar calendar=Calendar.getInstance();
		for(int i = 0; i < trackInfo.size(); i ++)
		{
			info = (InfoContainer) trackInfo.get(i);
			gpsTime = info.getDate(TrackIO.DEST_TIME_FLAG);
			calendar.setTime(gpsTime);
			hour = calendar.get(Calendar.HOUR_OF_DAY);
			periodStat = (PeriodStat) this.periodMap.get(hour);
			periodStat.periodTrackInfo.add(info);
		}
		Iterator it = this.periodMap.keySet().iterator();
		while(it.hasNext())
		{
			hour = (Integer) it.next();
			periodStat = (PeriodStat) this.periodMap.get(hour);
			periodStat.stat(destInfo);
		}
		//清除该车在该区域内的所有点
		this.clear();
	}
	
	public void statAvg()
	{
		Iterator it = this.periodMap.keySet().iterator();
		PeriodStat periodStat = null;
		while(it.hasNext())
		{
			periodStat = (PeriodStat) this.periodMap.get(it.next());
			periodStat.statAvg();
		}
	}
	
	private void clear()
	{
		this.trackInfo.clear();
	}
}
