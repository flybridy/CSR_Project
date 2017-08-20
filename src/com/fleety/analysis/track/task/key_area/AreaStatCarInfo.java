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
	int preTime = 5;//��λ����
	ArrayList trackInfo = new ArrayList();//��������������Ĺ켣��
	HashMap periodMap = new HashMap();//1��Сʱ��Ӧһ������ͳ�Ƶ�map
	
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
		
		//�����е�����������Ĺ켣����䵽ÿ��Сʱ��������ȥ
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
		//����ó��ڸ������ڵ����е�
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
