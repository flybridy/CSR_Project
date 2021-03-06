package com.fleety.analysis.track.task.nostop_area;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;

import server.var.VarManageServer;

import com.fleety.analysis.track.DestInfo;
import com.fleety.base.InfoContainer;
import com.fleety.base.StrFilter;
import com.fleety.track.TrackIO;

public class SchemeStatCarInfo 
{
	int schemeId ;
	String schemeName;
	int preTime = 5;//单位分钟
	ArrayList trackInfo = new ArrayList();//所有在方案里面的轨迹点
	HashMap periodMap = new HashMap();//key是自增的,每一小段超速对象 
	int comId;
	String comName="";
	String dest_no ="";
	
	int minSpeed = 0;
	
	int level1_time=0;
	int level2_time=0 ;
	int level3_time=0;
	
	public SchemeStatCarInfo()
	{
		super();
	}
	
	public void stat(DestInfo destInfo)
	{
	
		Calendar calendar=Calendar.getInstance();
		
		// 对该时段的点进行分类，连续的两个位置汇报间隔超过5分钟认定为两次进入
		int count = periodMap.size();  //一段超速一个value
		ArrayList tempList = new ArrayList();
		HashMap tempMap = new HashMap();
		int keyAreaIntevalSecond = 300;
		String temp = VarManageServer.getSingleInstance().getVarStringValue(
				"nostop_area_inteval_second");
		if (StrFilter.hasValue(temp)) {
			try {
				keyAreaIntevalSecond = Integer.parseInt(temp);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		int keyAreaMinSecond = 60;
		temp = VarManageServer.getSingleInstance().getVarStringValue(
				"key_area_min_second");
		if (StrFilter.hasValue(temp)) {
			try {
				keyAreaMinSecond = Integer.parseInt(temp);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		// 对区域内的点进行分段，5分钟以内的算是一段
		int curStatus = 0, lastStatus = 0, hour = 0;
		PeriodStat periodStat = null;
		
		InfoContainer info = null;
		InfoContainer preInfo = null;
		InfoContainer maxInfo = null;
		InfoContainer minInfo = null;
		int firseSpeed = 0;
		int lastSpeed = 0;
		int maxSpeed = 0;
		int minSpeed = 0 ;
		int tempSpeed = 0;
		int curSpeed = 0;
		
		InfoContainer firstInfo = null, lastInfo = null;
		Date gpsTime = null, preGpsTime = null;

		boolean isFirst = true;
		long totalTimes = 0l;
		for(int i = 0; i < trackInfo.size(); i++)
		{
			
			info = (InfoContainer) trackInfo.get(i);
			curSpeed=info.getInteger(TrackIO.DEST_SPEED_FLAG);
			if(isFirst){
				if(curSpeed>this.minSpeed){
					continue;
				}
			}

			if(isFirst){
				preInfo = info;
				periodStat = new PeriodStat();
				periodStat.periodTrackInfo.add(preInfo);
				periodMap.put(periodMap.size(), periodStat);
				if(i==(trackInfo.size()-1)){ //最后一个点，分析
//					periodStat.stat(destInfo);
				}
				isFirst=false;
				continue;
			}else{
				
				curSpeed=info.getInteger(TrackIO.DEST_SPEED_FLAG);
				if(curSpeed>this.minSpeed){
					
					if(totalTimes>=this.level1_time*60*1000){
						periodStat.stat(destInfo,this.level2_time,this.level3_time);//把之前的分析了
					}else{
						periodMap.remove(periodMap.size()-1);
					}
					isFirst=true;  //重新寻找第一个点
					totalTimes=0;
					continue;
				}else{
					gpsTime = info.getDate(TrackIO.DEST_TIME_FLAG);
					preGpsTime = preInfo.getDate(TrackIO.DEST_TIME_FLAG);
					if (gpsTime.getTime() - preGpsTime.getTime() < keyAreaIntevalSecond * 1000) {
						periodStat.periodTrackInfo.add(info);
						totalTimes=totalTimes+(gpsTime.getTime() - preGpsTime.getTime());
						preInfo=info;
						if(i==(trackInfo.size()-1)){ //最后一个点，分析
							if(totalTimes>=this.level1_time*60*1000){
								periodStat.stat(destInfo,this.level2_time,this.level3_time);
							}else{
								periodMap.remove(periodMap.size()-1);
								isFirst=true;  //重新寻找第一个点
								totalTimes=0;
							}
							
						}
						
					} else {
						
						//重新一次进入,把前一段分析掉
						if(totalTimes>=this.level1_time*60*1000){
							periodStat.stat(destInfo,this.level2_time,this.level3_time);
						}else{
							periodMap.remove(periodMap.size()-1);
						}
						preInfo = info; 
						periodStat = new PeriodStat();
						periodStat.periodTrackInfo.add(info);
						
						if(i==(trackInfo.size()-1)){ //最后一个点，直接忽略
//							periodStat.stat(destInfo);
						}else{
							periodMap.put(periodMap.size(), periodStat);
						}
						isFirst=false;
						totalTimes=0l;
						
					}
				}
			}
		}
		
		//清除该车在该方案的所有点
		this.clear();
	}
	
	public void statAvg()
	{
		Iterator it = this.periodMap.keySet().iterator();
		PeriodStat periodStat = null;
		while(it.hasNext())
		{
			periodStat = (PeriodStat) this.periodMap.get(it.next());
//			periodStat.statAvg();
		}
	}
	
	private void clear()
	{
		this.trackInfo.clear();
	}
}
