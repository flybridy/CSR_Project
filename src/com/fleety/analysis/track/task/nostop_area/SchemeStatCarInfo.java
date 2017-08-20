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
	int preTime = 5;//��λ����
	ArrayList trackInfo = new ArrayList();//�����ڷ�������Ĺ켣��
	HashMap periodMap = new HashMap();//key��������,ÿһС�γ��ٶ��� 
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
		
		// �Ը�ʱ�εĵ���з��࣬����������λ�û㱨�������5�����϶�Ϊ���ν���
		int count = periodMap.size();  //һ�γ���һ��value
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

		// �������ڵĵ���зֶΣ�5�������ڵ�����һ��
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
				if(i==(trackInfo.size()-1)){ //���һ���㣬����
//					periodStat.stat(destInfo);
				}
				isFirst=false;
				continue;
			}else{
				
				curSpeed=info.getInteger(TrackIO.DEST_SPEED_FLAG);
				if(curSpeed>this.minSpeed){
					
					if(totalTimes>=this.level1_time*60*1000){
						periodStat.stat(destInfo,this.level2_time,this.level3_time);//��֮ǰ�ķ�����
					}else{
						periodMap.remove(periodMap.size()-1);
					}
					isFirst=true;  //����Ѱ�ҵ�һ����
					totalTimes=0;
					continue;
				}else{
					gpsTime = info.getDate(TrackIO.DEST_TIME_FLAG);
					preGpsTime = preInfo.getDate(TrackIO.DEST_TIME_FLAG);
					if (gpsTime.getTime() - preGpsTime.getTime() < keyAreaIntevalSecond * 1000) {
						periodStat.periodTrackInfo.add(info);
						totalTimes=totalTimes+(gpsTime.getTime() - preGpsTime.getTime());
						preInfo=info;
						if(i==(trackInfo.size()-1)){ //���һ���㣬����
							if(totalTimes>=this.level1_time*60*1000){
								periodStat.stat(destInfo,this.level2_time,this.level3_time);
							}else{
								periodMap.remove(periodMap.size()-1);
								isFirst=true;  //����Ѱ�ҵ�һ����
								totalTimes=0;
							}
							
						}
						
					} else {
						
						//����һ�ν���,��ǰһ�η�����
						if(totalTimes>=this.level1_time*60*1000){
							periodStat.stat(destInfo,this.level2_time,this.level3_time);
						}else{
							periodMap.remove(periodMap.size()-1);
						}
						preInfo = info; 
						periodStat = new PeriodStat();
						periodStat.periodTrackInfo.add(info);
						
						if(i==(trackInfo.size()-1)){ //���һ���㣬ֱ�Ӻ���
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
		
		//����ó��ڸ÷��������е�
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
