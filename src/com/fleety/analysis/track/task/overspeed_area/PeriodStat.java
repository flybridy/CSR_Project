package com.fleety.analysis.track.task.overspeed_area;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;

import server.var.VarManageServer;

import com.fleety.analysis.track.DestInfo;
import com.fleety.base.GeneralConst;
import com.fleety.base.InfoContainer;
import com.fleety.base.StrFilter;
import com.fleety.track.TrackIO;

public class PeriodStat {
	
	private final static int STATUS_FREE = 0; // 空车
	private final static int STATUS_LOAD = 1; // 重车
	
	int preTime = 5;
	int period = 0;
	ArrayList periodTrackInfo = new ArrayList();

	int firstSpeed = 0;// 开始超速速度
	int lastSpeed = 0;// 最后超速速度
	int maxSpeed = 0;// 最大速度
	int minSpeed = 0;// 最小速度
	double avgSpeed = 0d; //平均速度

	
	Date firstTime = null;
	Date lastTime = null;

	
	int firstStatus = 0;
	int lastStatus = 0;
	
	double firstlo =0d;
	double firstla = 0d;
	
	double lastlo =0d;
	double lastla = 0d;
	
	
		
	

	public PeriodStat() {
		super();
	}

	public void stat(DestInfo destInfo) {
		// 对区域内的点进行分段，5分钟以内的算是一段
		int curStatus = 0, lastStatus = 0, hour = 0;
		PeriodStat periodStat = null;
		
		InfoContainer info = null;
		InfoContainer firstInfo = null;
		InfoContainer lastInfo = null;
		InfoContainer preInfo = null;
		InfoContainer maxInfo = null;
		InfoContainer minInfo = null;
		
		int curSpeed = 0;
		double totalSpeed = 0;
		
		int fspeed = 0;
		int lspeed = 0;
		int maspeed = 0;
		int mispeed = 0;
		
		if(periodTrackInfo.size()==0){
			return;
		}
		firstInfo=(InfoContainer) periodTrackInfo.get(0);
		lastInfo=(InfoContainer) periodTrackInfo.get(periodTrackInfo.size()-1);
		fspeed=firstInfo.getInteger(TrackIO.DEST_SPEED_FLAG);
		maspeed=fspeed;
		maxInfo=firstInfo;
		mispeed=fspeed;
		minInfo=firstInfo;
		totalSpeed +=firstSpeed;
		for(int i=1;i<periodTrackInfo.size();i++){
			info = (InfoContainer) periodTrackInfo.get(i);
			curSpeed=info.getInteger(TrackIO.DEST_SPEED_FLAG);
			if(curSpeed>maspeed){
				maspeed=curSpeed;
				maxInfo=info;
			}
			if(curSpeed<mispeed){
				mispeed=curSpeed;
				minInfo=info;
			}
			totalSpeed+=curSpeed;
		}
		this.firstSpeed=fspeed;
		this.firstStatus=firstInfo.getInteger(TrackIO.DEST_STATUS_FLAG) & 0xf;
		this.firstTime=firstInfo.getDate(TrackIO.DEST_TIME_FLAG);
		this.firstlo=firstInfo.getDouble(TrackIO.DEST_LO_FLAG);
		this.firstla=firstInfo.getDouble(TrackIO.DEST_LA_FLAG);
		
		this.lastSpeed=lspeed;
		this.lastStatus=lastInfo.getInteger(TrackIO.DEST_STATUS_FLAG) & 0xf;
		this.lastTime=lastInfo.getDate(TrackIO.DEST_TIME_FLAG);
		this.lastlo=lastInfo.getDouble(TrackIO.DEST_LO_FLAG);
		this.lastla=lastInfo.getDouble(TrackIO.DEST_LA_FLAG);
		
		this.maxSpeed=maspeed;
		this.minSpeed=mispeed;
		this.avgSpeed=totalSpeed/periodTrackInfo.size();
		this.clear();
	}

	private void clear() {
		this.periodTrackInfo.clear();
	}
}
