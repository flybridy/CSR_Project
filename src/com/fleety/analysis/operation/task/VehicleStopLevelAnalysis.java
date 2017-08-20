package com.fleety.analysis.operation.task;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import com.fleety.analysis.AnalysisServer;
import com.fleety.analysis.track.ITrackAnalysis;
import com.fleety.analysis.track.TrackInfo;
import com.fleety.base.InfoContainer;
import com.fleety.track.TrackIO;
import com.fleety.util.pool.db.redis.RedisConnPoolServer;
import com.fleety.util.pool.db.redis.RedisConnPoolServer.JedisHandle;

public class VehicleStopLevelAnalysis implements ITrackAnalysis {
	private HashMap vehicleMapping = null;

	public boolean startAnalysisTrack(AnalysisServer parentServer, InfoContainer statInfo) {
		if(vehicleMapping == null){
			vehicleMapping = new HashMap();
		}
		vehicleMapping.clear();

		return true;
	}

	public void analysisDestTrack(AnalysisServer parentServer, TrackInfo trackInfo) {
		if (trackInfo.trackArr == null || trackInfo.trackArr.length == 0) {
			return;
		}

		long minStopDuration = parentServer.getIntegerPara("level_min_stop_duration").intValue();
		minStopDuration = minStopDuration*60*1000;
		int speed = parentServer.getIntegerPara("level_max_stop_speed").intValue();
		
		String plateNo = trackInfo.dInfo.destNo;
		int mdtId = trackInfo.dInfo.mdtId,curSpeed;
		long startTime = 0, endTime = 0,curTime;

		for (int i = 0; i < trackInfo.trackArr.length; i++) {
			curTime = trackInfo.trackArr[i].getDate(TrackIO.DEST_TIME_FLAG).getTime();
			curSpeed = trackInfo.trackArr[i].getInteger(TrackIO.DEST_SPEED_FLAG).intValue();
			
			if (curSpeed > speed){
				startTime = endTime = 0;
			}else {
				if(startTime == 0){
					startTime = curTime;
				}
				endTime = curTime;
			}
		}
		
		if(startTime >0 && endTime-startTime > minStopDuration){
			this.vehicleMapping.put(plateNo, new Object[]{startTime,endTime});
		}
	}

	public void endAnalysisTrack(AnalysisServer parentServer, InfoContainer statInfo) {
		if (this.vehicleMapping == null) {
			return;
		}

		int num = 0;
		RedisConnPoolServer server = RedisConnPoolServer.getSingleInstance();
		JedisHandle conn = server.getJedisConnection();
		try {
			// delete data from redis
			DestStopLevelRecordBean bean = new DestStopLevelRecordBean();
			if (conn != null) {
				conn.select(0);
				server.clearTableRecord(bean);
			}

			String plateNo;
			Object[] infoArr;
			DestStopLevelRecordBean[] stopTimeBean = new DestStopLevelRecordBean[vehicleMapping.size()];
			for (Iterator itr = this.vehicleMapping.keySet().iterator(); itr.hasNext();) {
				plateNo = (String) itr.next();
				infoArr = (Object[]) this.vehicleMapping.get(plateNo);
				DestStopLevelRecordBean stopObj = new DestStopLevelRecordBean();
				stopObj.setUid(plateNo);
				stopObj.setStartTime(((Long)infoArr[0]).longValue());
				stopObj.setEndTime(((Long)infoArr[1]).longValue());
				stopObj.setRecordTime(System.currentTimeMillis());
				
				stopTimeBean[num] = stopObj;
				
				num ++;
			}

			if (conn != null) {
				conn.select(0);
				server.saveTableRecord(stopTimeBean);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			server.releaseJedisConnection(conn);
		}
		System.out.println("Finish vehicle stop level Analysis:" + this.toString() + " recordNum=" + num);
	}
}
