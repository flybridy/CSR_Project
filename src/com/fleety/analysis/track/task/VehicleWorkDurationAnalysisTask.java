package com.fleety.analysis.track.task;

import java.sql.ResultSet;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;

import server.db.DbServer;

import com.fleety.analysis.AnalysisServer;
import com.fleety.analysis.track.ITrackAnalysis;
import com.fleety.analysis.track.TrackInfo;
import com.fleety.base.GeneralConst;
import com.fleety.base.InfoContainer;
import com.fleety.track.TrackIO;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;

public class VehicleWorkDurationAnalysisTask implements ITrackAnalysis {
	private HashMap vehicleMapping = null;
	private int workMinSpeed = 5;
	@Override
	public boolean startAnalysisTrack(AnalysisServer parentServer,InfoContainer statInfo) {
		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
		Date eTime = statInfo.getDate(STAT_END_TIME_DATE);

		if(parentServer.getIntegerPara("work_min_speed") != null){
			this.workMinSpeed = parentServer.getIntegerPara("work_min_speed").intValue();
		}
		
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			StatementHandle stmt = conn.createStatement();
			StringBuilder sb = new StringBuilder();
			sb.append("select count(*) as sum from ANA_VEH_WORK_DURA_STAT ")
					.append(" where STAT_TIME = to_date('")
					.append(GeneralConst.YYYY_MM_DD.format(sTime)).append("','yyyy-mm-dd')");
			ResultSet sets = stmt.executeQuery(sb.toString());
			if (sets.next()) {
				int sum = sets.getInt("sum");
				if (sum == 0)
					this.vehicleMapping = new HashMap();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}

		if (this.vehicleMapping == null) {
			System.out.println("Not Need Analysis:" + this.toString());
		} else {
			System.out.println("Start Analysis:" + this.toString());
		}
		return this.vehicleMapping != null;
	}

	@Override
	public void analysisDestTrack(AnalysisServer parentServer,TrackInfo trackInfo) {
		StatInfo statInfo = new StatInfo();
		statInfo.dInfo = trackInfo.dInfo;
		
		long workDuration = 0,preTime = 0,time = 0;
		int speed;
		InfoContainer[] trackArr = trackInfo.trackArr;
		for(int i=0;i<trackArr.length;i++){
			speed = trackArr[i].getInteger(TrackIO.DEST_SPEED_FLAG).intValue();
			if(speed < this.workMinSpeed){
				time = preTime = 0;
				continue;
			}
			
			time = trackArr[i].getDate(TrackIO.DEST_TIME_FLAG).getTime()/1000;
			if(preTime > 0){
				workDuration += time - preTime;
			}
			
			preTime = time;
		}
		statInfo.workDuration = (int)Math.round(workDuration/60.0);
		statInfo.idleDuration = 24*60-statInfo.workDuration;
		
		this.vehicleMapping.put(statInfo.dInfo.destNo, statInfo);
	}

	@Override
	public void endAnalysisTrack(AnalysisServer parentServer,InfoContainer statInfo) {
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			conn.setAutoCommit(false);
			StatementHandle stmt = conn.prepareStatement("insert into ANA_VEH_WORK_DURA_STAT(id,plate_no,company_id,company_name,work_duration,idle_duration,stat_time) values(?,?,?,?,?,?,?)");
			
			StatInfo vInfo;
			int count = 0;
			for(Iterator itr = this.vehicleMapping.values().iterator();itr.hasNext();){
				vInfo = (StatInfo)itr.next();
				
				stmt.setInt(1, (int)DbServer.getSingleInstance().getAvaliableId(conn, "ANA_VEH_WORK_DURA_STAT", "id"));
				stmt.setString(2, vInfo.dInfo.destNo);
				stmt.setInt(3, vInfo.dInfo.companyId);
				stmt.setString(4, vInfo.dInfo.companyName);
				stmt.setInt(5, vInfo.workDuration);
				stmt.setInt(6, vInfo.idleDuration);
				stmt.setDate(7, new java.sql.Date(statInfo.getDate(STAT_START_TIME_DATE).getTime()));
				stmt.addBatch();
				count ++;
				
				if((count%1000) == 0){
					stmt.executeBatch();
				}
			}
			stmt.executeBatch();

			conn.commit();
		} catch (Exception e) {
			e.printStackTrace();
			if(conn != null){
				try{
					conn.rollback();
				}catch(Exception er){
					er.printStackTrace();
				}
			}
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}
		
		this.vehicleMapping = null;
	}

	private class StatInfo{
		public com.fleety.analysis.track.DestInfo dInfo = null;
		public int workDuration = 0;
		public int idleDuration = 0;
	}
}
