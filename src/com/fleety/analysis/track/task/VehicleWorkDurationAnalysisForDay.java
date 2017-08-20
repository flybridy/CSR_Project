package com.fleety.analysis.track.task;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;

import server.db.DbServer;

import com.fleety.analysis.AnalysisServer;
import com.fleety.analysis.track.ITrackAnalysis;
import com.fleety.analysis.track.TrackInfo;
import com.fleety.base.InfoContainer;
import com.fleety.track.TrackIO;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;

public class VehicleWorkDurationAnalysisForDay implements ITrackAnalysis{
	private HashMap vehicleMapping = null;
	private HashMap<String,CompanyInfo> vehComMapping = new HashMap<String,CompanyInfo>();
	private int duration = 60*1000;
	private SimpleDateFormat sdf  = new SimpleDateFormat("yyyy-MM-dd");
	private SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	@Override
	public boolean startAnalysisTrack(AnalysisServer parentServer,InfoContainer statInfo) {
		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
		Date eTime = statInfo.getDate(STAT_END_TIME_DATE);
		
		this.vehicleMapping = null;
//		this.driMapping = null;
//		String workDate = sdf.format(sTime);
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try{
			StatementHandle stmt = conn.createStatement();
			StringBuilder sb = new StringBuilder();
			sb.append("select count(*) as sum from ANA_VEHICLE_STATUS_DAY_STAT ")
			  .append(" where work_date = to_date('").append(sdf.format(sTime)).append("','yyyy-mm-dd')");
			ResultSet sets = stmt.executeQuery(sb.toString());
			if(sets.next()){
				int sum = sets.getInt("sum");
				if(sum == 0)
					this.vehicleMapping = new HashMap();
			}
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
		
		if(this.vehicleMapping == null){
			System.out.println("Not Need Analysis:"+this.toString());
		}else{
//			initCompanyInfo();
			System.out.println("Start Analysis:"+this.toString());
		}
		System.out.println("Start VehicleWorkDuration Analysis:"+this.toString());
		return this.vehicleMapping != null;
	}
	
//	private void initCompanyInfo()
//	{
//		DbHandle conn = DbServer.getSingleInstance().getConn();
//		try{
//			StatementHandle stmt = conn.createStatement();
//			ResultSet sets = stmt.executeQuery("select dest_no,company_id,company_name from v_ana_dest_info");
//			while(sets.next()){
//				CompanyInfo ci = new CompanyInfo();
//				String palteNo = sets.getString("dest_no");
//				ci.plateNo = palteNo;
//				ci.companyId = sets.getInt("company_id");
//				ci.companyName = sets.getString("company_name");
//				vehComMapping.put(palteNo, ci);
//			}
//		}catch(Exception e){
//			e.printStackTrace();
//		}finally{
//			DbServer.getSingleInstance().releaseConn(conn);
//		}
//	}
	
	@Override
	public void analysisDestTrack(AnalysisServer parentServer, TrackInfo trackInfo) {
		if (this.vehicleMapping == null) {
			return;
		}
		if(trackInfo.trackArr == null){
			return ;
		}
		
		/**
		 * 每辆车按照轨迹时间顺序计算过去,取发动机持续工作的时间，
		 * 当发动机熄火时间长达10分钟，不记为工作状态
		 */
		
		Calendar time = Calendar.getInstance();
		int status,preStatus = -1;
		String plateNo  = trackInfo.dInfo.destNo;
		long durationFlameoutTempTime = 0;
		long startTime = 0, endTime = 0;
		int gpsLocation = 0;
		int locationTimes = 0;
		VehicleInfo vehicleInfo = null;

		for (int i = 0; i < trackInfo.trackArr.length; i++) {
			time.setTime(trackInfo.trackArr[i].getDate(TrackIO.DEST_TIME_FLAG));
			status = (trackInfo.trackArr[i].getInteger(TrackIO.DEST_STATUS_FLAG).intValue()&0xF0);
			gpsLocation = trackInfo.trackArr[i].getInteger(TrackIO.DEST_LOCATE_FLAG);

			if(gpsLocation == 0)
				locationTimes ++;
			
			if(plateNo==null||plateNo.equals(""))
				continue;
			if(!vehicleMapping.containsKey(plateNo))
			{
				vehicleInfo = new VehicleInfo();
				vehicleInfo.companyId = trackInfo.dInfo.companyId;
				vehicleInfo.companyName = trackInfo.dInfo.companyName;
				vehicleInfo.analysisDate = new Date();
				vehicleInfo.plateNo = trackInfo.dInfo.destNo;
				vehicleInfo.workDate = trackInfo.trackArr[i].getDate(TrackIO.DEST_TIME_FLAG);
				vehicleInfo.startTime = time.getTime();
				vehicleInfo.reportTimes = trackInfo.trackArr.length;
			}
			else if(vehicleMapping.containsKey(plateNo))
			{
				vehicleInfo = (VehicleInfo) vehicleMapping.get(plateNo);
				vehicleInfo.locationTimes = locationTimes;
				vehicleInfo.endTime = time.getTime();
			}
			
			if (i == 0)// 第一个轨迹点
			{
				startTime = time.getTimeInMillis();
				endTime = time.getTimeInMillis();
			} else {
				startTime = endTime;
				endTime = time.getTimeInMillis();
				durationFlameoutTempTime=endTime-startTime;
				if(durationFlameoutTempTime<6*duration){
					vehicleInfo.totalOnlineTime += durationFlameoutTempTime;
				}else{
					vehicleInfo.totalOfflineTime += durationFlameoutTempTime;
				}
			}
			vehicleMapping.put(plateNo, vehicleInfo);
		}
	}
	
	@Override
	public void endAnalysisTrack(AnalysisServer parentServer,InfoContainer statInfo) {
		if(this.vehicleMapping == null){ 
			return ;
		}
		
		int recordNum = 0;
		String plateNo = "";
		VehicleInfo vehicleInfo;
		Date sDate = statInfo.getDate(STAT_START_TIME_DATE);
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try{
			conn.setAutoCommit(false);
//			StatementHandle stmt = null;
//			 delete data first
			StatementHandle stmt2 = conn.createStatement();
			int i=0,j=0;
			Date workDate =null;
			for(Iterator itr = this.vehicleMapping.keySet().iterator();itr.hasNext();){
				plateNo = (String)itr.next();
				vehicleInfo = (VehicleInfo)this.vehicleMapping.get(plateNo);
				workDate = vehicleInfo.workDate;
				break;
			}
			StringBuilder sql = new StringBuilder();
			sql.append("delete from ANA_VEHICLE_STATUS_DAY_STAT where ").append(" work_date = to_date('").append(sdf.format(workDate)).append("','yyyy-mm-dd')");
			stmt2.execute(sql.toString());
//			for(Iterator itr = this.vehicleMapping.keySet().iterator();itr.hasNext();){
//				plateNo = (String)itr.next();
//				i++;
//				vehicleInfo = (VehicleInfo)this.vehicleMapping.get(plateNo);
//				Date workDate = vehicleInfo.workDate;
//				StringBuilder sql = new StringBuilder();
//				sql.append("delete from ANA_VEHICLE_STATUS_DAY_STAT where plate_no ='").append(plateNo).append("'")
//				   .append(" and work_date = to_date('").append(sdf.format(workDate)).append("','yyyy-mm-dd')");
//				stmt2.addBatch(sql.toString());
//				if(i%200==0){
//					stmt2.executeBatch();
//				}
//			}
//			stmt2.executeBatch();
			// insert into databases
			StatementHandle stmt = conn.prepareStatement("insert into ANA_VEHICLE_STATUS_DAY_STAT(id,company_id,company_name,plate_no,work_date,online_minutes,offline_minutes,report_times,location_times,analysis_date,start_time,end_time) values(?,?,?,?,?,?,?,?,?,?,?,?)");
			for(Iterator itr = this.vehicleMapping.keySet().iterator();itr.hasNext();){
				plateNo = (String)itr.next();
				j++;
				vehicleInfo = (VehicleInfo)this.vehicleMapping.get(plateNo);
				String workDateStr = sdf.format(vehicleInfo.workDate);
				stmt.setInt(1, (int)DbServer.getSingleInstance().getAvaliableId(conn, "ANA_VEHICLE_STATUS_DAY_STAT", "id"));
				stmt.setInt(2, vehicleInfo.companyId);
				stmt.setString(3, vehicleInfo.companyName);
				stmt.setString(4, vehicleInfo.plateNo);
				stmt.setDate(5, new java.sql.Date(vehicleInfo.workDate.getTime()));
				float onlineTime = (Math.round((vehicleInfo.totalOnlineTime/duration)*100)/100);
				stmt.setFloat(6, onlineTime);
				stmt.setFloat(7, (24 * 60) - onlineTime);
				stmt.setInt(8, vehicleInfo.reportTimes);
				stmt.setInt(9, vehicleInfo.locationTimes);
				stmt.setTimestamp(10, new Timestamp(new Date().getTime()));
				stmt.setTimestamp(11, new Timestamp(sdf2.parse(workDateStr+" 00:00:01").getTime()));
				stmt.setTimestamp(12, new Timestamp(sdf2.parse(workDateStr+" 23:59:59").getTime()));
				recordNum ++;
				stmt.addBatch();
				if(j%200==0){
					stmt.executeBatch();
				}
			}
			stmt.executeBatch();
			conn.commit();
		}catch(Exception e){
			e.printStackTrace();
			if(conn != null){
				try{
					conn.rollback();
				}catch(Exception ee){
					ee.printStackTrace();
				}
			}
			recordNum = 0;
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
		System.out.println("Finish driver work duration Analysis:"+this.toString()+" recordNum="+recordNum);
	}
	
	
	public String toString(){
		return "DriverWorkDurationAnalysisForDay";
	}
	
	private class VehicleInfo{
		public int companyId = 0;
		public String companyName = null;
		public String plateNo = "";
		public long totalOnlineTime;
		public long totalOfflineTime;
		public int onlineMinutes = 0;
		public int offlineMinutes = 0;
		public int reportTimes = 0;
		public int locationTimes = 0;
		public Date workDate;
		public Date analysisDate;
		public Date startTime;
		public Date endTime;
	}
	
	private class CompanyInfo{
		public String plateNo = null;
		public int companyId = 0;
		public String companyName = null;
	}
}
