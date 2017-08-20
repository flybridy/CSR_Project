package com.fleety.analysis.track.task;

import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Calendar;
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

public class DriverWorkDurationAnalysisForDay implements ITrackAnalysis{
	private HashMap driMapping = null;
	private HashMap<String,String> driverMap = new HashMap<String,String>();
	private int duration = 60*1000;
	private SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");
	private SimpleDateFormat sdf2=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	@Override
	public boolean startAnalysisTrack(AnalysisServer parentServer,InfoContainer statInfo) {
		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
		Date eTime = statInfo.getDate(STAT_END_TIME_DATE);


		DbHandle conn = DbServer.getSingleInstance().getConn();
		try{
			StatementHandle stmt = conn.createStatement();
			ResultSet sets = stmt.executeQuery("select SERVICE_NO,DRIVER_NAME from driver_info");
			while(sets.next()){
				driverMap.put(sets.getString("SERVICE_NO"), sets.getString("DRIVER_NAME"));
			}
			StatementHandle stmt1 = conn.createStatement();
			ResultSet sets1 = stmt1.executeQuery("select count(*) sum from ANA_DRIVER_STATUS_DAY_STAT where WORK_DATE = '"+GeneralConst.YYYY_MM_DD.format(sTime)+"'");
			if(sets1.next()){
				int sum=sets1.getInt("sum");
				if(sum==0){
					this.driMapping = new HashMap();
				}	
			}
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
		
		if(this.driMapping == null){
			System.out.println("Not Need Analysis:"+this.toString());
		}else{
			System.out.println("Start Analysis:"+this.toString());
		}
		System.out.println("Start Analysis:"+this.toString());
		return this.driMapping != null;
	}
	
	@Override
	public void analysisDestTrack(AnalysisServer parentServer, TrackInfo trackInfo) {
		if(this.driMapping == null){
			return ;
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
		String driverNo  = "";
		long durationFlameoutTempTime = 0;
		long startTime = 0, endTime = 0;
		DriverInfo driverInfo = null;

		for (int i = 0; i < trackInfo.trackArr.length; i++) {
			time.setTime(trackInfo.trackArr[i].getDate(TrackIO.DEST_TIME_FLAG));
			driverNo = trackInfo.trackArr[i].getString(TrackIO.DRIVER_NO_INFO_FLAG);
			status = (trackInfo.trackArr[i].getInteger(TrackIO.DEST_STATUS_FLAG).intValue()&0xF0);
			if(driverNo==null||driverNo.equals(""))
				continue;
			if(!driMapping.containsKey(driverNo))
			{
				driverInfo = new DriverInfo();
				driverInfo.driverNo = driverNo;
				driverInfo.driverName = driverMap.containsKey(driverNo)?driverMap.get(driverNo):"";
				driverInfo.companyId = trackInfo.dInfo.companyId;
				driverInfo.companyName = trackInfo.dInfo.companyName;
				driverInfo.analysisDate = new Date();
				driverInfo.plateNo = trackInfo.dInfo.destNo;
				driverInfo.workDate = sdf.format(trackInfo.trackArr[i].getDate(TrackIO.DEST_TIME_FLAG));
				driverInfo.startTime = time.getTime();
			}
			else if(driMapping.containsKey(driverNo))
			{
				driverInfo = (DriverInfo) driMapping.get(driverNo);
				driverInfo.endTime = time.getTime();
			}
			
			if (i == 0)// 第一个轨迹点
			{
				startTime = time.getTimeInMillis();
				endTime = time.getTimeInMillis();
			} else {
				startTime = endTime;
				endTime = time.getTimeInMillis();
				preStatus = trackInfo.trackArr[i-1].getInteger(TrackIO.DEST_STATUS_FLAG).intValue() & 0xF0;
				String preDriverNo = trackInfo.trackArr[i-1].getString(TrackIO.DRIVER_NO_INFO_FLAG);
				if (driverNo!=null&&driverNo.equals(preDriverNo)) {
					if (status >= 1 && preStatus >= 1) {
						driverInfo.totalDurationWorkTime += endTime - startTime;
					} else if (status >= 1 && preStatus == 0) {
						if(durationFlameoutTempTime / duration <= 10)
							driverInfo.totalDurationWorkTime += durationFlameoutTempTime;
						else
							driverInfo.totalDurationFlameoutTime += durationFlameoutTempTime;
						startTime = time.getTimeInMillis();
						endTime = time.getTimeInMillis();
						durationFlameoutTempTime = 0;
					} else if (status == 0 && preStatus >= 1) {
						startTime = time.getTimeInMillis();
						endTime = time.getTimeInMillis();
					} else if (status == 0 && preStatus == 0) {
						durationFlameoutTempTime += endTime - startTime;
					}
				} else {
					startTime = time.getTimeInMillis();
					endTime = time.getTimeInMillis();
				}
			}
			driMapping.put(driverNo, driverInfo);
		}
	}
	
	@Override
	public void endAnalysisTrack(AnalysisServer parentServer,InfoContainer statInfo) {
		if(this.driMapping == null){ 
			return ;
		}
		
		int recordNum = 0;
		String driverNo = "";
		DriverInfo driverInfo;
		Date sDate = statInfo.getDate(STAT_START_TIME_DATE);
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try{
			conn.setAutoCommit(false);
			StatementHandle stmt = conn.prepareStatement("insert into ANA_DRIVER_STATUS_DAY_STAT(id,company_id,company_name,plate_no,driver_id,driver_name,work_date,work_minutes,flameout_minutes,unknown_minutes,online_minutes,analysis_time) values(?,?,?,?,?,?,?,?,?,?,?,?)");
			for(Iterator itr = this.driMapping.keySet().iterator();itr.hasNext();){
				driverNo = (String)itr.next();
				driverInfo = (DriverInfo)this.driMapping.get(driverNo);
				stmt.setInt(1, (int)DbServer.getSingleInstance().getAvaliableId(conn, "ANA_DRIVER_STATUS_DAY_STAT", "id"));
				stmt.setInt(2, driverInfo.companyId);
				stmt.setString(3, driverInfo.companyName);
				stmt.setString(4, driverInfo.plateNo);
				stmt.setString(5, driverNo);
				stmt.setString(6, driverInfo.driverName);
				stmt.setString(7, driverInfo.workDate);
				stmt.setLong(8, driverInfo.totalDurationWorkTime/duration);
				stmt.setLong(9, driverInfo.totalDurationFlameoutTime/duration);
				stmt.setLong(10, driverInfo.totalDurationUnknownTime/duration);
				stmt.setLong(11, (driverInfo.totalDurationUnknownTime+driverInfo.totalDurationFlameoutTime)/duration);
				stmt.setDate(12, new java.sql.Date(new Date().getTime()));
				stmt.addBatch();
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
	
	private class DriverInfo{
		public String driverNo = null;
		public String driverName = "";
		public int companyId = 0;
		public String companyName = null;
		public long totalDurationWorkTime =0;
		public long totalDurationFlameoutTime = 0;
		public long totalDurationUnknownTime = 0;
		public long onlineTime = 0;
		public String plateNo = "";
		public String workDate = "";
		public Date analysisDate;
		public Date startTime;
		public Date endTime;
	}
}
