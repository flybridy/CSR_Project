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

public class VehicleSpeedAVGAnalysisForDay implements ITrackAnalysis{
	private HashMap vehicleMapping = null;
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
			sb.append("select count(*) as sum from ANA_VEHICLE_SPEED_AVG_DAY ")
			  .append(" where workdate = to_date('").append(sdf.format(sTime)).append("','yyyy-mm-dd')");
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
		System.out.println("Start VehicleSpeedAVGAnalysisForDay Analysis:"+this.toString());
		return this.vehicleMapping != null;
	}
	

	
	@Override
	public void analysisDestTrack(AnalysisServer parentServer, TrackInfo trackInfo) {
		if (this.vehicleMapping == null) {
			return;
		}
		if(trackInfo.trackArr == null||trackInfo.trackArr.length==0){
			return ;
		}
					
		Calendar time = Calendar.getInstance();
		int status,preStatus = -1;
		String plateNo  = trackInfo.dInfo.destNo;
		int type=trackInfo.dInfo.carType;
		
		
		
		long speed=0;
		long m_speed=0;//早高峰速度和
		long d_speed=0;
		long n_speed=0;
		int m_speed_point=0;//早高峰统计点数
		int d_speed_point=0;
		int n_speed_point=0;
		VehicleInfo vehicleInfo = null;

		for (int i = 0; i < trackInfo.trackArr.length; i++) {
			time.setTime(trackInfo.trackArr[i].getDate(TrackIO.DEST_TIME_FLAG));
			status = (trackInfo.trackArr[i].getInteger(TrackIO.DEST_STATUS_FLAG).intValue()&0xF0);
           speed=trackInfo.trackArr[i].getLong(TrackIO.DEST_SPEED_FLAG);
			if(plateNo==null||plateNo.equals("")){
				System.out.println("咩有获取到车牌号");
				continue;
			}
			
			if(!vehicleMapping.containsKey(plateNo))
			{
				vehicleInfo = new VehicleInfo();
				vehicleInfo.companyId = trackInfo.dInfo.companyId;
				vehicleInfo.companyName = trackInfo.dInfo.companyName;
				vehicleInfo.analysisDate = new Date();
				vehicleInfo.plateNo = trackInfo.dInfo.destNo;
				vehicleInfo.workDate = trackInfo.trackArr[i].getDate(TrackIO.DEST_TIME_FLAG);
			}
			else if(vehicleMapping.containsKey(plateNo))
			{
				vehicleInfo = (VehicleInfo) vehicleMapping.get(plateNo);
				
			}
			vehicleInfo.type=type;
			vehicleInfo.day_speed_points++;
			int res=DateJudge(time.getTimeInMillis());
			if(res==1){
				vehicleInfo.monring_speed+=speed;
				vehicleInfo.monring_speed_points++;
				
			}else if(res==2){
				vehicleInfo.night_speed+=speed;
				vehicleInfo.night_speed_pints++;
			}
			vehicleMapping.put(plateNo, vehicleInfo);
		}
	}
	
	@Override
	public void endAnalysisTrack(AnalysisServer parentServer,InfoContainer statInfo) {
		System.out.println("插入数据，插入数据"+vehicleMapping.size()+"条");
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
			
			int i=0,j=0;
			
			StatementHandle stmt = conn.prepareStatement("insert into ANA_VEHICLE_SPEED_AVG_DAY(id,company_id,company_name,plate_no,type,analysis_date,monring_speed,night_speed,day_speed,workDate) values(?,?,?,?,?,?,?,?,?,?)");
			for(Iterator itr = this.vehicleMapping.keySet().iterator();itr.hasNext();){
				plateNo = (String)itr.next();
				j++;
				vehicleInfo = (VehicleInfo)this.vehicleMapping.get(plateNo);
				
				stmt.setInt(1, (int)DbServer.getSingleInstance().getAvaliableId(conn, "ANA_VEHICLE_SPEED_AVG_DAY", "id"));
				stmt.setInt(2, vehicleInfo.companyId);
				if(vehicleInfo.companyName==null){
					stmt.setString(3, "");
					System.out.println("该车辆公司名为空  公司id为："+vehicleInfo.companyId+" 车牌"+vehicleInfo.plateNo);
				}else{
					stmt.setString(3, vehicleInfo.companyName);
				}
				stmt.setString(4, vehicleInfo.plateNo);
				stmt.setInt(5, vehicleInfo.type);
				stmt.setTimestamp(6, new Timestamp(new Date().getTime()));
				stmt.setLong(7, (long) (vehicleInfo.monring_speed*1.0/vehicleInfo.monring_speed_points));
				stmt.setLong(8, (long) (vehicleInfo.night_speed*1.0/vehicleInfo.night_speed_pints));
				stmt.setLong(9, (long) (vehicleInfo.day_speed*1.0/vehicleInfo.day_speed_points));
				stmt.setDate(10, new java.sql.Date(vehicleInfo.workDate.getTime()));
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
		System.out.println("Finish VehicleSpeedAVGAnalysisForDay Analysis:"+this.toString()+" recordNum="+recordNum);
	}
	
	
	public String toString(){
		return "VehicleSpeedAVGAnalysisForDay";
	}
	
	private class VehicleInfo{
		public int companyId = 0;
		public String companyName ="";
		public String plateNo = "";
		public Date workDate;
		public Date analysisDate;
		public int type;
		
		public long monring_speed;
		public long night_speed;
		public long day_speed;
		
		public int monring_speed_points;
		public int day_speed_points;
		public int night_speed_pints;
		
	}
	
	private int DateJudge(long da){
		 int res=0;
		 Calendar ca= Calendar.getInstance();
		 ca.setTimeInMillis(da);
		 int hour  = ca.get(Calendar.HOUR_OF_DAY);
		 int minute = ca.get(Calendar.MINUTE);
		 if(hour>=7 && hour<=9){
			 return 1;//早高峰
		 }else if(hour==17&&minute>=30){
				 return 2;//晚高峰
		 }else if (hour==18){
			 return 2;//晚高峰
		 }else if(hour==19&&minute<=30){
			 return 2;//晚高峰
		 }
		return res;
	}
}
