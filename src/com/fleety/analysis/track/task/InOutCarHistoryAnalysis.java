package com.fleety.analysis.track.task;

import java.awt.Polygon;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;

import server.db.DbServer;
import server.threadgroup.PoolInfo;
import server.threadgroup.ThreadPoolGroupServer;

import com.fleety.analysis.AnalysisServer;
import com.fleety.analysis.track.ITrackAnalysis;
import com.fleety.analysis.track.TrackInfo;
import com.fleety.base.GeneralConst;
import com.fleety.base.InfoContainer;
import com.fleety.server.area.AreaLoadServer;
import com.fleety.track.TrackIO;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.fleety.util.pool.thread.ITask;
import com.fleety.util.pool.thread.ThreadPool;

public class InOutCarHistoryAnalysis  implements ITrackAnalysis {
	private HashMap carInOutHistoryMapping = null;
	private HashMap<String,Polygon> polygonMap = null;
	private Polygon polygonsz = null;
	private Polygon polygongn = null;
	private Polygon polygongnw = null;
	private ThreadPool pool = null;
	private int type_0 = 0;
	private int type_1 = 1;
	private int typeId_2 = 2;//绿的
	private long maxTime = 2*60*1000;
	
	public boolean startAnalysisTrack(AnalysisServer parentServer,
			InfoContainer statInfo) {
		polygonMap = AreaLoadServer.getSingleInstance().polygonMap;
		if(polygonMap==null){
			return false;
		}
		if(!polygonMap.containsKey("area_sz")){
			System.out.println("深圳区域未加载");
			return false;
		}
		if(!polygonMap.containsKey("area_gn")){
			System.out.println("关内区域未加载");
			return false;
		}
		if(!polygonMap.containsKey("area_gn_w")){
			System.out.println("关内区域未加载");
			return false;
		}
		polygonsz = polygonMap.get("area_sz");
		polygongn = polygonMap.get("area_gn");
		polygongnw = polygonMap.get("area_gn_w");
		PoolInfo pInfo = new PoolInfo();
		pInfo.workersNumber = 5;
		pInfo.taskCapacity = 10000;
		pInfo.poolType = ThreadPool.MULTIPLE_TASK_LIST_POOL;
		try{
			this.pool = ThreadPoolGroupServer.getSingleInstance().createThreadPool("inoutcarhistory_data_save", pInfo);
		}catch(Exception e){
			e.printStackTrace();
			return false;
		}
		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);

		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			StringBuilder sb = new StringBuilder();
			sb.append("select * from car_in_out_history_info ").append(
					" where query_time>=? and query_time<?");
			StatementHandle stmt = conn.prepareStatement(sb.toString());
			stmt.setTimestamp(1, new Timestamp(sTime.getTime()));
			stmt.setTimestamp(2, new Timestamp(sTime.getTime()+GeneralConst.ONE_DAY_TIME));
			ResultSet sets = stmt.executeQuery();
			if (!sets.next()) {
					this.carInOutHistoryMapping = new HashMap();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}

		if (this.carInOutHistoryMapping == null) {
			System.out.println("Not Need Analysis:" + this.toString());
		} else {
			System.out.println("Start Analysis:" + this.toString());
		}
		return this.carInOutHistoryMapping != null;
	}
	
	public void analysisDestTrack(AnalysisServer parentServer,
			TrackInfo trackInfo) {
		try {
			this.addCityInOut(trackInfo);
			this.addGuanNeiInOut(trackInfo);
		} catch (Exception e) {
			carInOutHistoryMapping = null;
			e.printStackTrace();
		}
	}
	private void addCityInOut(TrackInfo trackInfo) throws Exception{
		String carNo = trackInfo.dInfo.destNo;
		if (trackInfo.trackArr.length<=0) {
			return;
		}
		double slo = -1;
		double sla = -1;
		double sSpeed = 0;
		long sTime = 0;
		int sStatus = -1;
		double templo = 0;
		double templa = 0;
		double tempSpeed = 0;
		int tempStatus = -1;
		long tempTime = 0;
		boolean isStart = false,isIn;
		
		InOutCarHistoryInfo inOutCarHistoryInfo = null;
		for (int i = 0; i < trackInfo.trackArr.length; i++) {
			templo = trackInfo.trackArr[i].getDouble(TrackIO.DEST_LO_FLAG);
			templa = trackInfo.trackArr[i].getDouble(TrackIO.DEST_LA_FLAG);
			tempSpeed = trackInfo.trackArr[i].getDouble(TrackIO.DEST_SPEED_FLAG);
			tempStatus = trackInfo.trackArr[i].getInteger(TrackIO.DEST_STATUS_FLAG);
			tempTime = trackInfo.trackArr[i].getDate(TrackIO.DEST_TIME_FLAG).getTime();
			
			isIn = polygonsz.contains(templo*10000000, templa*10000000);
			if(!isIn){
				if(!isStart){
					isStart = true;
					slo = templo;
					sla = templa;
					sSpeed = tempSpeed;
					sStatus = tempStatus;
					sTime = tempTime;
				}
				continue;
			}else{
				if(!isStart){
					continue;
				}
			}
			
			if(tempTime - sTime > this.maxTime){
				inOutCarHistoryInfo = new InOutCarHistoryInfo();
				inOutCarHistoryInfo.carNo = carNo;
				inOutCarHistoryInfo.companyId = trackInfo.dInfo.companyId;
				inOutCarHistoryInfo.companyName = trackInfo.dInfo.companyName;
				inOutCarHistoryInfo.platId = trackInfo.dInfo.gpsRunComId;
				inOutCarHistoryInfo.platName = trackInfo.dInfo.gpsRunComName;
				inOutCarHistoryInfo.s_lo = slo;
				inOutCarHistoryInfo.s_la = sla;
				inOutCarHistoryInfo.e_lo = templo;
				inOutCarHistoryInfo.e_la = templa;
				inOutCarHistoryInfo.s_speed = sSpeed;
				inOutCarHistoryInfo.e_speed = tempSpeed;
				inOutCarHistoryInfo.s_status = sStatus;
				inOutCarHistoryInfo.e_status = tempStatus;
				inOutCarHistoryInfo.queryTime = new Timestamp(trackInfo.sDate.getTime());
				inOutCarHistoryInfo.outTime = new Timestamp(sTime);
				inOutCarHistoryInfo.inTime = new Timestamp(tempTime);
				inOutCarHistoryInfo.type = this.type_0;
				
				carInOutHistoryMapping.put(carNo+"_"+trackInfo.trackArr.length+"_"+this.type_0, inOutCarHistoryInfo);
			}
			//清理信息，重新开始
			isStart = false;
		}
		
		if(isStart && tempTime - sTime > this.maxTime){
			inOutCarHistoryInfo = new InOutCarHistoryInfo();
			inOutCarHistoryInfo.carNo = carNo;
			inOutCarHistoryInfo.companyId = trackInfo.dInfo.companyId;
			inOutCarHistoryInfo.companyName = trackInfo.dInfo.companyName;
			inOutCarHistoryInfo.platId = trackInfo.dInfo.gpsRunComId;
			inOutCarHistoryInfo.platName = trackInfo.dInfo.gpsRunComName;
			inOutCarHistoryInfo.s_lo = slo;
			inOutCarHistoryInfo.s_la = sla;
			inOutCarHistoryInfo.e_lo = templo;
			inOutCarHistoryInfo.e_la = templa;
			inOutCarHistoryInfo.s_speed = sSpeed;
			inOutCarHistoryInfo.e_speed = tempSpeed;
			inOutCarHistoryInfo.s_status = sStatus;
			inOutCarHistoryInfo.e_status = tempStatus;
			inOutCarHistoryInfo.queryTime = new Timestamp(trackInfo.sDate.getTime());
			inOutCarHistoryInfo.outTime = new Timestamp(sTime);
			inOutCarHistoryInfo.inTime = new Timestamp(tempTime);
			inOutCarHistoryInfo.type = this.type_0;
			
			carInOutHistoryMapping.put(carNo+"_"+trackInfo.trackArr.length+"_"+this.type_0, inOutCarHistoryInfo);
		}
	}
	private void addGuanNeiInOut(TrackInfo trackInfo) throws Exception{
		String carNo = trackInfo.dInfo.destNo;
		int carType = trackInfo.dInfo.carType;
		if(carType!=this.typeId_2 ){//绿的进关
			return;
		}
		if (trackInfo.trackArr.length<=0) {
			return;
		}
		double slo = -1;
		double sla = -1;
		double sSpeed = 0;
		long sTime = 0;
		int sStatus = -1;
		double templo = 0;
		double templa = 0;
		double tempSpeed = 0;
		int tempStatus = -1;
		long tempTime = 0;
		boolean isStart = false,isIn;
		InOutCarHistoryInfo inOutCarHistoryInfo = null;
		for (int i = 0; i < trackInfo.trackArr.length; i++) {
			templo = trackInfo.trackArr[i].getDouble(TrackIO.DEST_LO_FLAG);
			templa = trackInfo.trackArr[i].getDouble(TrackIO.DEST_LA_FLAG);
			tempSpeed = trackInfo.trackArr[i].getDouble(TrackIO.DEST_SPEED_FLAG);
			tempStatus = trackInfo.trackArr[i].getInteger(TrackIO.DEST_STATUS_FLAG);
			tempTime = trackInfo.trackArr[i].getDate(TrackIO.DEST_TIME_FLAG).getTime();
			
			isIn = polygongn.contains(templo*10000000, templa*10000000);
			if(isIn){
				if(!isStart){
					isStart = true;
					slo = templo;
					sla = templa;
					sSpeed = tempSpeed;
					sStatus = tempStatus;
					sTime = tempTime;
				}
				continue;
			}else{
				if(!isStart){
					continue;
				}
			}
			if(isStart && tempTime - sTime > this.maxTime){
				inOutCarHistoryInfo = new InOutCarHistoryInfo();
				inOutCarHistoryInfo.carNo = carNo;
				inOutCarHistoryInfo.companyId = trackInfo.dInfo.companyId;
				inOutCarHistoryInfo.companyName = trackInfo.dInfo.companyName;
				inOutCarHistoryInfo.platId = trackInfo.dInfo.gpsRunComId;
				inOutCarHistoryInfo.platName = trackInfo.dInfo.gpsRunComName;
				inOutCarHistoryInfo.s_lo = slo;
				inOutCarHistoryInfo.s_la = sla;
				inOutCarHistoryInfo.e_lo = templo;
				inOutCarHistoryInfo.e_la = templa;
				inOutCarHistoryInfo.s_speed = sSpeed;
				inOutCarHistoryInfo.e_speed = tempSpeed;
				inOutCarHistoryInfo.s_status = sStatus;
				inOutCarHistoryInfo.e_status = tempStatus;
				inOutCarHistoryInfo.queryTime = new Timestamp(trackInfo.sDate.getTime());
				inOutCarHistoryInfo.outTime = new Timestamp(sTime);
				inOutCarHistoryInfo.inTime = new Timestamp(tempTime);
				inOutCarHistoryInfo.type = this.type_1;
				carInOutHistoryMapping.put(carNo+"_"+i+"_"+this.type_1, inOutCarHistoryInfo);
			}
			isStart = false;
		}
		if(isStart && tempTime - sTime > this.maxTime){
			inOutCarHistoryInfo = new InOutCarHistoryInfo();
			inOutCarHistoryInfo.carNo = carNo;
			inOutCarHistoryInfo.companyId = trackInfo.dInfo.companyId;
			inOutCarHistoryInfo.companyName = trackInfo.dInfo.companyName;
			inOutCarHistoryInfo.platId = trackInfo.dInfo.gpsRunComId;
			inOutCarHistoryInfo.platName = trackInfo.dInfo.gpsRunComName;
			inOutCarHistoryInfo.s_lo = slo;
			inOutCarHistoryInfo.s_la = sla;
			inOutCarHistoryInfo.e_lo = templo;
			inOutCarHistoryInfo.e_la = templa;
			inOutCarHistoryInfo.s_speed = sSpeed;
			inOutCarHistoryInfo.e_speed = tempSpeed;
			inOutCarHistoryInfo.s_status = sStatus;
			inOutCarHistoryInfo.e_status = tempStatus;
			inOutCarHistoryInfo.queryTime = new Timestamp(trackInfo.sDate.getTime());
			inOutCarHistoryInfo.outTime = new Timestamp(sTime);
			inOutCarHistoryInfo.inTime = new Timestamp(tempTime);
			inOutCarHistoryInfo.type = this.type_1;
			carInOutHistoryMapping.put(carNo+"_"+trackInfo.trackArr.length+"_"+this.type_1, inOutCarHistoryInfo);
		}
	}

	public void endAnalysisTrack(AnalysisServer parentServer,
			InfoContainer statInfo) {
		if(this.carInOutHistoryMapping==null){
			return;
		}
		this.pool.addTask(new SaveTask(this.carInOutHistoryMapping));
	}
	
	private class InOutCarHistoryInfo{
		public int id;
		public String carNo;
		public int companyId;
		public String companyName;
		public int platId;
		public String platName;
		public Timestamp queryTime;
		public Timestamp outTime;
		public Timestamp inTime;
		public int s_status;
		public int e_status;
		public double s_lo;
		public double s_la;
		public double e_lo;
		public double e_la;
		public double s_speed;
		public double e_speed;
		public int type;
	}
	private class SaveTask implements ITask {
		private HashMap carInOutHistoryMapping = null;

		public SaveTask(HashMap carInOutHistoryMapping) {
			this.carInOutHistoryMapping = carInOutHistoryMapping;
		}

		public boolean execute() throws Exception {
			if(this.carInOutHistoryMapping==null){
				return false;
			}
			DbHandle conn = DbServer.getSingleInstance().getConn();
			try {
				int count = 0;
				StatementHandle stmt = conn.prepareStatement("insert into car_in_out_history_info(id,car_no," +
						"company_id,company_name,plat_id,plat_name,query_time,out_time,in_time,s_status,e_status," +
						"s_lo,s_la,e_lo,e_la,s_speed,e_speed,type) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
				for (Iterator iterator = carInOutHistoryMapping.values().iterator(); iterator.hasNext();) {
					InOutCarHistoryInfo info = (InOutCarHistoryInfo) iterator.next();
					stmt.setInt(1, (int)DbServer.getSingleInstance().getAvaliableId(conn, "car_in_out_history_info", "id"));
					stmt.setString(2, info.carNo);
					stmt.setInt(3, info.companyId);
					stmt.setString(4, info.companyName);
					stmt.setInt(5, info.platId);
					stmt.setString(6, info.platName);
					stmt.setTimestamp(7, info.queryTime);
					stmt.setTimestamp(8, info.outTime);
					stmt.setTimestamp(9, info.inTime);
					stmt.setInt(10,info.s_status);
					stmt.setInt(11,info.e_status);
					stmt.setDouble(12, info.s_lo);
					stmt.setDouble(13, info.s_la);
					stmt.setDouble(14, info.e_lo);
					stmt.setDouble(15, info.e_la);
					stmt.setDouble(16, info.s_speed);
					stmt.setDouble(17, info.e_speed);
					stmt.setInt(18,info.type);
					stmt.addBatch();
					if(count%200==0){
						stmt.executeBatch();
					}
					count++;
				}
				stmt.executeBatch();
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				DbServer.getSingleInstance().releaseConn(conn);
			}
			return true;
		}

		public String getDesc() {
			return null;
		}

		public Object getFlag() {
			return null;
		}
	}
}
