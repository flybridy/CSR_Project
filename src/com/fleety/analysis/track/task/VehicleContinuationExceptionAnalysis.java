package com.fleety.analysis.track.task;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;


import org.json.JSONException;
import org.json.JSONObject;

import server.db.DbServer;
import server.var.VarManageServer;

import com.fleety.analysis.AnalysisServer;
import com.fleety.analysis.track.ITrackAnalysis;
import com.fleety.analysis.track.TrackInfo;
import com.fleety.base.GeneralConst;
import com.fleety.base.InfoContainer;
import com.fleety.base.StrFilter;
import com.fleety.track.TrackIO;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;

public class VehicleContinuationExceptionAnalysis implements ITrackAnalysis {
	private HashMap noQualifiedMapping = null;
	private HashMap speedMap = null;// 速度区段
	private int status_num = 1;// 营运状态未发生变化最大次数
	private int frequentness = 1440;// 最小上传频度
	private double valid = 0.9;// 一天内的有效点不足频度要求点数的百分比
	private long valid_time = 5 * 60 * 1000;// 判断点是否有 效的最大允许值
	private long normal_time = 1 * 60 * 1000;// 单点平均延长时长正常的最大允许值
	private int fly_num = 10;//最大允许的飞点数
	private double fly_mile = 10.0;//判断非飞点最小标准
	private int type_0 = 0;//速度不合格
	private int type_1 = 1;//状态不合格
	private int type_2 = 2;//上传频度不合格
	private int type_3 = 3;//有效点数不合格
	private int type_4 = 4;//数据实时性
	private int type_8 = 8;//飞点数不合格

	@Override
	public void analysisDestTrack(AnalysisServer parentServer,
			TrackInfo trackInfo) {
		if (this.noQualifiedMapping == null) {
			return;
		}
		if (trackInfo.trackArr == null) {
			return;
		}
		try {
			this.continuationStatusException(trackInfo);
			this.continuationFrequentnessException(trackInfo);
			this.continuationValidException(trackInfo);
			this.continuationDelayException(trackInfo);
			this.continuationSpeedException(trackInfo);
			this.continuationFlyNumException(trackInfo);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	// 速度不合格（连续点速度跨段）
	private void continuationSpeedException(TrackInfo trackInfo)
			throws Exception {
		Calendar time = Calendar.getInstance();
		time.setTime(trackInfo.sDate);
		String carNo = trackInfo.dInfo.destNo;
		if (trackInfo.trackArr.length<=0) {
			return;
		}
		SpeedSegment segment = null;
		for (int i = 0; i < trackInfo.trackArr.length; i++) {
			double speed = trackInfo.trackArr[i].getDouble(TrackIO.DEST_SPEED_FLAG);
			for (Iterator iterator = speedMap.keySet().iterator(); iterator.hasNext();) {
				Integer duan = (Integer) iterator.next();
				segment = (SpeedSegment)speedMap.get(duan);
				segment.isExist = segment.isIn(speed);
			}
		}
		for (Iterator iterator = speedMap.keySet().iterator(); iterator.hasNext();) {
			Integer duan = (Integer) iterator.next();
			segment = (SpeedSegment)speedMap.get(duan);
			if(!segment.isExist){
				NoQualifiedData noQualifiedData = new NoQualifiedData();
				noQualifiedData.carNo = carNo;
				noQualifiedData.companyId = trackInfo.dInfo.companyId;
				noQualifiedData.companyName = trackInfo.dInfo.companyName;
				noQualifiedData.runComId = trackInfo.dInfo.gpsRunComId;
				noQualifiedData.runComName = trackInfo.dInfo.gpsRunComName;
				noQualifiedData.queryTime = GeneralConst.YYYY_MM_DD.format(time
						.getTime());
				JSONObject jsonObject = new JSONObject();
				try {
					jsonObject.put("remark", "未跨区段");
				} catch (JSONException e) {
					e.printStackTrace();
				}
				noQualifiedData.remark = jsonObject.toString();
				noQualifiedData.type = this.type_0;
				noQualifiedMapping.put(carNo+"_"+noQualifiedData.type, noQualifiedData);
			}else {
				segment.isExist = false;	
			}
		}
	}

	// 营运状态不合格数据
	private void continuationStatusException(TrackInfo trackInfo) throws Exception{
		Calendar time = Calendar.getInstance();
		time.setTime(trackInfo.sDate);
		String carNo  = trackInfo.dInfo.destNo;
		if (trackInfo.trackArr.length<=0) {
			return;
		}
		int num = 0;
		int startStatus = -1;
		int endstatus = -1;
		for (int i = 0; i < trackInfo.trackArr.length; i++) {
			endstatus = trackInfo.trackArr[i].getInteger(TrackIO.DEST_STATUS_FLAG);
			endstatus = endstatus&0x0F;
			if(i>0){
				if(endstatus!=startStatus){
					num++;
				}
			}
			startStatus = endstatus;
		}
		if(num>=status_num){
			return;
		}
		NoQualifiedData noQualifiedData = new NoQualifiedData();
		noQualifiedData.carNo = carNo;
		noQualifiedData.companyId = trackInfo.dInfo.companyId;
		noQualifiedData.companyName = trackInfo.dInfo.companyName;
		noQualifiedData.runComId = trackInfo.dInfo.gpsRunComId;
		noQualifiedData.runComName = trackInfo.dInfo.gpsRunComName;
		noQualifiedData.queryTime = GeneralConst.YYYY_MM_DD.format(time.getTime());
		//营运状态改变次数：num 营运状态改变次数标准：status_num
		JSONObject jsonObject = new JSONObject();
		try {
			jsonObject.put("num", num);
			jsonObject.put("status_num", status_num);
		} catch (JSONException e) {
			e.printStackTrace();
		}
		noQualifiedData.remark = jsonObject.toString();
		noQualifiedData.type = this.type_1;
		noQualifiedMapping.put(carNo+"_"+noQualifiedData.type, noQualifiedData);
	}

	// 上传频度不合格数据
	private void continuationFrequentnessException(TrackInfo trackInfo)
			throws Exception {
		String carNo = trackInfo.dInfo.destNo;
		int sfrequentness = trackInfo.trackArr.length;

		if (sfrequentness > this.frequentness) {
			return;
		}
		Calendar time = Calendar.getInstance();
		time.setTime(trackInfo.sDate);
		NoQualifiedData noQualifiedData = new NoQualifiedData();
		noQualifiedData.carNo = carNo;
		noQualifiedData.companyId = trackInfo.dInfo.companyId;
		noQualifiedData.companyName = trackInfo.dInfo.companyName;
		noQualifiedData.runComId = trackInfo.dInfo.gpsRunComId;
		noQualifiedData.runComName = trackInfo.dInfo.gpsRunComName;
		noQualifiedData.queryTime = GeneralConst.YYYY_MM_DD.format(time
				.getTime());
		// 上传频度：+sfrequentness 上传频度标准：frequentness
		JSONObject jsonObject = new JSONObject();
		try {
			jsonObject.put("sfrequentness", sfrequentness);
			jsonObject.put("frequentness", frequentness);
		} catch (JSONException e) {
			e.printStackTrace();
		}
		noQualifiedData.remark = jsonObject.toString();
		noQualifiedData.type = this.type_2;
		noQualifiedMapping.put(carNo+"_"+noQualifiedData.type, noQualifiedData);

	}

	// 有效点不足数据
	private void continuationValidException(TrackInfo trackInfo)
			throws Exception {
		Calendar cal = Calendar.getInstance();
		long destTime = 0;
		long recordTime = 0;
		int validRecord = 0;
		String carNo = trackInfo.dInfo.destNo;
		for (int i = 0; i < trackInfo.trackArr.length; i++) {
			cal.setTime(trackInfo.trackArr[i].getDate(TrackIO.DEST_TIME_FLAG));
			destTime = cal.getTimeInMillis();
			cal.setTime(trackInfo.trackArr[i]
					.getDate(TrackIO.DEST_RECORD_TIME_FLAG));
			recordTime = cal.getTimeInMillis();
			if (recordTime - destTime > this.valid_time) {
				continue;
			}
			validRecord++;
		}
		if (validRecord > this.frequentness * valid) {
			return;
		}
		Calendar time = Calendar.getInstance();
		time.setTime(trackInfo.sDate);
		NoQualifiedData noQualifiedData = new NoQualifiedData();
		noQualifiedData.carNo = carNo;
		noQualifiedData.companyId = trackInfo.dInfo.companyId;
		noQualifiedData.companyName = trackInfo.dInfo.companyName;
		noQualifiedData.runComId = trackInfo.dInfo.gpsRunComId;
		noQualifiedData.runComName = trackInfo.dInfo.gpsRunComName;
		noQualifiedData.queryTime = GeneralConst.YYYY_MM_DD.format(time
				.getTime());
		// 有效点数：validnum 有效点个数标准：validstandard
		JSONObject jsonObject = new JSONObject();
		try {
			jsonObject.put("validnum", validRecord);
			jsonObject.put("validstandard", this.frequentness * valid);
		} catch (JSONException e) {
			e.printStackTrace();
		}
		noQualifiedData.remark = jsonObject.toString();
		noQualifiedData.type = this.type_3;
		noQualifiedMapping.put(carNo+"_"+noQualifiedData.type, noQualifiedData);
	}

	// 数据实时性
	private void continuationDelayException(TrackInfo trackInfo)
			throws Exception {
		Calendar cal = Calendar.getInstance();
		long destTime = 0;
		long recordTime = 0;
		int validRecord = 0;// 有效点数
		long delay = 0;// 延迟总时长
		String carNo = trackInfo.dInfo.destNo;
		for (int i = 0; i < trackInfo.trackArr.length; i++) {
			cal.setTime(trackInfo.trackArr[i].getDate(TrackIO.DEST_TIME_FLAG));
			destTime = cal.getTimeInMillis();
			cal.setTime(trackInfo.trackArr[i]
					.getDate(TrackIO.DEST_RECORD_TIME_FLAG));
			recordTime = cal.getTimeInMillis();
			long rec_dest = recordTime - destTime;
			if (rec_dest <= this.valid_time) {
				validRecord++;
				delay = delay + rec_dest;
			}
		}
		long avgDelay = 0;
		if (validRecord > 0) {
			avgDelay = delay / validRecord;
		}
		if (avgDelay < this.normal_time) {
			return;
		}
		Calendar time = Calendar.getInstance();
		time.setTime(trackInfo.sDate);
		NoQualifiedData noQualifiedData = new NoQualifiedData();
		noQualifiedData.carNo = carNo;
		noQualifiedData.companyId = trackInfo.dInfo.companyId;
		noQualifiedData.companyName = trackInfo.dInfo.companyName;
		noQualifiedData.runComId = trackInfo.dInfo.gpsRunComId;
		noQualifiedData.runComName = trackInfo.dInfo.gpsRunComName;
		noQualifiedData.queryTime = GeneralConst.YYYY_MM_DD.format(time
				.getTime());
		// 实时点数：validRecord 总延迟时长（毫秒）：delay单点平均延迟时长（毫秒）：avgDelay
		// 单点延迟时长标准：normal_time
		JSONObject jsonObject = new JSONObject();
		try {
			jsonObject.put("validRecord", validRecord);
			jsonObject.put("delay", delay);
			jsonObject.put("avgDelay", avgDelay);
			jsonObject.put("normal_time", this.normal_time);
		} catch (JSONException e) {
			e.printStackTrace();
		}
		noQualifiedData.remark = jsonObject.toString();
		noQualifiedData.type = this.type_4;
		noQualifiedMapping.put(carNo+"_"+noQualifiedData.type, noQualifiedData);
	}
	//飞点数不合格
	private void continuationFlyNumException(TrackInfo trackInfo) throws Exception{
		Calendar time = Calendar.getInstance();
		time.setTime(trackInfo.sDate);
		String carNo  = trackInfo.dInfo.destNo;
		if (trackInfo.trackArr.length<=0) {
			return;
		}
		int num = 0;
		double slo = -1;
		double sla = -1;
		double templo = -1;
		double templa = -1;
		for (int i = 0; i < trackInfo.trackArr.length; i++) {
			templo = trackInfo.trackArr[i].getDouble(TrackIO.DEST_LO_FLAG);
			templa = trackInfo.trackArr[i].getDouble(TrackIO.DEST_LA_FLAG);
			if(slo==-1){
				slo = templo;
				sla = templa;
				continue;
			}
			double dis = countDistance(slo,sla,templo,templa);
			if(dis > fly_mile){
				num++;
			}
			slo = templo;
			sla = templa;
		}
		if(num<=fly_num){
			return;
		}
		NoQualifiedData noQualifiedData = new NoQualifiedData();
		noQualifiedData.carNo = carNo;
		noQualifiedData.companyId = trackInfo.dInfo.companyId;
		noQualifiedData.companyName = trackInfo.dInfo.companyName;
		noQualifiedData.runComId = trackInfo.dInfo.gpsRunComId;
		noQualifiedData.runComName = trackInfo.dInfo.gpsRunComName;
		noQualifiedData.queryTime = GeneralConst.YYYY_MM_DD.format(time.getTime());
		//飞点次数：num 飞点次数标准：fly_num
		JSONObject jsonObject = new JSONObject();
		try {
			jsonObject.put("num", num);
			jsonObject.put("fly_num", fly_num);
		} catch (JSONException e) {
			e.printStackTrace();
		}
		noQualifiedData.remark = jsonObject.toString();
		noQualifiedData.type = this.type_8;
		noQualifiedMapping.put(carNo+"_"+noQualifiedData.type, noQualifiedData);
	}

	@Override
	public void endAnalysisTrack(AnalysisServer parentServer,
			InfoContainer statInfo) {
		if (this.noQualifiedMapping == null) {
			return;
		}
		int count = 0;
		String carNo = "";
		NoQualifiedData noQualifiedData;
		Date sDate = statInfo.getDate(STAT_START_TIME_DATE);
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			conn.setAutoCommit(false);
			StatementHandle stmt = conn
					.prepareStatement("insert into no_qualified_data(id,car_no,company_id,company_name,run_com_id,run_com_name,query_time,remark,record_time,type) values(?,?,?,?,?,?,?,?,?,?)");
			for (Iterator itr = this.noQualifiedMapping.keySet().iterator(); itr
					.hasNext();) {
				carNo = (String) itr.next();
				noQualifiedData = (NoQualifiedData) this.noQualifiedMapping
						.get(carNo);
				if(noQualifiedData.runComName==null||noQualifiedData.runComName.equals("")){
					continue;
				}
				stmt.setInt(1, (int) DbServer.getSingleInstance()
						.getAvaliableId(conn, "no_qualified_data", "id"));
				stmt.setString(2, noQualifiedData.carNo);
				stmt.setInt(3, noQualifiedData.companyId);
				if(noQualifiedData.companyName==null){
					stmt.setString(4, "");
				}else{
					stmt.setString(4, noQualifiedData.companyName);
				}
				
				
				stmt.setInt(5, noQualifiedData.runComId);
				stmt.setString(6, noQualifiedData.runComName);
				stmt.setDate(7, new java.sql.Date(
						GeneralConst.YYYY_MM_DD_HH_MM_SS.parse(
								noQualifiedData.queryTime + " 00:00:00")
								.getTime()));
				stmt.setString(8, noQualifiedData.remark);
				stmt.setDate(9, new java.sql.Date(new Date().getTime()));
				stmt.setInt(10, noQualifiedData.type);
				stmt.addBatch();
				if (count % 200 == 0) {
					stmt.executeBatch();
				}
				count++;
			}
			stmt.executeBatch();
			conn.commit();
		} catch (Exception e) {
			e.printStackTrace();
			if (conn != null) {
				try {
					conn.rollback();
				} catch (Exception ee) {
					ee.printStackTrace();
				}
			}
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}

	}

	@Override
	public boolean startAnalysisTrack(AnalysisServer parentServer,
			InfoContainer statInfo) {
		this.noQualifiedMapping = null;
		String temp = VarManageServer.getSingleInstance().getVarStringValue(
				"continuation_exception_max_status_num");
		if (StrFilter.hasValue(temp)) {
			try {
				this.status_num = Integer.parseInt(temp);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		temp = VarManageServer.getSingleInstance().getVarStringValue(
				"continuation_exception_min_Frequentness");
		if (StrFilter.hasValue(temp)) {
			try {
				this.frequentness = Integer.parseInt(temp);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		temp = VarManageServer.getSingleInstance().getVarStringValue(
				"continuation_exception_valid");
		if (StrFilter.hasValue(temp)) {
			try {
				this.valid = Double.parseDouble(temp);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		temp = VarManageServer.getSingleInstance().getVarStringValue("continuationSpeedArea");
		if (StrFilter.hasValue(temp)) {
			speedMap = new HashMap();
			SpeedSegment segment = null;
			try {
				String[] strArr = temp.split(",");
				for (int i = 0; i < strArr.length; i++) {
					segment = new SpeedSegment();
					String duan = strArr[i];
					String[] duanArr = duan.split("-");
					if(duanArr.length<2){
						segment.minSpeed = Double.valueOf(duanArr[0]);
						segment.maxSpeed = Double.MAX_VALUE;
					}else{
						if(duanArr[0].equals("")){
							segment.minSpeed = Double.MIN_VALUE;
							segment.maxSpeed = Double.valueOf(duanArr[1]);
						}else{
							segment.minSpeed = Double.valueOf(duanArr[0]);
							segment.maxSpeed = Double.valueOf(duanArr[1]);
						}
					}
					speedMap.put(i, segment);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		temp = VarManageServer.getSingleInstance().getVarStringValue(
				"continuation_exception_valid_time");
		if (StrFilter.hasValue(temp)) {
			try {
				this.normal_time = Integer.parseInt(temp) * 60 * 1000l;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		temp = VarManageServer.getSingleInstance().getVarStringValue(
				"continuation_exception_normal_time");
		if (StrFilter.hasValue(temp)) {
			try {
				this.normal_time = Integer.parseInt(temp) * 60 * 1000l;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		temp = VarManageServer.getSingleInstance().getVarStringValue(
				"continuation_exception_fly_num");
		if (StrFilter.hasValue(temp)) {
			try {
				this.fly_num = Integer.parseInt(temp);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		temp = VarManageServer.getSingleInstance().getVarStringValue(
				"continuation_exception_fly_mile");
		if (StrFilter.hasValue(temp)) {
			try {
				this.fly_mile = Double.parseDouble(temp);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
		Date eTime = statInfo.getDate(STAT_END_TIME_DATE);

		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			StringBuilder sb = new StringBuilder();
			sb.append("select count(*) as sum from no_qualified_data ").append(
					" where query_time>=? and query_time<?"+
					" and type <= 4");
			StatementHandle stmt = conn.prepareStatement(sb.toString());
			stmt.setTimestamp(1, new Timestamp(sTime.getTime()));
			stmt.setTimestamp(2, new Timestamp(sTime.getTime()+GeneralConst.ONE_DAY_TIME));
			ResultSet sets = stmt.executeQuery();
			if (sets.next()) {
				int sum = sets.getInt("sum");
				if (sum == 0)
					this.noQualifiedMapping = new HashMap();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}

		if (this.noQualifiedMapping == null) {
			System.out.println("Not Need Analysis:" + this.toString());
		} else {
			System.out.println("Start Analysis:" + this.toString());
		}
		return this.noQualifiedMapping != null;
	}

	private class NoQualifiedData {
		public int id;
		public String carNo;
		public int companyId;
		public String companyName;
		public int runComId;
		public String runComName;
		public String queryTime;
		public int type;
		public String remark;
		public String recordTime;
	}
	
	public class SpeedSegment{
		public double minSpeed = 0;
		public double maxSpeed = 0;
		public boolean isExist = false;
		
		public boolean isIn(double speed){
			if(this.isExist){
				return this.isExist;
			}
			this.isExist =  speed >= this.minSpeed && speed <= this.maxSpeed;
			
			return this.isExist;
		}
	}
	public static double countDistance(double lo1, double la1, double lo2,double la2)
	{
		double radLat1 = rad(la1);
		double radLat2 = rad(la2);
		double a = radLat1 - radLat2;
		double b = rad(lo1) - rad(lo2);
		double s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2), 2)
				+ Math.cos(radLat1) * Math.cos(radLat2)
				* Math.pow(Math.sin(b / 2), 2)));
		s = s * EARTH_RADIUS;
		s = Math.round(s * 100000) / 100000.0;
		return s;
	}
	private static double EARTH_RADIUS = 6378.137;
	private static double rad(double d)
	{
		return d * Math.PI / 180.0;
	}
}
