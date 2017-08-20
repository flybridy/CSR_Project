package com.fleety.analysis.track.task.overspeed_area;

import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;

import oracle.sql.BLOB;

import com.fleety.analysis.AnalysisServer;
import com.fleety.analysis.track.ITrackAnalysis;
import com.fleety.analysis.track.TrackInfo;
import com.fleety.analysis.track.YesterdayTrackAnalysisServer;
import com.fleety.base.GeneralConst;
import com.fleety.base.InfoContainer;
import com.fleety.base.StrFilter;
import com.fleety.track.TrackIO;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;

import server.db.DbServer;
import server.var.VarManageServer;

public class DailyStatOverSpeedAreaAction implements ITrackAnalysis {

	private int preTime = 5;// 单位分钟
	private HashMap resultMap = null;   //存放分析结果，方案id+车牌号做key
	private HashMap schemeMap = null;    //存放需要分析的方案，id做key,OverSpeedSchemeInfo做value

	private final static int STATUS_FREE = 0; // 空车
	private final static int STATUS_LOAD = 1; // 重车

	
	public void init() {
		this.preTime = YesterdayTrackAnalysisServer.getSingleInstance().preTime;  //相邻两个点间隔的最大时间，超过这个时间，认为是两次在区域内的轨迹
//		areaMap = OverSpeedAreaInfoUtil.getAreaInfo();
		schemeMap = OverSpeedAreaInfoUtil.getOverSpeedSchemeInfo();

	}

	public boolean startAnalysisTrack(AnalysisServer parentServer,
			InfoContainer statInfo) {
		this.init();
//		statInfo 是从父类传过来的统计当天的开始时间和结束时间，不是方案的开始和结束时间
		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
		Date eTime = statInfo.getDate(STAT_END_TIME_DATE);
		this.resultMap = null;
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			StatementHandle pstmt = conn
					.prepareStatement("select * from ANA_OVERSPEED_AREA_MONITOR where stat_time between ? and ?");
			pstmt.setDate(1, new java.sql.Date(sTime.getTime() + 1000));
			pstmt.setDate(2, new java.sql.Date(eTime.getTime()));
			ResultSet sets = pstmt.executeQuery();
			if (!sets.next()) {
				if(schemeMap==null || schemeMap.size()==0){
					
				}else{
					//有需要分析的方案
					resultMap = new HashMap();
				}
				
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}
		if (this.resultMap == null) {
			System.out.println("Not Need Analysis:" + this.toString());
		} else {
			System.out.println("Start Analysis:" + this.toString());
		}
		return resultMap != null;
	}

	public void analysisDestTrack(AnalysisServer parentServer,
			TrackInfo trackInfo) {
		if (this.resultMap == null) {
			return;
		}
		if (trackInfo == null) {
			return;
		}

		InfoContainer info = null;
		int lastStatus = -1, curStatus = -1;
		double lo, la;
		int gpsLocation = 0;
		Iterator it;
		SchemeStatCarInfo schemeStatInfo;
		String carNo = trackInfo.dInfo.destNo;
		for (int i = 0; i < trackInfo.trackArr.length; i++) {
			info = trackInfo.trackArr[i];

			info.setInfo("DEST_NO", carNo);

			gpsLocation = info.getInteger(TrackIO.DEST_LOCATE_FLAG);
			curStatus = info.getInteger(TrackIO.DEST_STATUS_FLAG) & 0xf;
			if (curStatus != STATUS_FREE && curStatus != STATUS_LOAD) {
				continue;
			}
			if (gpsLocation != 0) {
				continue;
			}

			it = schemeMap.keySet().iterator();
			OverSpeedSchemeInfo scheme = null;
			
//			OverSpeedAreaInfo keyAreaInfo = null;
			Integer schemeId = null;
			while (it.hasNext()) {
				
				schemeId = (Integer) it.next();
				String resultMap_key = schemeId+carNo;  //方案id+车牌号
				scheme = (OverSpeedSchemeInfo) schemeMap.get(schemeId);
				if (scheme.isInScheme(info)) {
					schemeStatInfo = (SchemeStatCarInfo) resultMap.get(resultMap_key);
					if (schemeStatInfo == null) {
						schemeStatInfo = new SchemeStatCarInfo();
						schemeStatInfo.schemeId=schemeId;
						schemeStatInfo.schemeName=scheme.getScheme_name();
						schemeStatInfo.comId=trackInfo.dInfo.companyId;
						schemeStatInfo.comName=trackInfo.dInfo.companyName;
						schemeStatInfo.dest_no=trackInfo.dInfo.destNo;
						schemeStatInfo.preTime = this.preTime;
						schemeStatInfo.overSpeed=scheme.getOverSpeed();
						resultMap.put(resultMap_key, schemeStatInfo);
						
					}
					schemeStatInfo.trackInfo.add(info);  //方案内的所有轨迹点
				}
			}
		}

		it = resultMap.keySet().iterator();
		while (it.hasNext()) {
			schemeStatInfo = (SchemeStatCarInfo) resultMap.get(it.next());
			schemeStatInfo.stat(trackInfo.dInfo);
		}

	}

	public void endAnalysisTrack(AnalysisServer parentServer,
			InfoContainer statInfo) {
		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
		this.logStatToDB(resultMap,sTime);
	}

	private void logStatToDB(HashMap resultMap,Date sTime) {
		if (resultMap == null) {
			return;
		}
		//walter
		System.out
				.println("DailyStatOverSpeedAreaAction log db resultMap size:"
						+ resultMap.size());

		DbHandle conn = DbServer.getSingleInstance().getConn();
		String sql = "insert into ANA_OVERSPEED_AREA_MONITOR(ID,SCHEME_ID,SCHEME_NAME,COM_ID,COM_NAME,DEST_NO,FIRST_STATUS,FIRST_TIME,FIRST_LO,FIRST_LA,STAT_TIME,FIRST_SPEED,"
				+ "LAST_STATUS,LAST_TIME,LAST_LO,LAST_LA,LAST_SPEED,MAX_SPEED,MIN_SPEED,AVG_SPEED)"
				+" values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)" ;
		
		try {
			StatementHandle pstmt = conn.prepareStatement(sql);
			Iterator it = resultMap.keySet().iterator();
			SchemeStatCarInfo schemeStatCarInfo = null;
			
			while (it.hasNext()) {
				schemeStatCarInfo = (SchemeStatCarInfo) resultMap.get(it.next());
				Iterator itr = schemeStatCarInfo.periodMap.keySet().iterator();
				PeriodStat periodStat = null;
				while (itr.hasNext()){ 
					periodStat = (PeriodStat) schemeStatCarInfo.periodMap.get(itr
							.next());
					long id = DbServer.getSingleInstance().getAvaliableId(conn,
							"ANA_OVERSPEED_AREA_MONITOR", "id");
					pstmt.setLong(1, id);
					pstmt.setInt(2, schemeStatCarInfo.schemeId);
					pstmt.setString(3, schemeStatCarInfo.schemeName);
					pstmt.setInt(4, schemeStatCarInfo.comId);
					pstmt.setString(5, schemeStatCarInfo.comName);
					pstmt.setString(6, schemeStatCarInfo.dest_no);
					pstmt.setInt(7, periodStat.firstStatus);
					pstmt.setTimestamp(8, new Timestamp(periodStat.firstTime.getTime()));
					pstmt.setDouble(9, periodStat.firstlo);
					pstmt.setDouble(10, periodStat.firstla);
					pstmt.setDate(
							11,
							new java.sql.Date(sTime.getTime()));
					pstmt.setInt(12, periodStat.firstSpeed);
					
					pstmt.setInt(13, periodStat.lastStatus);
					pstmt.setTimestamp(14, new Timestamp(periodStat.lastTime.getTime()));
					pstmt.setDouble(15, periodStat.lastlo);
					pstmt.setDouble(16, periodStat.lastla);
					pstmt.setInt(17, periodStat.lastSpeed);
					pstmt.setInt(18, periodStat.maxSpeed);
					pstmt.setInt(19, periodStat.minSpeed);
					pstmt.setDouble(20, periodStat.avgSpeed);
					pstmt.addBatch();
				}
			}
			pstmt.executeBatch();
			System.out.println("DailyStatOverSpeedAreaAction end log db!");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}
	}
}
