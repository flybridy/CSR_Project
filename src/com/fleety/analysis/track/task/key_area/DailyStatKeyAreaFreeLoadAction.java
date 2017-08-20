package com.fleety.analysis.track.task.key_area;

import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
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

public class DailyStatKeyAreaFreeLoadAction implements ITrackAnalysis {

	private int preTime = 5;// 单位分钟
	private HashMap resultMap = null;
	private HashMap areaMap = null;

	private final static int STATUS_FREE = 0; // 空车
	private final static int STATUS_LOAD = 1; // 重车
	
	public final static int TYPE_RED=1;
	public static int TYPE_GREEN=2;
	public final static int TYPE_BLUE=3;

	public void init() {
		this.preTime = YesterdayTrackAnalysisServer.getSingleInstance().preTime;
		areaMap = KeyAreaInfoUtil.getAreaInfo();
		
		String temp=VarManageServer.getSingleInstance().getVarStringValue("type_greed_id");
		if(StrFilter.hasValue(temp)){
			TYPE_GREEN=Integer.parseInt(temp);
		}
	}

	public boolean startAnalysisTrack(AnalysisServer parentServer,
			InfoContainer statInfo) {
		this.init();
		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
		Date eTime = statInfo.getDate(STAT_END_TIME_DATE);
		this.resultMap = null;
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			StatementHandle pstmt = conn
					.prepareStatement("select * from key_area_car_stat where stat_time between ? and ?");
			pstmt.setDate(1, new java.sql.Date(sTime.getTime() + 1000));
			pstmt.setDate(2, new java.sql.Date(eTime.getTime()));
			ResultSet sets = pstmt.executeQuery();
			if (!sets.next()) {
				resultMap = new HashMap();
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
		AreaStatCarInfo areaStatInfo;
		String carNo = trackInfo.dInfo.destNo;
		for (int i = 0; i < trackInfo.trackArr.length; i++) {
			info = trackInfo.trackArr[i];
			lo = info.getDouble(TrackIO.DEST_LO_FLAG);
			la = info.getDouble(TrackIO.DEST_LA_FLAG);

			info.setInfo("DEST_NO", carNo);

			gpsLocation = info.getInteger(TrackIO.DEST_LOCATE_FLAG);
			curStatus = info.getInteger(TrackIO.DEST_STATUS_FLAG) & 0xf;
			if (curStatus != STATUS_FREE && curStatus != STATUS_LOAD) {
				continue;
			}
			if (gpsLocation != 0) {
				continue;
			}

			it = areaMap.keySet().iterator();
			KeyAreaInfo keyAreaInfo = null;
			Integer areaId = null;
			while (it.hasNext()) {
				areaId = (Integer) it.next();
				keyAreaInfo = (KeyAreaInfo) areaMap.get(areaId);
				if (keyAreaInfo.isInArea(lo, la)) {
					areaStatInfo = (AreaStatCarInfo) resultMap.get(areaId);
					if (areaStatInfo == null) {
						areaStatInfo = new AreaStatCarInfo();
						areaStatInfo.areaId = keyAreaInfo.getAreaId();
						areaStatInfo.areaName = keyAreaInfo.getCname();
						areaStatInfo.preTime = this.preTime;
						resultMap.put(keyAreaInfo.getAreaId(), areaStatInfo);
					}
					areaStatInfo.trackInfo.add(info);
				}
			}
		}

		it = resultMap.keySet().iterator();
		while (it.hasNext()) {
			areaStatInfo = (AreaStatCarInfo) resultMap.get(it.next());
			areaStatInfo.stat(trackInfo.dInfo);
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
		System.out
				.println("DailyStatKeyAreaFreeLoadAction log db resultMap size:"
						+ resultMap.size());
		Iterator it1 = resultMap.keySet().iterator();
		AreaStatCarInfo areaStatInfo = null;
		while (it1.hasNext()) {
			areaStatInfo = (AreaStatCarInfo) resultMap.get(it1.next());
			areaStatInfo.statAvg();
		}
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DAY_OF_MONTH, -1);

		DbHandle conn = DbServer.getSingleInstance().getConn();
		String sql = "insert into key_area_car_stat(id,area_id,area_name,stat_time,hour,all_num,free_num,"
				+ "load_num,f2l_num,f2l_detail,l2f_num,l2f_detail,totalin_num,totalin_detail)values(?,?,?,?,?,?,?,?,?,empty_blob()"
				+ ",?,empty_blob(),?,empty_blob())";

		String sqlInOut = "insert into ana_key_area_car_inout_stat(id, area_id,area_name, hour,"
				+ " free_in, free_out, load_in, load_out,"
				+ " stat_time, create_time, car_type,red_free_in,red_free_out,red_load_in,red_load_out,green_free_in,green_free_out,green_load_in,green_load_out) "
				+ " values (?, ?,?, ?, ?, ?, ?, ?, ?, sysdate, 1,?,?,?,?,?,?,?,?)";

		try {
			StatementHandle pstmt = conn.prepareStatement(sql);
			StatementHandle pstmt1 = conn.prepareStatement(sqlInOut);
			Iterator it = resultMap.keySet().iterator();
			AreaStatCarInfo areaStatCarInfo = null;
			HashMap map = new HashMap();
			while (it.hasNext()) {
				areaStatCarInfo = (AreaStatCarInfo) resultMap.get(it.next());
				Iterator itr = areaStatCarInfo.periodMap.keySet().iterator();
				PeriodStat periodStat = null;
				while (itr.hasNext()) {
					periodStat = (PeriodStat) areaStatCarInfo.periodMap.get(itr
							.next());
					long id = DbServer.getSingleInstance().getAvaliableId(conn,
							"key_area_car_stat", "id");
					pstmt.setLong(1, id);
					pstmt.setInt(2, areaStatCarInfo.areaId);
					pstmt.setString(3, areaStatCarInfo.areaName);
					pstmt.setDate(
							4,
							new java.sql.Date(sTime.getTime()));
					pstmt.setInt(5, periodStat.period);
					pstmt.setInt(6, periodStat.avgTotalCarNum);
					pstmt.setInt(7, periodStat.avgFreeCarNum);
					pstmt.setInt(8, periodStat.avgLoadCarNum);
					pstmt.setInt(9, periodStat.totalF2LNum);
					pstmt.setInt(10, periodStat.totalL2FNum);
					pstmt.setInt(11, periodStat.totalInNum);
					pstmt.addBatch();

					long id1 = (int) DbServer.getSingleInstance()
							.getAvaliableId(conn,
									"ana_key_area_car_inout_stat", "id");
					pstmt1.setLong(1, id1);
					pstmt1.setInt(2, areaStatCarInfo.areaId);
					pstmt1.setString(3, areaStatCarInfo.areaName);
					pstmt1.setInt(4, periodStat.period);
					pstmt1.setInt(5, periodStat.totalFreeInNum);
					pstmt1.setInt(6, periodStat.totalFreeOutNum);
					pstmt1.setInt(7, periodStat.totalLoadInNum);
					pstmt1.setInt(8, periodStat.totalLoadOutNum);
					pstmt1.setDate(
							9,
							new java.sql.Date(sTime.getTime()));
					pstmt1.setInt(10, periodStat.totalFreeInRedNum);
					pstmt1.setInt(11, periodStat.totalFreeOutRedNum);
					pstmt1.setInt(12, periodStat.totalLoadInRedNum);
					pstmt1.setInt(13, periodStat.totalLoadOutRedNum);
					pstmt1.setInt(14, periodStat.totalFreeInGreenNum);
					pstmt1.setInt(15, periodStat.totalFreeOutGreenNum);
					pstmt1.setInt(16, periodStat.totalLoadInGreenNum);
					pstmt1.setInt(17, periodStat.totalLoadOutGreenNum);
					pstmt1.addBatch();

					map.put(id, periodStat);
				}
			}
			pstmt.executeBatch();
			pstmt1.executeBatch();
			try {
				conn.setAutoCommit(false);
				pstmt = conn
						.prepareStatement("select f2l_detail,l2f_detail,totalin_detail from key_area_car_stat where id=? for update");
				it = map.keySet().iterator();
				ResultSet sets;
				BLOB blob1, blob2, blob3;
				OutputStream out1 = null, out2 = null, out3 = null;
				long id = 0;
				while (it.hasNext()) {
					id = (Long) it.next();
					PeriodStat periodStat = (PeriodStat) map.get(id);
					pstmt.setLong(1, id);
					sets = pstmt.executeQuery();
					if (sets.next()) {
						blob1 = (BLOB) sets.getBlob(1);
						blob2 = (BLOB) sets.getBlob(2);
						blob3 = (BLOB) sets.getBlob(3);
						byte[] b1 = periodStat.detailF2L.getBytes();
						byte[] b2 = periodStat.detailL2F.getBytes();
						byte[] b3 = periodStat.detailTotalIn.getBytes();

						out1 = blob1.getBinaryOutputStream();
						out1.write(b1);
						out1.close();

						out2 = blob2.getBinaryOutputStream();
						out2.write(b2);
						out2.close();

						out3 = blob3.getBinaryOutputStream();
						out3.write(b3);
						out3.close();
					}
				}
				if (out1 != null) {
					out1.close();
				}
				if (out2 != null) {
					out2.close();
				}
				if (out3 != null) {
					out3.close();
				}
				conn.commit();
			} catch (Exception e) {
				try {
					conn.rollback();
				} catch (Exception ex) {
					ex.printStackTrace();
				}
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println("DailyStatKeyAreaFreeLoadAction end log db!");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}
	}
}
