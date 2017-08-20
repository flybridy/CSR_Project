package com.fleety.analysis.track.task;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import server.db.DbServer;
import server.var.VarManageServer;
import com.fleety.analysis.AnalysisServer;
import com.fleety.analysis.track.ITrackAnalysis;
import com.fleety.analysis.track.TrackInfo;
import com.fleety.base.InfoContainer;
import com.fleety.base.StrFilter;
import com.fleety.track.TrackIO;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;

/**
 * 离线判断方法：去掉不定位点和黄车点，连续两个汇报点的时间差超过设定值
 * @author admin
 *
 */
public class VehicleOffLineAnalysisForDay implements ITrackAnalysis {
	private HashMap vehicleMapping = null;
	private int duration = 30 * 60 * 1000;
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

	private int offLineAnalyStartHour = 7;
	private int offLineAnalyEndHour = 24;

	private long startTime = 0;
	private long endTime = 0;

	@Override
	public boolean startAnalysisTrack(AnalysisServer parentServer,
			InfoContainer statInfo) {
		String temp = VarManageServer.getSingleInstance().getVarStringValue(
				"off_line_duration_min");
		if (StrFilter.hasValue(temp)) {
			try {
				this.duration = Integer.parseInt(temp) * 60 * 1000;
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		temp = VarManageServer.getSingleInstance().getVarStringValue(
				"off_line_analy_start_hour");
		if (StrFilter.hasValue(temp)) {
			try {
				offLineAnalyStartHour = Integer.parseInt(temp);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		temp = VarManageServer.getSingleInstance().getVarStringValue(
				"off_line_analy_end_hour");
		if (StrFilter.hasValue(temp)) {
			try {
				offLineAnalyEndHour = Integer.parseInt(temp);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
		Date eTime = statInfo.getDate(STAT_END_TIME_DATE);

		Calendar calendar = Calendar.getInstance();
		calendar.setTime(sTime);
		calendar.set(Calendar.HOUR_OF_DAY, offLineAnalyStartHour);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		calendar.set(Calendar.MILLISECOND, 0);

		startTime = calendar.getTimeInMillis();

		calendar.set(Calendar.HOUR_OF_DAY, offLineAnalyEndHour - 1);
		calendar.set(Calendar.MINUTE, 59);
		calendar.set(Calendar.SECOND, 59);
		endTime = calendar.getTimeInMillis();

		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			StatementHandle stmt = conn.createStatement();
			StringBuilder sb = new StringBuilder();
			sb.append("select count(*) as sum from ANA_VEHICLE_OFFLINE_STAT ")
					.append(" where STAT_TIME = to_date('")
					.append(sdf.format(sTime)).append("','yyyy-mm-dd')");
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
		System.out.println("Start Analysis:" + this.toString());
		return this.vehicleMapping != null;
	}

	@Override
	public void analysisDestTrack(AnalysisServer parentServer,
			TrackInfo trackInfo) {
		if (this.vehicleMapping == null) {
			return;
		}
		/**
		 * 分析所有的轨迹，将连续两个轨迹点的时间差超过一定时间的点作为一条离线记录
		 */
		Calendar time = Calendar.getInstance();
		String plateNo = trackInfo.dInfo.destNo;
		int status = 0;
		long preTime = 0, curTime = 0;
		double preLo = 0, preLa = 0, lo = 0, la = 0;
		int gpsLocation = 0;
		int totalValidCount = 0;
		if (!StrFilter.hasValue(plateNo)) {
			return;
		}
		if (trackInfo.trackArr != null && trackInfo.trackArr.length > 0) {
			// 需要计算有效点的数量，如果有效点的数量是0，也表示全天不在线
			for (int i = 0; i < trackInfo.trackArr.length; i++) {
				time.setTime(trackInfo.trackArr[i]
						.getDate(TrackIO.DEST_TIME_FLAG));
				if (!(startTime <= time.getTimeInMillis() && time
						.getTimeInMillis() <= endTime)) {
					continue;
				}

				status = (trackInfo.trackArr[i].getInteger(
						TrackIO.DEST_STATUS_FLAG).intValue() & 0x0f);
				gpsLocation = trackInfo.trackArr[i]
						.getInteger(TrackIO.DEST_LOCATE_FLAG);
				lo = trackInfo.trackArr[i].getDouble(TrackIO.DEST_LO_FLAG);
				la = trackInfo.trackArr[i].getDouble(TrackIO.DEST_LA_FLAG);

				// 不定位和黄车不参与
				if (gpsLocation != 0 || status == 3) {
					continue;
				}
				if (totalValidCount == 0) {
					preTime = time.getTimeInMillis();
				}
				curTime = time.getTimeInMillis();
				totalValidCount++;
				if (totalValidCount > 1) {
					if (curTime - preTime > duration) {
						// 记录一条离线记录
						this.addOfflineInfo(plateNo, trackInfo, new Date(
								preTime), new Date(curTime), preLo, preLa, lo,
								la);
					}
				}
				preTime = curTime;
				preLo = lo;
				preLa = la;
			}
		}
		if (totalValidCount <= 1) {
			this.addOfflineInfo(plateNo, trackInfo, new Date(startTime),
					new Date(endTime), 0, 0, 0, 0);
		} else {
			if (endTime - curTime > duration) {
				this.addOfflineInfo(plateNo, trackInfo, new Date(curTime),
						new Date(endTime), lo, la, 0, 0);
			}

		}
	}

	@Override
	public void endAnalysisTrack(AnalysisServer parentServer,
			InfoContainer statInfo) {
		if (this.vehicleMapping == null) {
			return;
		}
		String plateNo = "";
		Date sDate = statInfo.getDate(STAT_START_TIME_DATE);
		DbHandle conn = null;
		int count = 0;
		try {
			conn = DbServer.getSingleInstance().getConn();
			conn.setAutoCommit(false);
			VehicleOfflineContainer container = null;
			VehicleOfflineInfo offlineInfo = null;
			StatementHandle stmt = conn.createStatement();
			StatementHandle psmt1 = null;
			StatementHandle psmt2 = null;
			StringBuilder deleteSql = new StringBuilder();
			deleteSql
					.append("delete from ANA_VEHICLE_OFFLINE_STAT where stat_time = to_date('")
					.append(sdf.format(sDate)).append("','yyyy-mm-dd')");
			stmt.addBatch(deleteSql.toString());
			deleteSql.delete(0, deleteSql.length());
			deleteSql
					.append("delete from ANA_VEHICLE_OFFLINE_DETAIL where stat_time = to_date('")
					.append(sdf.format(sDate)).append("','yyyy-mm-dd')");
			stmt.addBatch(deleteSql.toString());
			stmt.executeBatch();

			String sql = "";
			sql = "insert into ana_vehicle_offline_stat (id, stat_time, car_no,"
					+ "  mdt_id, taxi_company, taxi_company_name, min_offline_time,"
					+ "  min_start_time, min_end_time, max_offline_time,"
					+ "  max_start_time, max_end_time, total_times, recode_time) "
					+ " values "
					+ " (?, ?, ?, ?, ?, "
					+ "  ?, ?, ?, ?, ?,"
					+ "  ?, ?, ?, sysdate)";
			psmt1 = conn.prepareStatement(sql);
			sql = "insert into ana_vehicle_offline_detail (id, stat_time, car_no, "
					+ " mdt_id, taxi_company, taxi_company_name, offline_time, "
					+ " start_time, end_time, start_lo, start_la, end_lo, end_la, recode_time) "
					+ " values ("
					+ " ?, ?, ?, ?, ?,"
					+ " ?, ?, ?, ?, ?, "
					+ " ?, ?, ?, sysdate)";
			psmt2 = conn.prepareStatement(sql);

			for (Iterator itr = this.vehicleMapping.keySet().iterator(); itr
					.hasNext();) {
				plateNo = (String) itr.next();
				container = (VehicleOfflineContainer) this.vehicleMapping
						.get(plateNo);
				if (container == null) {
					continue;
				}
				VehicleOfflineInfo[] arr = container.getAllOfflineInfo();
				if (arr == null || arr.length == 0) {
					continue;
				}
				count++;
				int id = (int) DbServer.getSingleInstance().getAvaliableId(
						conn, "ana_vehicle_offline_stat", "id");
				// 每条统计信息记录到统计信息表
				psmt1.setInt(1, id);
				psmt1.setDate(2, new java.sql.Date(sDate.getTime()));
				psmt1.setString(3, container.minInfo.plateNo);
				psmt1.setInt(4, container.minInfo.mdtId);
				psmt1.setInt(5, container.minInfo.companyId);
				psmt1.setString(6, container.minInfo.companyName);
				psmt1.setInt(7, container.minInfo.offlineTime);
				psmt1.setTimestamp(8,
						new Timestamp(container.minInfo.startTime.getTime()));
				psmt1.setTimestamp(9,
						new Timestamp(container.minInfo.endTime.getTime()));

				psmt1.setInt(10, container.maxInfo.offlineTime);
				psmt1.setTimestamp(11, new Timestamp(
						container.maxInfo.startTime.getTime()));
				psmt1.setTimestamp(12,
						new Timestamp(container.maxInfo.endTime.getTime()));
				psmt1.setInt(13, arr.length);
				psmt1.addBatch();

				for (int i = 0; i < arr.length; i++) {
					count++;
					// 每条详情记录到详情记录表
					offlineInfo = arr[i];

					sql = "insert into ana_vehicle_offline_detail (id, stat_time, car_no, "
							+ " mdt_id, taxi_company, taxi_company_name, offline_time, "
							+ " start_time, end_time, start_lo, start_la, end_lo, end_la, recode_time) "
							+ " values ("
							+ " ?, ?, ?, ?, ?,"
							+ " ?, ?, ?, ?, ?, " + " ?, ?, ?, sysdate)";
					id = (int) DbServer.getSingleInstance().getAvaliableId(
							conn, "ANA_VEHICLE_OFFLINE_DETAIL", "id");
					psmt2.setInt(1, id);
					psmt2.setDate(2, new java.sql.Date(sDate.getTime()));
					psmt2.setString(3, offlineInfo.plateNo);
					psmt2.setInt(4, offlineInfo.mdtId);
					psmt2.setInt(5, offlineInfo.companyId);
					psmt2.setString(6, offlineInfo.companyName);
					psmt2.setInt(7, offlineInfo.offlineTime);
					psmt2.setTimestamp(8,
							new Timestamp(offlineInfo.startTime.getTime()));
					psmt2.setTimestamp(9,
							new Timestamp(offlineInfo.endTime.getTime()));

					psmt2.setDouble(10, offlineInfo.startLo);
					psmt2.setDouble(11, offlineInfo.startLa);
					psmt2.setDouble(12, offlineInfo.endLo);
					psmt2.setDouble(13, offlineInfo.endLa);
					psmt2.addBatch();
				}
				if (count % 200 == 0) {
					psmt1.executeBatch();
					psmt2.executeBatch();
				}
			}
			if (count > 0) {
				psmt1.executeBatch();
				psmt2.executeBatch();
			}
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
		System.out.println("Finish vehicle offline Analysis:" + this.toString()
				+ " recordNum=" + (count - 1));
	}

	public String toString() {
		return "VehicleOffLineAnalysisForDay";
	}

	private void addOfflineInfo(String plateNo, TrackInfo trackInfo,
			Date startTime, Date endTime, double startLo, double startLa,
			double endLo, double endLa) {
		VehicleOfflineInfo offlineInfo = null;
		VehicleOfflineContainer container = (VehicleOfflineContainer) this.vehicleMapping
				.get(plateNo);
		if (container == null) {
			container = new VehicleOfflineContainer();
		}
		offlineInfo = new VehicleOfflineInfo();
		offlineInfo.companyId = trackInfo.dInfo.companyId;
		offlineInfo.companyName = trackInfo.dInfo.companyName;
		offlineInfo.mdtId = trackInfo.dInfo.mdtId;
		offlineInfo.plateNo = trackInfo.dInfo.destNo;
		offlineInfo.startLo = startLo;
		offlineInfo.startLa = startLa;
		offlineInfo.endLo = endLo;
		offlineInfo.endLa = endLa;
		offlineInfo.startTime = startTime;
		offlineInfo.endTime = endTime;
		offlineInfo.offlineTime = (int) ((endTime.getTime() - startTime
				.getTime()) / (60 * 1000));
		container.addOfflineInfo(offlineInfo);
		this.vehicleMapping.put(plateNo, container);
	}

	private class VehicleOfflineContainer {
		private List offlineList = new ArrayList();
		private VehicleOfflineInfo minInfo = null;
		private VehicleOfflineInfo maxInfo = null;

		public void addOfflineInfo(VehicleOfflineInfo offlineInfo) {
			if (this.minInfo == null) {
				this.minInfo = offlineInfo;
				this.maxInfo = offlineInfo;
				this.offlineList.add(offlineInfo);
				return;
			}
			if (offlineInfo.offlineTime <= this.minInfo.offlineTime) {
				this.minInfo = offlineInfo;
			}
			if (offlineInfo.offlineTime >= this.maxInfo.offlineTime) {
				this.maxInfo = offlineInfo;
			}
			this.offlineList.add(offlineInfo);
		}

		public VehicleOfflineInfo[] getAllOfflineInfo() {
			VehicleOfflineInfo[] arr = new VehicleOfflineInfo[this.offlineList
					.size()];
			this.offlineList.toArray(arr);
			return arr;
		}
	}

	private class VehicleOfflineInfo {
		public int companyId = 0;
		public String companyName = null;
		public String plateNo = "";
		public int mdtId = 0;
		public int offlineTime;
		public Date startTime;
		public Date endTime;
		public double startLo = 0;
		public double startLa = 0;
		public double endLo = 0;
		public double endLa = 0;
	}
}
