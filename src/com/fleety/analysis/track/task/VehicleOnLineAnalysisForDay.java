package com.fleety.analysis.track.task;

import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;

import server.db.DbServer;

import com.fleety.analysis.AnalysisServer;
import com.fleety.analysis.track.ITrackAnalysis;
import com.fleety.analysis.track.TrackInfo;
import com.fleety.base.GeneralConst;
import com.fleety.base.InfoContainer;
import com.fleety.base.StrFilter;
import com.fleety.track.TrackIO;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;

public class VehicleOnLineAnalysisForDay implements ITrackAnalysis {
	private HashMap vehicleMapping = null;
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	private int totalPointNum = 0;
	private int totalPositionPointNum = 0;
	private int totalCarNum = 0;

	@Override
	public boolean startAnalysisTrack(AnalysisServer parentServer,
			InfoContainer statInfo) {
		vehicleMapping = null;
		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
		Date eTime = statInfo.getDate(STAT_END_TIME_DATE);

		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			StatementHandle stmt = conn.createStatement();
			StringBuilder sb = new StringBuilder();
			sb.append("select count(*) as sum from ANA_CAR_NUM_STAT")
					.append(" where STAT_TIME = to_date('")
					.append(sdf.format(sTime)).append("','yyyy-mm-dd')");
			System.out.println(sb.toString());
			ResultSet sets = stmt.executeQuery(sb.toString());
			if (sets.next()) {
				int sum=sets.getInt("sum");
				if(sum==0){
					this.vehicleMapping = new HashMap();
				}				
			}
			this.totalPointNum = 0;
			this.totalPositionPointNum = 0;
			this.totalCarNum = 0;
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
		String plateNo = trackInfo.dInfo.destNo;
		int status = 0;
		int gpsLocation = 0;
		int totalPositionPoint = 0, totalPoint = 0;
		if (!StrFilter.hasValue(plateNo)) {
			return;
		}
		totalCarNum = trackInfo.dInfo.totalCarNum;
		if (trackInfo.trackArr != null && trackInfo.trackArr.length > 0) {
			// 需要计算有效点的数量，如果有效点的数量是0，也表示全天不在线
			for (int i = 0; i < trackInfo.trackArr.length; i++) {
				status = (trackInfo.trackArr[i].getInteger(
						TrackIO.DEST_STATUS_FLAG).intValue() & 0x0f);
				gpsLocation = trackInfo.trackArr[i]
						.getInteger(TrackIO.DEST_LOCATE_FLAG);

				if (status != 3) {
					totalPoint++;
					this.totalPointNum++;
					if (gpsLocation == 0) {
						totalPositionPoint++;
						this.totalPositionPointNum++;
					}
				}
			}
			if (totalPoint > 0) {
				VehicleStatInfo info = new VehicleStatInfo();
				info.carNo = trackInfo.dInfo.destNo;
				info.mdtId = trackInfo.dInfo.mdtId;
				info.totalPointNum = totalPoint;
				info.totalPositionPointNum = totalPositionPoint;

				this.vehicleMapping.put(info.carNo, info);
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
		int id = 0;
		try {
			conn = DbServer.getSingleInstance().getConn();
			conn.setAutoCommit(false);
			VehicleStatInfo info = null;
			Iterator itr = this.vehicleMapping.keySet().iterator();
			String sql = "insert into ANA_CAR_NUM_STAT_DETAIL (id,stat_time,car_no,mdt_id,point_total_num,point_position_num) values(?,?,?,?,?,?)";
			StatementHandle psmt = conn.prepareStatement(sql);

			while (itr.hasNext()) {
				plateNo = (String) itr.next();
				info = (VehicleStatInfo) this.vehicleMapping.get(plateNo);
				id = (int) DbServer.getSingleInstance().getAvaliableId(conn,
						"ANA_CAR_NUM_STAT_DETAIL", "id");
				psmt.setInt(1, id);
				psmt.setDate(2, new java.sql.Date(sDate.getTime()));
				psmt.setString(3, plateNo);
				psmt.setInt(4, info.mdtId);
				psmt.setInt(5, info.totalPointNum);
				psmt.setInt(6, info.totalPositionPointNum);
				psmt.addBatch();
				count++;

				if (count % 200 == 0) {
					psmt.executeBatch();
				}
			}
			psmt.executeBatch();

			id = (int) DbServer.getSingleInstance().getAvaliableId(conn,
					"ANA_CAR_NUM_STAT", "id");
			sql = "insert into ANA_CAR_NUM_STAT (id,stat_time,total_car_num,total_online_car_num,total_point_num,total_point_position_num) values(?,?,?,?,?,?)";
			StatementHandle stmt = conn.prepareStatement(sql);
			stmt.setInt(1, id);
			stmt.setDate(2, new java.sql.Date(sDate.getTime()));
			stmt.setInt(3, this.totalCarNum);
			stmt.setInt(4, this.vehicleMapping.size());
			stmt.setInt(5, this.totalPointNum);
			stmt.setInt(6, this.totalPositionPointNum);
			stmt.execute();

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
		return "VehicleTrackAnalysisForDay";
	}

	private class VehicleStatInfo {
		public String carNo = "";
		public int mdtId = 0;
		public int totalPointNum = 0;
		public int totalPositionPointNum = 0;
	}
}
