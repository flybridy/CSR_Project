package com.fleety.analysis.track.task;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import server.cluster.GISMarkClusterInstance;
import server.cluster.GISMarkClusterInstance.Cluster;
import server.db.DbServer;
import server.var.VarManageServer;
import com.fleety.analysis.AnalysisServer;
import com.fleety.analysis.track.DestInfo;
import com.fleety.analysis.track.ITrackAnalysis;
import com.fleety.analysis.track.TrackInfo;
import com.fleety.base.GeneralConst;
import com.fleety.base.InfoContainer;
import com.fleety.base.StrFilter;
import com.fleety.server.GlobalUtilServer;
import com.fleety.track.TrackIO;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;

public class VehicleStopAnalysis implements ITrackAnalysis {
	private List vehicleList = null;

	private long startTime = 0;
	private long endTime = 0;
	private int maxSpeed = 5;
	private int distance = 500;

	private int duration = 120 * 60 * 1000;

	@Override
	public boolean startAnalysisTrack(AnalysisServer parentServer,
			InfoContainer statInfo) {
		int startHour = 7;
		int endHour = 24;
		String temp = VarManageServer.getSingleInstance().getVarStringValue(
				"stop_analy_start_hour");
		if (StrFilter.hasValue(temp)) {
			try {
				startHour = Integer.parseInt(temp);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		temp = VarManageServer.getSingleInstance().getVarStringValue(
				"stop_analy_end_hour");
		if (StrFilter.hasValue(temp)) {
			try {
				endHour = Integer.parseInt(temp);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		// TODO Auto-generated method stub
		Date sDate = statInfo.getDate(ITrackAnalysis.STAT_START_TIME_DATE);
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(sDate);
		calendar.set(Calendar.HOUR_OF_DAY, startHour);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		calendar.set(Calendar.MILLISECOND, 0);
		startTime = calendar.getTimeInMillis();
		calendar.set(Calendar.HOUR_OF_DAY, endHour - 1);
		calendar.set(Calendar.MINUTE, 59);
		calendar.set(Calendar.SECOND, 59);
		endTime = calendar.getTimeInMillis();

		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			StatementHandle stmt = conn.createStatement();
			StringBuilder sb = new StringBuilder();
			sb.append("select count(*) as sum from ANA_VEHICLE_STOP_STAT ")
					.append(" where stat_time = to_date('")
					.append(GeneralConst.YYYY_MM_DD.format(sDate))
					.append("','yyyy-mm-dd')");
			ResultSet sets = stmt.executeQuery(sb.toString());
			if (sets.next()) {
				int sum = sets.getInt("sum");
				if (sum == 0) {
					this.vehicleList = new ArrayList();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}

		if (this.vehicleList == null) {
			System.out.println("Not Need Analysis:" + this.toString());
		} else {
			System.out.println("Start Analysis:" + this.toString());
		}
		return this.vehicleList != null;
	}

	public String toString() {
		return this.getClass().getName();
	}

	@Override
	public void analysisDestTrack(AnalysisServer parentServer,
			TrackInfo trackInfo) {
		// TODO Auto-generated method stub
		if (trackInfo.trackArr == null) {
			return;
		}
		/**
		 * 现将所有位置汇报进行分段，分成每个停驶段列表，然后对每个列表进行分析
		 */
		Calendar time = Calendar.getInstance();
		int speed = 0;
		int status = 0;
		int gpsLocation = 0;
		int totalLen = trackInfo.trackArr.length;
		HashMap pointListMap = new HashMap();
		List pointList = null;
		int count = 0;
		for (int i = 0; i < trackInfo.trackArr.length; i++) {
			time.setTime(trackInfo.trackArr[i].getDate(TrackIO.DEST_TIME_FLAG));
			if (time.getTimeInMillis() < startTime
					|| time.getTimeInMillis() > endTime) {
				continue;
			}
			status = (trackInfo.trackArr[i]
					.getInteger(TrackIO.DEST_STATUS_FLAG).intValue() & 0x0f);
			gpsLocation = trackInfo.trackArr[i]
					.getInteger(TrackIO.DEST_LOCATE_FLAG);
			// 不定位和黄车不参与
			if (gpsLocation != 0 || status == 3) {
				continue;
			}

			speed = trackInfo.trackArr[i].getInteger(TrackIO.DEST_SPEED_FLAG);
			if (speed <= this.maxSpeed) {
				if (pointList == null) {
					pointList = new ArrayList();
					count++;
					pointListMap.put(count, pointList);
				}
				pointList.add(trackInfo.trackArr[i]);
			} else {
				if (pointList == null) {
					continue;
				}
				// 得到后续两个点，如果有后续两点速度都大于5，那就要重新开始
				boolean isStop = false;
				if (i < (totalLen - 2)) {
					speed = trackInfo.trackArr[i + 1]
							.getInteger(TrackIO.DEST_SPEED_FLAG);
					if (speed > this.maxSpeed) {
						speed = trackInfo.trackArr[i + 2]
								.getInteger(TrackIO.DEST_SPEED_FLAG);
						if (speed > this.maxSpeed) {
							isStop = true;
						}
					}
				}
				if (isStop) {
					pointList = null;
				} else {
					if (pointList != null) {
						pointList.add(trackInfo.trackArr[i]);
					}
				}
			}
		}

		if (pointListMap.size() > 0) {
			Iterator itr = pointListMap.values().iterator();
			LocalCluster localCuster = null;
			while (itr.hasNext()) {
				pointList = (ArrayList) itr.next();
				if (pointList != null) {
					InfoContainer[] infos = new InfoContainer[pointList.size()];
					pointList.toArray(infos);
					localCuster = this.judgeStop(infos, trackInfo.dInfo.destNo);
					if (localCuster != null) {
						this.vehicleList.add(localCuster);
					}
				}

			}
		}
	}

	@Override
	public void endAnalysisTrack(AnalysisServer parentServer,
			InfoContainer statInfo) {
		if (this.vehicleList == null) {
			return;
		}
		this.insertDb(this.vehicleList);

		System.out.println("Finish Analysis:" + this.toString() + " recordNum="
				+ (this.vehicleList.size()));
	}

	private LocalCluster judgeStop(InfoContainer[] infos, String destNo) {
		if (infos == null || infos.length == 0) {
			return null;
		}
		boolean isStop = true;

		Date firstTime = infos[0].getDate(TrackIO.DEST_TIME_FLAG);
		Date lastTime = infos[infos.length - 1].getDate(TrackIO.DEST_TIME_FLAG);
		if (lastTime.getTime() - firstTime.getTime() < this.duration) {
			return null;
		}
			
		GISMarkClusterInstance clusterServer = new GISMarkClusterInstance(
				(int) Math.round(distance / 0.9), true);
		clusterServer.setScaleLevel(16);
		LocalCluster localCluster = null;
		int validTotalPointNum = 0;
		double lo = 0;
		double la = 0;
		for (int i = 0; i < infos.length; i++) {
			Date reportTime = infos[i].getDate(TrackIO.DEST_TIME_FLAG);
			int position = infos[i].getInteger(TrackIO.DEST_LOCATE_FLAG)
					.intValue();
			if (position != 0) {
				continue;
			}
			lo = infos[i].getDouble(TrackIO.DEST_LO_FLAG);
			la = infos[i].getDouble(TrackIO.DEST_LA_FLAG);
			clusterServer.addPoint(reportTime.getTime() + "", lo, la);
			validTotalPointNum++;
		}
		List<Cluster> list = clusterServer.getClusterPoint();
		Cluster tempCluster = null;
		int clusterMaxPointNum = 0;
		if (list != null && list.size() > 0) {
			for (int i = 0; i < list.size(); i++) {
				tempCluster = list.get(i);
				if (tempCluster.pList.size() > clusterMaxPointNum) {
					clusterMaxPointNum = tempCluster.pList.size();
					localCluster = new LocalCluster();
					localCluster.cla = tempCluster.cla;
					localCluster.clo = tempCluster.clo;
				}
			}
			if (clusterMaxPointNum < validTotalPointNum * 0.95) {
				isStop = false;
			}
		}
		clusterServer.clear();
		clusterServer = null;
		if(Boolean.parseBoolean(VarManageServer.getSingleInstance().getVarStringValue("stop_valid_point_num"))){
			if(validTotalPointNum<(lastTime.getTime() - firstTime.getTime())/(2*60*1000)){
				return null;
			}
		}	
		if (isStop && localCluster != null) {
			localCluster.destNo = destNo;
			localCluster.startTime = firstTime;
			localCluster.endTime = lastTime;
			return localCluster;
		} else {
			return null;
		}
	}

	private void insertDb(List insertList) {
		DbHandle conn = null;
		try {
			conn = DbServer.getSingleInstance().getConn();
			String sql = "insert into ANA_VEHICLE_STOP_STAT "
					+ " (id, car_no, mdt_id, type_id, taxi_company,"
					+ "  taxi_company_name, START_TIME, END_TIME,STOP_TIME,START_LO,START_LA,"
					+ "  stat_time, recode_time) " + " values "
					+ " (?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, sysdate)";
			StatementHandle stmt = conn.prepareStatement(sql);

			DestInfo destInfo = null;
			LocalCluster localCuster = null;
			int id = 0;
			for (int i = 0; i < insertList.size(); i++) {
				localCuster = (LocalCluster) insertList.get(i);
				destInfo = GlobalUtilServer.getDestInfo(localCuster.destNo);
				if (destInfo == null) {
					continue;
				}
				id = (int) DbServer.getSingleInstance().getAvaliableId(conn,
						"ANA_VEHICLE_STOP_STAT", "id");
				stmt.setInt(1, id);
				stmt.setString(2, destInfo.destNo);
				stmt.setInt(3, destInfo.mdtId);
				stmt.setInt(4, destInfo.carType);
				stmt.setInt(5, destInfo.companyId);
				stmt.setString(6, destInfo.companyName);
				stmt.setTimestamp(7,
						new Timestamp(localCuster.startTime.getTime()));
				stmt.setTimestamp(8,
						new Timestamp(localCuster.endTime.getTime()));
				stmt.setInt(9, localCuster.getStopTime());
				stmt.setDouble(10, localCuster.clo);
				stmt.setDouble(11, localCuster.cla);
				stmt.setDate(12,
						new java.sql.Date(localCuster.startTime.getTime()));
				stmt.addBatch();
				if ((i + 1) % 200 == 0) {
					stmt.executeBatch();
				}
			}
			stmt.executeBatch();
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}
	}

	public class LocalCluster {
		public String destNo = "";
		public double clo = 0;
		public double cla = 0;
		public Date startTime = null;
		public Date endTime = null;

		public int getStopTime() {
			if (this.startTime == null || this.endTime == null) {
				return 0;
			} else {
				return (int) ((this.endTime.getTime() - this.startTime
						.getTime()) / (60 * 1000));
			}
		}
	}
}
