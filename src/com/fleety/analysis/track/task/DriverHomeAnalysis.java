package com.fleety.analysis.track.task;

import java.io.OutputStream;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import oracle.sql.BLOB;
import server.cluster.GISMarkClusterInstance;
import server.cluster.GISMarkClusterInstance.Cluster;
import server.cluster.GISMarkClusterInstance.PointInfo;
import server.db.DbServer;
import com.fleety.analysis.AnalysisServer;
import com.fleety.analysis.track.ITrackAnalysis;
import com.fleety.analysis.track.TrackInfo;
import com.fleety.base.InfoContainer;
import com.fleety.base.StrFilter;
import com.fleety.track.TrackIO;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;

public class DriverHomeAnalysis implements ITrackAnalysis {
	private HashMap vehicleMapping = null;
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	private SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	private int startHour = 0;
	private int endHour = 5;
	private long startTime = 0;
	private long endTime = 0;
	private int distanceScope = 500;
	private int minPointNum = 10;

	@Override
	public boolean startAnalysisTrack(AnalysisServer parentServer,
			InfoContainer statInfo) {
		// TODO Auto-generated method stub
		Date sDate = statInfo.getDate(ITrackAnalysis.STAT_START_TIME_DATE);
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(sDate);
		calendar.set(Calendar.HOUR_OF_DAY, startHour);
		startTime = calendar.getTimeInMillis();

		calendar.set(Calendar.HOUR_OF_DAY, endHour);
		endTime = calendar.getTimeInMillis();

		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			StatementHandle stmt = conn.createStatement();
			StringBuilder sb = new StringBuilder();
			sb.append("select count(*) as sum from ana_driver_home_stat ")
					.append(" where stat_time = to_date('")
					.append(sdf.format(sDate)).append("','yyyy-mm-dd')");
			ResultSet sets = stmt.executeQuery(sb.toString());
			if (sets.next()) {
				int sum = sets.getInt("sum");
				if (sum == 0) {
					this.vehicleMapping = new HashMap();
				}
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
		return this.vehicleMapping != null;
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
		 * 每辆车按照轨迹时间顺序计算过去,在凌晨1-5点速度小于10并且位置不变化的点
		 */
		Calendar time = Calendar.getInstance();
		int speed = 0;
		double lo = 0, la = 0;
		GISMarkClusterInstance clusterServer = new GISMarkClusterInstance(
				(int) Math.round(this.distanceScope / 0.9), true);
		clusterServer.setScaleLevel(16);
		for (int i = 0; i < trackInfo.trackArr.length; i++) {
			time.setTime(trackInfo.trackArr[i].getDate(TrackIO.DEST_TIME_FLAG));
			if (time.getTimeInMillis() < startTime
					|| time.getTimeInMillis() > endTime) {
				continue;
			}
			speed = trackInfo.trackArr[i].getInteger(TrackIO.DEST_SPEED_FLAG);
			if (speed > 0) {
				continue;
			}
			lo = trackInfo.trackArr[i].getDouble(TrackIO.DEST_LO_FLAG);
			la = trackInfo.trackArr[i].getDouble(TrackIO.DEST_LA_FLAG);

			clusterServer.addPoint(time.getTimeInMillis() + "", lo, la);
		}
		List<Cluster> list = clusterServer.getClusterPoint();
		Cluster tempCluster = null;
		LocalCluster localCluster = null;
		int count = 0;
		if (list != null && list.size() > 0) {
			for (int i = 0; i < list.size(); i++) {
				tempCluster = list.get(i);
				if (tempCluster.pList.size() > count) {
					count = tempCluster.pList.size();
					localCluster = new LocalCluster();
					localCluster.cla=tempCluster.cla;
					localCluster.clo=tempCluster.clo;
				}
			}
		}
		if (count > minPointNum) {
			this.vehicleMapping.put(trackInfo.dInfo.destNo, localCluster);
		}
		clusterServer.clear();
	}

	@Override
	public void endAnalysisTrack(AnalysisServer parentServer,
			InfoContainer statInfo) {
		
		// TODO Auto-generated method stub
		Date sDate = statInfo.getDate(ITrackAnalysis.STAT_START_TIME_DATE);
		Date eDate = statInfo.getDate(ITrackAnalysis.STAT_END_TIME_DATE);
		int distanceScope=3000;
		GISMarkClusterInstance clusterServer = new GISMarkClusterInstance(
				(int) Math.round(distanceScope / 0.9), true);
		clusterServer.setScaleLevel(16);
		Iterator itr = this.vehicleMapping.keySet().iterator();
		String destNo = null;
		LocalCluster localCluster = null;
		while (itr.hasNext()) {
			destNo = (String) itr.next();
			localCluster = (LocalCluster) this.vehicleMapping.get(destNo);
			clusterServer.addPoint(destNo, localCluster.clo, localCluster.cla);
		}
		
		DbHandle conn = null;
		try {
			conn = DbServer.getSingleInstance().getConn();
			String sql = "insert into ana_driver_home_stat(id,stat_time,lo,la,total_car,car_detail) values(?,?,?,?,?,empty_blob())";

			StatementHandle psmt = conn.prepareStatement(sql);
			StatementHandle stmt=conn.createStatement();
			
			int id = 0;
			List<Cluster> list = clusterServer.getClusterPoint();
			System.out.println("Start endAnalysisTrack:" + list.size());
			Cluster cluster=null;
			HashMap tempMap=new HashMap();
			
			if (list != null && list.size() > 0) {
				for (int i = 0; i < list.size(); i++) {
					cluster = list.get(i);
					if (cluster.pList.size() > 0) {
						PointInfo pointInfo = null;
						StringBuffer carNoBuff = new StringBuffer();
						for (int j = 0; j < cluster.pList.size(); j++) {
							pointInfo = (PointInfo) cluster.pList.get(j);
							if (j > 0) {
								carNoBuff.append("," + pointInfo.id);
							} else {
								carNoBuff.append(pointInfo.id);
							}
						}
						if (StrFilter.hasValue(carNoBuff.toString())) {
							// 向数据库中插入数据
							id = (int) DbServer.getSingleInstance()
									.getAvaliableId(conn,
											"ana_driver_home_stat", "id");
							tempMap.put(id, carNoBuff.toString());
							psmt.setInt(1, id);
							psmt.setDate(2, new java.sql.Date(sDate.getTime()));
							psmt.setDouble(3, cluster.clo);
							psmt.setDouble(4, cluster.cla);
							psmt.setInt(5, cluster.pList.size());
							psmt.addBatch();
						}

					}
				}
				psmt.executeBatch();
				Iterator itr1=tempMap.keySet().iterator();	
				String carNos=null;
				try {
					conn.setAutoCommit(false);
					while(itr1.hasNext()){
						id=(Integer)itr1.next();
						carNos=(String)tempMap.get(id);
						sql = "select * from ana_driver_home_stat where id="
								+ id+" for update";
						ResultSet rs = stmt.executeQuery(sql);
						if (rs.next()) {
							byte[] tempBytes = carNos
									.getBytes();
							BLOB blob = (BLOB) rs.getBlob("car_detail");
							OutputStream out = blob.getBinaryOutputStream();
							out.write(tempBytes);
							out.close();
						}
						
					}
					conn.commit();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					conn.rollback();
				}				
			}

		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			clusterServer.clear();
			DbServer.getSingleInstance().releaseConn(conn);
		}
	}
	public class LocalCluster{
		public double clo,cla;
	}
}
