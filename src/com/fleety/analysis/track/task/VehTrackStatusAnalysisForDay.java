package com.fleety.analysis.track.task;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.text.NumberFormat;
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

public class VehTrackStatusAnalysisForDay implements ITrackAnalysis {
	private HashMap vehicleMapping = null;
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
//	private int totalPointNum = 0;
//	private int totalPositionPointNum = 0;
//	private int totalCarNum = 0;
	private final static int STATUS_FREE = 0; // 空车
	private final static int STATUS_LOAD = 1; // 重车
	private final static int STATUS_CALL = 2; // 电招
	private final static int STATUS_OFF = 3; // 离线
	
	
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
			sb.append("select count(*) as sum from ANA_CAR_TRACKSTATUS_STAT")
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
		 * 分析所有的轨迹，将在同一个状态概率超过80%的车辆信息
		 */
		String plateNo = trackInfo.dInfo.destNo;
		int totalPointNum = 0;
		int pointStatusANum = 0;
		int pointStatusBNum = 0;
		int pointStatusCNum = 0;
		int pointStatusDNum = 0;
		int maxStatusNum = 0;
		String maxStatus ="";
		double maxPointStatusPec = 0;
		int status = 0;
		if (!StrFilter.hasValue(plateNo)) {
			return;
		}
		if (trackInfo.trackArr != null && trackInfo.trackArr.length > 0) {
			totalPointNum = trackInfo.trackArr.length;
			//所有轨迹点中，统一状态超过80%的，保存
			for (int i = 0; i < trackInfo.trackArr.length; i++) {
				status = (trackInfo.trackArr[i].getInteger(
						TrackIO.DEST_STATUS_FLAG).intValue() & 0x0f);
				if(status==STATUS_FREE){
					pointStatusANum++;
				}else if(status==STATUS_LOAD){
					pointStatusBNum++;
				}else if(status==STATUS_CALL){
					pointStatusCNum++;
				}else if(status==STATUS_OFF){
					pointStatusDNum++;
				}
			}
			System.out.println(GeneralConst.YYYY_MM_DD.format(trackInfo.sDate)+"  plateNo:"+plateNo+"  totalPoint:"+totalPointNum);
			maxStatusNum = pointStatusANum;
			maxStatus = "空车";
			if(maxStatusNum<pointStatusBNum){
				maxStatusNum = pointStatusBNum;
				maxStatus = "重车";
			}
			if(maxStatusNum<pointStatusCNum){
				maxStatusNum = pointStatusCNum;
				maxStatus = "电招";
			}
			if(maxStatusNum<pointStatusDNum){
				maxStatusNum = pointStatusDNum;
				maxStatus = "离线";
			}
		    BigDecimal value = new BigDecimal(maxStatusNum/totalPointNum);
	        value = value.setScale(4, BigDecimal.ROUND_HALF_UP);
	        maxPointStatusPec = value.doubleValue();
	        NumberFormat fmt = NumberFormat.getPercentInstance();  
	        fmt.setMaximumFractionDigits(2);
	        fmt.setMinimumFractionDigits(2);
	        if(maxPointStatusPec>=0.8000){
				VehicleStatInfo info = new VehicleStatInfo();
				info.carNo = trackInfo.dInfo.destNo;
				info.mdtId = trackInfo.dInfo.mdtId;
				info.companyid = trackInfo.dInfo.companyId;
				info.companyName = trackInfo.dInfo.companyName;
				info.totalPointNum = totalPointNum;
				info.pointStatusANum = pointStatusANum;
				info.pointStatusBNum = pointStatusBNum;
				info.pointStatusCNum = pointStatusCNum;
				info.pointStatusDNum = pointStatusDNum;
				info.maxStatus = maxStatus;
				info.maxPointStatusPec=fmt.format(maxPointStatusPec);
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
			String sql = "insert into ANA_CAR_TRACKSTATUS_STAT (id,stat_time,car_no,mdt_id,point_total_num,point_free_num,point_load_num,point_call_num,point_off_num,max_status,max_status_pec,companyid,companyname,createtime) values(?,?,?,?,?,?,?,?,?,?,?,?,?,sysdate)";
			StatementHandle psmt = conn.prepareStatement(sql);

			while (itr.hasNext()) {
				plateNo = (String) itr.next();
				info = (VehicleStatInfo) this.vehicleMapping.get(plateNo);
				id = (int) DbServer.getSingleInstance().getAvaliableId(conn,
						"ANA_CAR_TRACKSTATUS_STAT", "id");
				psmt.setInt(1, id);
				psmt.setDate(2, new java.sql.Date(sDate.getTime()));
				psmt.setString(3, plateNo);
				psmt.setInt(4, info.mdtId);
				psmt.setInt(5, info.totalPointNum);
				psmt.setInt(6, info.pointStatusANum);
				psmt.setInt(7, info.pointStatusBNum);
				psmt.setInt(8, info.pointStatusCNum);
				psmt.setInt(9, info.pointStatusDNum);
				psmt.setString(10, info.maxStatus);
				psmt.setString(11, info.maxPointStatusPec);
				psmt.setInt(12, info.companyid);
				psmt.setString(13, info.companyName);
				psmt.addBatch();
				count++;
				if (count % 200 == 0) {
					psmt.executeBatch();
				}
			}
			psmt.executeBatch();
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
		public String companyName = "";
		public int companyid = 0;
		public int totalPointNum = 0;
		public int pointStatusANum = 0;
		public int pointStatusBNum = 0;
		public int pointStatusCNum = 0;
		public int pointStatusDNum = 0;
		public String maxStatus ="";
		public String maxPointStatusPec = "";
		
	}
}
