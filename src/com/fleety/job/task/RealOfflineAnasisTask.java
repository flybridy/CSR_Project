package com.fleety.job.task;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import server.db.DbServer;
import server.var.VarManageServer;

import com.fleety.analysis.track.DestInfo;
import com.fleety.base.GeneralConst;
import com.fleety.base.StrFilter;
import com.fleety.common.redis.Gps_Pos;
import com.fleety.server.GlobalUtilServer;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.fleety.util.pool.db.redis.RedisConnPoolServer;
import com.fleety.util.pool.db.redis.RedisTableBean;
import com.fleety.util.pool.thread.BasicTask;

public class RealOfflineAnasisTask extends BasicTask {

	private int duration = 30 * 60 * 1000;

	@Override
	// 每10分钟读取redis中的车辆最后位置汇报，如果最后汇报时间与当前时间超过30分钟，记录为不在线
	public boolean execute() throws Exception {
		String temp = VarManageServer.getSingleInstance().getVarStringValue(
				"off_line_duration_min");
		if (StrFilter.hasValue(temp)) {
			try {
				this.duration = Integer.parseInt(temp) * 30 * 1000;
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		// TODO Auto-generated method stub
		try {
			if (!RedisConnPoolServer.getSingleInstance().isRunning()) {
				return true;
			}
			HashMap destMap = GlobalUtilServer.getDestInfoMapClone();
			Gps_Pos gps = new Gps_Pos();
			List list = RedisConnPoolServer.getSingleInstance()
					.queryTableRecord(new RedisTableBean[] { gps });

			List offlineList = new ArrayList();
			if (list != null) {
				for (int i = 0; i < list.size(); i++) {
					gps = (Gps_Pos) list.get(i);
					if (destMap.containsKey(gps.getUid())) {
						destMap.remove(gps.getUid());
						if (System.currentTimeMillis()
								- gps.getSysDate().getTime() > duration) {
							offlineList.add(gps);
						}
					}
				}
			}
			DestInfo destInfo = null;
			Date today = GeneralConst.YYYY_MM_DD.parse(GeneralConst.YYYY_MM_DD
					.format(new Date()));
			if (destMap.size() > 0) {
				Iterator itr = destMap.values().iterator();
				while (itr.hasNext()) {
					destInfo = (DestInfo) itr.next();
					gps = new Gps_Pos();
					gps.setUid(destInfo.destNo);
					gps.setLa(0);
					gps.setLo(0);
					gps.setDt(today);
					gps.setSysDate(today);
					offlineList.add(gps);
				}
			}
			// 对offlineList进行处理
			this.disposeOfflineList(offlineList);

		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return true;
	}

	private void disposeOfflineList(List offlineList) {
		Gps_Pos gps = null, dbGps = null;
		List updateList = new ArrayList();
		List insertList = new ArrayList();
		for (int i = 0; i < offlineList.size(); i++) {
			gps = (Gps_Pos) offlineList.get(i);
			if (isDbExist(gps.getUid(), gps.getSysDate())) {
				updateList.add(gps);
			} else {
				insertList.add(gps);
			}
		}
		this.insertDb(insertList);
		this.updateDb(updateList);
	}

	private void insertDb(List insertList) {
		DbHandle conn = null;
		try {
			conn = DbServer.getSingleInstance().getConn();
			String sql = "insert into ana_vehicle_offline_realtime"
					+ " (id, car_no, mdt_id, type_id, taxi_company, taxi_company_name,"
					+ "  offline_time, start_time, end_time, start_lo, start_la, recode_time) "
					+ " values(?, ?, ?, ?, ?, ?, ?, ?, sysdate, ?, ?, sysdate)";
			StatementHandle stmt = conn.prepareStatement(sql);

			DestInfo destInfo = null;
			Gps_Pos gps = null;
			int id = 0;
			for (int i = 0; i < insertList.size(); i++) {
				gps = (Gps_Pos) insertList.get(i);
				destInfo = GlobalUtilServer.getDestInfo(gps.getUid());
				id = (int) DbServer.getSingleInstance().getAvaliableId(conn,
						"ana_vehicle_offline_realtime", "id");
				stmt.setInt(1, id);
				stmt.setString(2, destInfo.destNo);
				stmt.setInt(3, destInfo.mdtId);
				stmt.setInt(4, destInfo.carType);
				stmt.setInt(5, destInfo.companyId);
				stmt.setString(6, destInfo.companyName);
				stmt.setInt(7, (int) ((System.currentTimeMillis() - gps
						.getSysDate().getTime()) / 60000));
				stmt.setTimestamp(8, new Timestamp(gps.getSysDate().getTime()/1000*1000));
				stmt.setDouble(9, gps.getLo() * 1.0 / 10000000.0);
				stmt.setDouble(10, gps.getLa() * 1.0 / 10000000.0);
				stmt.addBatch();
				if ((i+1) % 200 == 0) {
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

	private void updateDb(List updateList) {
		DbHandle conn = null;
		try {
			conn = DbServer.getSingleInstance().getConn();
			String sql = "update ANA_VEHICLE_OFFLINE_REALTIME set offline_time=?,end_time=sysdate,recode_time=sysdate where car_no=? and start_time=?";
			StatementHandle stmt = conn.prepareStatement(sql);

			Gps_Pos gps = null;
			for (int i = 0; i < updateList.size(); i++) {
				gps = (Gps_Pos) updateList.get(i);
				stmt.setInt(1, (int) ((System.currentTimeMillis() - gps
						.getSysDate().getTime()) / 60000));
				stmt.setString(2, gps.getUid());
				stmt.setTimestamp(3, new Timestamp(gps.getSysDate().getTime()/1000*1000));
				stmt.addBatch();
				if ((i+1) % 200 == 0) {
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

	private boolean isDbExist(String carNo, Date lastReportTime) {
		DbHandle conn = null;
		try {
			conn = DbServer.getSingleInstance().getConn();
			
			String sql = "select id,offline_time from ANA_VEHICLE_OFFLINE_REALTIME where car_no=? and start_time=?";
			StatementHandle stmt = conn.prepareStatement(sql);
			stmt.setString(1, carNo);
			stmt.setTimestamp(2, new Timestamp(lastReportTime.getTime()/1000*1000));
			ResultSet rs = stmt.executeQuery();
			if (rs.next()) {
				return true;
			}

		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}
		return false;
	}

}
