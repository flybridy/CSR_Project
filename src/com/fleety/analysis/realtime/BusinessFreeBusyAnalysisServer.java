package com.fleety.analysis.realtime;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import server.db.DbServer;
import com.fleety.analysis.RealTimeAnalysisServer;
import com.fleety.analysis.track.DestInfo;
import com.fleety.common.redis.BusinessFreeBusyBean;
import com.fleety.common.redis.Gps_Pos;
import com.fleety.server.GlobalUtilServer;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.fleety.util.pool.db.redis.BasicRedisObserver;
import com.fleety.util.pool.db.redis.IRedisObserver;
import com.fleety.util.pool.db.redis.RedisConnPoolServer;
import com.fleety.util.pool.db.redis.RedisTableBean;
import com.fleety.util.pool.thread.BasicTask;
import com.fleety.util.pool.timer.FleetyTimerTask;

public class BusinessFreeBusyAnalysisServer extends RealTimeAnalysisServer {
	private IRedisObserver observer = null;

	public boolean startServer() {
		this.isRunning = super.startServer();
		if (!this.isRunning) {
			return false;
		}
		BusinessFreeBusyBean bean = new BusinessFreeBusyBean();
		try {
			List beanList = RedisConnPoolServer.getSingleInstance()
					.queryTableRecord(new BusinessFreeBusyBean[] { bean });
			if (beanList != null) {
				for (int i = 0; i < beanList.size(); i++) {
					bean = (BusinessFreeBusyBean) beanList.get(i);
					if (bean.getLastSystemDate().getDate() == (new Date()
							.getDate())) {
						destMapping.put(bean.getUid(), bean);
					}
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		ArrayList patternList = new ArrayList(2);
		patternList.add("D_REALTIME_VEHICLE_INFO_CHANNEL_*");
		this.observer = new BasicRedisObserver(patternList) {
			public void msgArrived(String pattern, String msg, String content) {
				if (content != null) {
					BusinessFreeBusyAnalysisServer.this
							.addExecTask(new LocationAnalysis(content));
				}
			}
		};
		RedisConnPoolServer.getSingleInstance().addListener(this.observer);

		GlobalUtilServer.globalTimerPool.schedule(new FleetyTimerTask() {
			@Override
			public void run() {
				// TODO Auto-generated method stub
				HashMap tempMap = null;
				synchronized (destMapping) {
					tempMap = (HashMap) destMapping.clone();
				}
				GlobalUtilServer.globalThreadPool.addTask(new BatchSaveTask(
						tempMap));
			}
		}, 60000, 15 * 60 * 1000);
		return this.isRunning();
	}

	public void stopServer() {
		RedisConnPoolServer.getSingleInstance().removeListener(this.observer);
		super.stopServer();
	}

	private HashMap destMapping = new HashMap();

	private class BatchSaveTask extends BasicTask {

		private HashMap mapping = null;

		public BatchSaveTask(HashMap mapping) {
			this.mapping = mapping;
		}

		@Override
		public boolean execute() throws Exception {

			if (this.mapping == null) {
				return true;
			}
			BusinessFreeBusyBean bean = null;
			Iterator itr = this.mapping.values().iterator();
			List insertList = new ArrayList();
			List updateList = new ArrayList();
			while (itr.hasNext()) {
				bean = (BusinessFreeBusyBean) itr.next();
				if (this.isDbExist(bean.getUid(), bean.getLastSystemDate())) {
					updateList.add(bean);
				} else {
					insertList.add(bean);
				}
			}
			this.insertDb(insertList);
			this.updateDb(updateList);
			// TODO Auto-generated method stub
			return true;
		}

		private void insertDb(List insertList) {
			DbHandle conn = null;
			try {
				conn = DbServer.getSingleInstance().getConn();
				String sql = "insert into ana_vehicle_business_realtime "
						+ " (id, car_no, mdt_id, type_id, taxi_company,"
						+ "  taxi_company_name, free_busy, busy_free,"
						+ "  stat_time, recode_time) " + " values "
						+ " (?, ?, ?, ?, ?, ?, ?, ?, ?, sysdate)";
				StatementHandle stmt = conn.prepareStatement(sql);

				DestInfo destInfo = null;
				BusinessFreeBusyBean bean = null;
				int id = 0;
				for (int i = 0; i < insertList.size(); i++) {
					bean = (BusinessFreeBusyBean) insertList.get(i);
					destInfo = GlobalUtilServer.getDestInfo(bean.getUid());
					if(destInfo==null){
						continue;
					}
					id = (int) DbServer.getSingleInstance().getAvaliableId(
							conn, "ana_vehicle_business_realtime", "id");
					stmt.setInt(1, id);
					stmt.setString(2, destInfo.destNo);
					stmt.setInt(3, destInfo.mdtId);
					stmt.setInt(4, destInfo.carType);
					stmt.setInt(5, destInfo.companyId);
					stmt.setString(6, destInfo.companyName);
					stmt.setInt(7, bean.getFree2Busy());
					stmt.setInt(8, bean.getBusy2Free());
					stmt.setDate(9, new java.sql.Date(bean.getLastSystemDate()
							.getTime()));
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

		private void updateDb(List updateList) {
			DbHandle conn = null;
			try {
				conn = DbServer.getSingleInstance().getConn();
				String sql = "update ana_vehicle_business_realtime set free_busy=?,busy_free=? where car_no=? and stat_time=?";
				StatementHandle stmt = conn.prepareStatement(sql);

				BusinessFreeBusyBean bean = null;
				for (int i = 0; i < updateList.size(); i++) {
					bean = (BusinessFreeBusyBean) updateList.get(i);
					stmt.setInt(1, bean.getFree2Busy());
					stmt.setInt(2, bean.getBusy2Free());
					stmt.setString(3, bean.getUid());
					stmt.setDate(4, new java.sql.Date(bean.getLastSystemDate()
							.getTime()));
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

		private boolean isDbExist(String carNo, Date lastSysTime) {
			DbHandle conn = null;
			try {
				conn = DbServer.getSingleInstance().getConn();
				String sql = "select id from ana_vehicle_business_realtime where car_no=? and stat_time=?";
				StatementHandle stmt = conn.prepareStatement(sql);
				stmt.setString(1, carNo);
				stmt.setDate(2, new java.sql.Date(lastSysTime.getTime()));
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

	private class LocationAnalysis extends BasicTask {
		private String jsonLocation = null;

		public LocationAnalysis(String jsonLocation) {
			this.jsonLocation = jsonLocation;
		}

		public boolean execute() throws Exception {

			Gps_Pos gps = new Gps_Pos();
			gps.parseJSONString(this.jsonLocation);

			BusinessFreeBusyBean bean = null;
			synchronized (destMapping) {
				bean = (BusinessFreeBusyBean) destMapping.get(gps.getUid());
				if (bean == null
						|| (gps.getSysDate().getDate() != bean
								.getLastSystemDate().getDate())) {
					if (bean != null) {
						HashMap temp = new HashMap();
						temp.put(bean.getUid(), bean);
						GlobalUtilServer.globalThreadPool
								.addTask(new BatchSaveTask(temp));
					}
					bean = this.createBean(gps, null);
				} else {
					if (bean.getStatus() != 0 && gps.getState() == 0) {
						// 重变空，更新重变空记录
						bean.setBusy2Free(bean.getBusy2Free() + 1);
					} else if (bean.getStatus() != 1 && gps.getState() == 1) {
						// 空变重,更新空变重记录
						bean.setFree2Busy(bean.getFree2Busy() + 1);
					}
					bean = this.createBean(gps, bean);
				}
				destMapping.put(gps.getUid(), bean);
				RedisConnPoolServer.getSingleInstance().saveTableRecord(
						new RedisTableBean[] { bean });
			}
			return true;
		}

		public Object getFlag() {
			return "LocationAnalysis";
		}

		private BusinessFreeBusyBean createBean(Gps_Pos gps,
				BusinessFreeBusyBean bean) {
			if (bean == null) {
				bean = new BusinessFreeBusyBean();
			}
			bean.setUid(gps.getUid());
			bean.setStatus(gps.getState());
			bean.setGpsStatus(gps.getGpsStatus());
			bean.setLastResportDate(gps.getDt());
			bean.setLastSystemDate(gps.getSysDate());
			return bean;
		}
	}
}
