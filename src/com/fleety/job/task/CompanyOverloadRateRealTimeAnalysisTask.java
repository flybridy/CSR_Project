package com.fleety.job.task;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import server.db.DbServer;
import server.track.TrackServer;

import com.fleety.analysis.realtime.CompanyOverRateRealTimeBean;
import com.fleety.analysis.track.DestInfo;
import com.fleety.analysis.track.TrackInfo;
import com.fleety.base.InfoContainer;
import com.fleety.track.TrackIO;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.fleety.util.pool.db.redis.RedisConnPoolServer;
import com.fleety.util.pool.db.redis.RedisTableBean;
import com.fleety.util.pool.thread.BasicTask;

public class CompanyOverloadRateRealTimeAnalysisTask extends BasicTask {
	private long refresh_interval = 10 * 60 * 1000;// 刷新间隔是10分钟

	public boolean execute() throws Exception {
		DbHandle conn = DbServer.getSingleInstance().getConnWithUseTime(0);
		DbHandle conn2 = DbServer.getSingleInstance().getConnWithUseTime(0);
		StatementHandle stmt2 = conn2
				.prepareStatement("insert into today_company_clock_overload(company_id,overload_num_red,empty_num_red,task_num_red,other_num_red,overload_num_green,empty_num_green,task_num_green,other_num_green,overload_num_electric,empty_num_electric,task_num_electric,other_num_electric,overload_num_accessible,empty_num_accessible,task_num_accessible,other_num_accessible,total_num,index_num) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
		try {
			DestInfo dInfo;
			ArrayList destList = new ArrayList(1024);
			try {
				StatementHandle stmt = conn
						.prepareStatement("select mdt_id,dest_no,company_id,company_name,type_id,gps_run_com_id,gps_run_com_name from v_ana_dest_info");
				ResultSet sets = stmt.executeQuery();
				while (sets.next()) {
					dInfo = new DestInfo();
					dInfo.mdtId = sets.getInt("mdt_id");
					dInfo.destNo = sets.getString("dest_no");
					dInfo.companyId = sets.getInt("company_id");
					dInfo.companyName = sets.getString("company_name");
					dInfo.gpsRunComId = sets.getInt("gps_run_com_id");
					dInfo.gpsRunComName = sets.getString("gps_run_com_name");
					dInfo.carType = sets.getInt("type_id");
					destList.add(dInfo);
				}
			} catch (Exception e) {
				throw e;
			}
			// 每十分钟取样一次，以最后一个轨迹点状态作为空重率的判断依据
			Calendar now = Calendar.getInstance();// 当前时间
			now.set(Calendar.MINUTE,
					(int) (now.getTimeInMillis() % 3600000) / 600000 * 10);
			now.set(Calendar.SECOND, 0);
			now.set(Calendar.MILLISECOND, 0);
			Date eDate = now.getTime();
			int flag = 0;
			
			if ((eDate.getTime() % 3600000) >= 0
					&& (eDate.getTime() % 3600000) < 600000) {
				flag = 1;
			}
			now.setTimeInMillis(now.getTimeInMillis() - refresh_interval + 1000);
			Date sDate = now.getTime();
			InfoContainer queryInfo = new InfoContainer();
			queryInfo.setInfo(TrackServer.START_DATE_FLAG, sDate);
			queryInfo.setInfo(TrackServer.END_DATE_FLAG, eDate);
			TrackInfo trackInfo;
			HashMap<String, CompanyOverRateRealTimeBean> list = new HashMap<String, CompanyOverRateRealTimeBean>();
			CompanyOverRateRealTimeBean bean = null;
			for (Iterator itr = destList.iterator(); itr.hasNext();) {
				dInfo = (DestInfo) itr.next();
				queryInfo.setInfo(TrackServer.DEST_NO_FLAG, dInfo.destNo);

				trackInfo = new TrackInfo();
				trackInfo.dInfo = dInfo;
				trackInfo.sDate = sDate;
				trackInfo.eDate = eDate;
				trackInfo.trackArr = TrackServer.getSingleInstance()
						.getTrackInfo(queryInfo);
				int status = this.analysisTrack(trackInfo.trackArr);
				if (list.containsKey(String.valueOf(dInfo.companyId))) {
					bean = list.get(String.valueOf(dInfo.companyId));
				} else {
					bean = new CompanyOverRateRealTimeBean();
					bean.setCompany_id(dInfo.companyId);
				}
				bean.setUid(String.valueOf(dInfo.companyId)
						+ "_"
						+ String.valueOf(((eDate.getTime() % 3600000) / 600000 + 6) % 6));// uid为“企业号_点序号”
				bean.setIndex((int) (((eDate.getTime() % 3600000) / 600000 + 6) % 6));// 在一个小时中，每10分钟一个点的点序号
				if (status == 0||status == 8) {
					switch (trackInfo.dInfo.carType) {
					case 1:
						bean.setEmpty_num_red(bean.getEmpty_num_red() + 1);
						break;
					case 2:
						bean.setEmpty_num_green(bean.getEmpty_num_green() + 1);
						break;
					case 3:
						bean.setEmpty_num_electric(bean.getEmpty_num_electric() + 1);
						break;
					case 4:
						bean.setEmpty_num_accessible(bean
								.getEmpty_num_accessible() + 1);
						break;
					}
					bean.setTotal_num(bean.getTotal_num() + 1);
				} else if (status == 1||status == 9) {
					switch (trackInfo.dInfo.carType) {
					case 1:
						bean.setOverload_num_red(bean.getOverload_num_red() + 1);
						break;
					case 2:
						bean.setOverload_num_green(bean.getOverload_num_green() + 1);
						break;
					case 3:
						bean.setOverload_num_electric(bean
								.getOverload_num_electric() + 1);
						break;
					case 4:
						bean.setOverload_num_accessible(bean
								.getOverload_num_accessible() + 1);
						break;
					}
					bean.setTotal_num(bean.getTotal_num() + 1);
				} else if (status == 2) {
					switch (trackInfo.dInfo.carType) {
					case 1:
						bean.setTask_num_red(bean.getTask_num_red() + 1);
						break;
					case 2:
						bean.setTask_num_green(bean.getTask_num_green() + 1);
						break;
					case 3:
						bean.setTask_num_electric(bean.getTask_num_electric() + 1);
						break;
					case 4:
						bean.setTask_num_accessible(bean
								.getTask_num_accessible() + 1);
						break;
					}
					bean.setTotal_num(bean.getTotal_num() + 1);
				} else if (status != -1) {
					switch (trackInfo.dInfo.carType) {
					case 1:
						bean.setOther_num_red(bean.getOther_num_red() + 1);
						break;
					case 2:
						bean.setOther_num_green(bean.getOther_num_green() + 1);
						break;
					case 3:
						bean.setOther_num_electric(bean.getOther_num_electric() + 1);
						break;
					case 4:
						bean.setOther_num_accessible(bean
								.getOther_num_accessible() + 1);
						break;
					}
					bean.setTotal_num(bean.getTotal_num() + 1);
				}
				list.put(String.valueOf(dInfo.companyId), bean);
			}
			CompanyOverRateRealTimeBean beans[] = new CompanyOverRateRealTimeBean[list
					.size()];
			if (((eDate.getTime() / 3600000) % 24 + 8) % 24 == 0
					&& eDate.getTime() % (3600 * 1000) >= 0
					&& eDate.getTime() % (3600 * 1000) < 10 * 60 * 1000)// 如果过了0点，前一天的数据被删除（23点-0点，数据序号为0，0点-1点序号为1）
			{
				StatementHandle stmt = conn
						.prepareStatement("delete from today_company_clock_overload");
				stmt.execute();
			}
			int i = 0;
			for (Iterator it = list.keySet().iterator(); it.hasNext();) {
				beans[i] = (CompanyOverRateRealTimeBean) list.get(it.next());
				if (flag == 1) {
					stmt2.setLong(1, beans[i].getCompany_id());
					stmt2.setLong(2, beans[i].getOverload_num_red());
					stmt2.setLong(3, beans[i].getEmpty_num_red());
					stmt2.setLong(4, beans[i].getTask_num_red());
					stmt2.setLong(5, beans[i].getOther_num_red());
					stmt2.setLong(6, beans[i].getOverload_num_green());
					stmt2.setLong(7, beans[i].getEmpty_num_green());
					stmt2.setLong(8, beans[i].getTask_num_green());
					stmt2.setLong(9, beans[i].getOther_num_green());
					stmt2.setLong(10, beans[i].getOverload_num_electric());
					stmt2.setLong(11, beans[i].getEmpty_num_electric());
					stmt2.setLong(12, beans[i].getTask_num_electric());
					stmt2.setLong(13, beans[i].getOther_num_electric());
					stmt2.setLong(14, beans[i].getOverload_num_accessible());
					stmt2.setLong(15, beans[i].getEmpty_num_accessible());
					stmt2.setLong(16, beans[i].getTask_num_accessible());
					stmt2.setLong(17, beans[i].getOther_num_accessible());
					stmt2.setLong(18, beans[i].getTotal_num());
					stmt2.setInt(19,
							(int) ((eDate.getTime() / 3600000) % 24 + 8) % 24);// 在一天中，每个小时一个点的点序号(当正好在整点上时，算在前一个时段，如九点算出来的数据序号是8)
					stmt2.addBatch();
					if (i % 200 == 0)
						stmt2.executeBatch();
				}
				i++;
			}
			stmt2.executeBatch();
			if ((eDate.getTime() % 3600000) >= 0
					&& (eDate.getTime() % 3600000) < 600000) {
				this.deleteInfo();
			}
			RedisConnPoolServer.getSingleInstance().saveTableRecord(beans);
		} catch (Exception e) {
			throw e;
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
			DbServer.getSingleInstance().releaseConn(conn2);
		}
		return false;
	}

	private int analysisTrack(InfoContainer[] info) {
		if (info == null||info.length==0) {
			return -1;// 如果没有轨迹文件，状态设置为-1
		}
		InfoContainer ifc = info[info.length - 1];
		return ifc.getInteger(TrackIO.DEST_STATUS_FLAG).intValue()&0x0F;
	}

	private void deleteInfo() throws Exception {
		CompanyOverRateRealTimeBean bean = new CompanyOverRateRealTimeBean();
		Set<String> keySet = RedisConnPoolServer.getSingleInstance()
				.getAllIdsForTable(bean);
		Iterator<String> it = keySet.iterator();
		String uid = "";
		List<CompanyOverRateRealTimeBean> list = new ArrayList<CompanyOverRateRealTimeBean>();
		while (it.hasNext()) {
			bean = new CompanyOverRateRealTimeBean();
			uid = it.next();
			bean.setUid(uid);
			list.add(bean);
		}

		if (list.size() > 0) {
			RedisTableBean[] beanArr = new CompanyOverRateRealTimeBean[list
					.size()];
			list.toArray(beanArr);
			RedisConnPoolServer.getSingleInstance().deleteTableRecord(beanArr);
		}
	}

	public String getDesc() {
		return "企业重载率实时分析";
	}

	public Object getFlag() {
		return "CompanyOverloadRateRealTimeAnalysisTask";
	}

}
