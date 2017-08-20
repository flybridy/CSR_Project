package com.fleety.analysis.track.task;

import java.io.File;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;

import server.db.DbServer;
import server.threadgroup.PoolInfo;
import server.threadgroup.ThreadPoolGroupServer;
import server.track.TrackServer;
import server.var.VarManageServer;

import com.fleety.analysis.track.DestInfo;
import com.fleety.base.GeneralConst;
import com.fleety.base.InfoContainer;
import com.fleety.server.BasicServer;
import com.fleety.server.area.AreaDataLoadServer;
import com.fleety.server.area.AreaInfo;
import com.fleety.track.TrackIO;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.fleety.util.pool.thread.BasicTask;
import com.fleety.util.pool.thread.ThreadPool;
import com.fleety.util.pool.timer.FleetyTimerTask;
import com.fleety.util.pool.timer.TimerPool;

public class CallCarTroubleAreaDataServer extends BasicServer {
	private static final String String = null;
	private TimerTask task = null;
	private TimerPool timer = null;
	private ThreadPool pool = null;
	ArrayList destList = new ArrayList(1024);
	private HashMap callCarTroubleAreaPassMap = null;
	private HashMap<Integer, CallCarTroubleAreaPlan> callCarTroubleAreaPlanMap = null;
	private HashMap singleBusinessDataBsMap = null;
	private HashMap<Integer, AreaInfo> areaData = null;
	private int days = 1;
	private int type_1 = 1;//空进重出 2重进空出 3重进重出 4空进空出
	private long relate = 60 * 1000l;

	@Override
	public boolean startServer() {
		
		Integer timerThreadNum = VarManageServer.getSingleInstance()
				.getIntegerPara("timer_thread_num");
		if (timerThreadNum == null) {
			timerThreadNum = new Integer(1);
		}
		this.timer = ThreadPoolGroupServer.getSingleInstance().createTimerPool(
				"CallCarTroubleAreaDataServer_Timer",
				timerThreadNum.intValue(), false);

		Integer threadNum = VarManageServer.getSingleInstance().getIntegerPara(
				"pool_thread_num");
		if (threadNum == null) {
			threadNum = new Integer(1);
		}
		PoolInfo pInfo = new PoolInfo();
		pInfo.taskCapacity = 10000;
		pInfo.workersNumber = threadNum;
		pInfo.poolType = ThreadPool.MULTIPLE_TASK_LIST_POOL;
		try {
			this.pool = ThreadPoolGroupServer.getSingleInstance()
					.createThreadPool(
							"CallCarTroubleAreaDataServer_Thread_Pool", pInfo);
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (this.timer != null && this.pool != null) {
			this.isRunning = true;
		}

		int hour = this.getIntegerPara("hour").intValue();
		int minute = this.getIntegerPara("minute").intValue();

		String temp = this.getStringPara("preDays");
		if (temp != null && !temp.equals("")) {
			days = this.getIntegerPara("preDays").intValue();
		}
		System.out.println("days:" + days);
		temp = this.getStringPara("relate");
		if (temp != null && !temp.equals("")) {
			relate = this.getIntegerPara("relate").intValue()*60*1000l;
		}
		try {
			loadDestInfo();
		} catch (Exception e) {
			e.printStackTrace();
		}

		Calendar cal = this.getNextExecCalendar(hour, minute);

		if (cal.get(Calendar.DAY_OF_MONTH) != Calendar.getInstance().get(
				Calendar.DAY_OF_MONTH)) {
			this.scheduleTask(new TimerTask(days), 500);
		}

		long delay = cal.getTimeInMillis() - System.currentTimeMillis();
		this.isRunning = this.scheduleTask(this.task = new TimerTask(1), delay,
				GeneralConst.ONE_DAY_TIME);

		return true;
	}

	public void stopServer() {
		if (this.task != null) {
			this.task.cancel();
		}
		super.stopServer();
	}

	private void loadOneDayData(Date sTime) {
		this.callCarTroubleAreaPassMap = null;
		this.callCarTroubleAreaPlanMap = null;
		this.areaData = null;
		AreaDataLoadServer areaDataLoadServer = new AreaDataLoadServer();
		// 加载打车难区域信息
		this.areaData = areaDataLoadServer.getAreaData(101, false);
		// 加载打车难区域计划
		this.callCarTroubleAreaPlanMap = this
				.queryCallCarTroubleAreaPlan(sTime);
		// 加载在打车难区域内的营运数据
		 if(!querySingleBusinessDataBs(sTime)){
			 return ;
		 }
		if (this.callCarTroubleAreaPlanMap == null || areaData == null) {
			return;
		}
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			StatementHandle stmt = conn.createStatement();
			StringBuilder sb = new StringBuilder();
			sb.append("select * from plan_area_inout_info ")
					.append(" where stat_date = to_date('")
					.append(GeneralConst.YYYY_MM_DD.format(sTime))
					.append("','yyyy-MM-dd hh24:mi:ss')");
			ResultSet sets = stmt.executeQuery(sb.toString());
			if (sets.next()) {
				return;
			} else {
				this.callCarTroubleAreaPassMap = new HashMap();

			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}

		if (this.callCarTroubleAreaPassMap == null) {
			System.out.println("Not Need Analysis:" + this.toString());
		} else {
			System.out.println("Start Analysis:" + this.toString());
		}
		execDayData(sTime);
		relateServiceNo();
		endAnalysisTrack(sTime);
	}

	private void relateServiceNo() {
		if(callCarTroubleAreaPassMap==null){
			return;
		}
		System.out.println("Start RelateServiceNo:"+GeneralConst.YYYY_MM_DD_HH_MM_SS.format(new Date()));
		for (Iterator iterator = callCarTroubleAreaPassMap.keySet().iterator(); iterator.hasNext();) {
			String destNo = (String) iterator.next();
			PlanAreaInoutInfo planAreaInoutInfo = (PlanAreaInoutInfo)callCarTroubleAreaPassMap.get(destNo);
			if(planAreaInoutInfo==null){
				continue;
			}
			//if(planAreaInoutInfo.in_out_type!=this.type_1 ){
				//continue;
			//}
			long dateUp = 0;
			long lastEmptyToHeavyTIME = 0;
			for (Iterator iterator2 = singleBusinessDataBsMap.keySet().iterator(); iterator2.hasNext();) {
				String id = (String)iterator2.next();
				SingleBusinessDataBs singleBusinessDataBs = (SingleBusinessDataBs)singleBusinessDataBsMap.get(id);
				if(singleBusinessDataBs==null){
					continue;
				}
				if(!singleBusinessDataBs.dest_no.equals(planAreaInoutInfo.TAXI_NO)){
					continue;
				}
				dateUp = singleBusinessDataBs.date_up;
				lastEmptyToHeavyTIME = planAreaInoutInfo.last_empty_to_heavy_TIME;
				if(Math.abs(lastEmptyToHeavyTIME-dateUp) <= this.relate){
					planAreaInoutInfo.BUSINESS_ID = Integer.valueOf(id);
				}
			}
		}
		System.out.println("End RelateServiceNo:"+GeneralConst.YYYY_MM_DD_HH_MM_SS.format(new Date()));
	}

	private void loadDestInfo() throws Exception {
		destList = new ArrayList(1024);
		DbHandle conn = DbServer.getSingleInstance().getConn();
		DestInfo dInfo;
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
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}
	}

	private void execDayData(Date sTime) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(sTime);
		cal.setTimeInMillis(cal.getTimeInMillis() + GeneralConst.ONE_DAY_TIME
				- 1000);
		Date eDate = cal.getTime();
		System.out.println(GeneralConst.YYYY_MM_DD_HH_MM_SS.format(sTime)
				+ "   " + GeneralConst.YYYY_MM_DD_HH_MM_SS.format(eDate));
		int k = 0;
		DestInfo dInfo;
		Iterator itr = destList.iterator();
		int taxiTimes = 0;
		while (itr.hasNext()) {
			dInfo = (DestInfo) itr.next();
//			dInfo.destNo = "粤B1UG27";
			double slo = -1;
			double sla = -1;
			long sTime1 = 0;
			int sStatus = -1;
			long dest_time;
			double templo = 0;
			double templa = 0;
			int tempStatus = -1;
			int first = 0;
			double preKilo=0;

			for (Iterator iterator = this.callCarTroubleAreaPlanMap.values()
					.iterator(); iterator.hasNext();) {
				CallCarTroubleAreaPlan callCarTroubleAreaPlan = (CallCarTroubleAreaPlan) iterator
						.next();
				if (!callCarTroubleAreaPlan.carMap.containsKey(dInfo.destNo)) {
					// System.out.println("not continue: destNo:"+dInfo.destNo+" size:"+callCarTroubleAreaPlan.carMap.size()+" id:"+callCarTroubleAreaPlan.plan_id);
					continue;
				} else {
//					System.out.println("destNo:" + dInfo.destNo);
				}
//				System.out.println("taxiTimes:" + (taxiTimes++));
				boolean preInArea = false;
				boolean tempInArea = false;
				InfoContainer queryInfo = new InfoContainer();
				queryInfo.setInfo(TrackServer.START_DATE_FLAG, sTime);
				queryInfo.setInfo(TrackServer.END_DATE_FLAG, eDate);
				queryInfo.setInfo(TrackServer.DEST_NO_FLAG, dInfo.destNo);
				File file = TrackServer.getSingleInstance().getTrackFile(
						dInfo.destNo, sTime);
//				System.err.println("-------" + file.getAbsolutePath());
				InfoContainer[] trackArr = TrackServer.getSingleInstance()
						.getTrackInfo(queryInfo);
				AreaInfo areaInfo = areaData
						.get(callCarTroubleAreaPlan.area_id);
				PlanAreaInoutInfo obj = null;
				int firstInAreaStatus = 0;
				for (int i = 0; i < trackArr.length; i++) {
					dest_time = trackArr[i].getDate(TrackIO.DEST_TIME_FLAG)
							.getTime();
					// if(dest_time<singleBusinessDataBs.date_up-this.duras||dest_time>singleBusinessDataBs.date_down+this.durax){
					// continue;
					// }
					templo = trackArr[i].getDouble(TrackIO.DEST_LO_FLAG);
					templa = trackArr[i].getDouble(TrackIO.DEST_LA_FLAG);
					// tempSpeed =
					// trackArr[i].getDouble(TrackIO.DEST_SPEED_FLAG);
					tempStatus = trackArr[i]
							.getInteger(TrackIO.DEST_STATUS_FLAG) & 0x07;
					tempInArea = areaInfo.contains(templo, templa);
//					if("粤B1UG27".equals(dInfo.destNo))
//					{
//						System.out.println(GeneralConst.YYYY_MM_DD_HH_MM_SS.format(dest_time)+"  tempStatus:"+tempStatus);
//					}
					if (first == 0 && tempInArea) {
						obj = new PlanAreaInoutInfo();
						obj.plan_id=callCarTroubleAreaPlan.plan_id;
						obj.area_id=callCarTroubleAreaPlan.area_id;
						obj.TAXI_NO= dInfo.destNo;
						firstInAreaStatus = tempStatus;
						obj.IN_TIME = dest_time;
						callCarTroubleAreaPassMap.put(dInfo.destNo + "_"
								+ dest_time, obj);
					} else if (first == 1 && tempInArea && !preInArea) {
						obj = new PlanAreaInoutInfo();
						obj.IN_TIME = dest_time;
						obj.plan_id=callCarTroubleAreaPlan.plan_id;
						obj.area_id=callCarTroubleAreaPlan.area_id;
						obj.TAXI_NO= dInfo.destNo;
						firstInAreaStatus = tempStatus;
						callCarTroubleAreaPassMap.put(dInfo.destNo + "_"
								+ dest_time, obj);
					} else if (first == 1 && !tempInArea && preInArea) {
						obj.OUT_TIME = dest_time;
						obj.in_out_type = getByte(firstInAreaStatus, tempStatus);
//						System.out.println("in Area:"
//								+ GeneralConst.YYYY_MM_DD_HH_MM_SS
//										.format(new Date(obj.IN_TIME))
//								+ " out Area:"
//								+ GeneralConst.YYYY_MM_DD_HH_MM_SS
//										.format(new Date(obj.OUT_TIME))
//								+ " first_empty_to_heavy_TIME:"
//								+ GeneralConst.YYYY_MM_DD_HH_MM_SS
//										.format(new Date(
//												obj.first_empty_to_heavy_TIME))
//								+ " firstInAreaStatus:" + firstInAreaStatus
//								+ " outArea:" + tempStatus);
					}

					if (first == 1 && obj != null
							&& obj.IN_TIME != 0 
							&& obj.first_empty_to_heavy_TIME == 0) {
						if (sStatus == 0 && tempStatus != 0) {
							obj.first_empty_to_heavy_TIME = dest_time;
						}
					}
					if (tempInArea && obj != null && sStatus == 0
							&& tempStatus != 0) {
						obj.last_empty_to_heavy_TIME = dest_time;
					}
					if(obj!=null&& sStatus != 0)
					{
						double dis = countDistance(slo,sla,templo,templa);
						if(dis < 10){
							preKilo += dis;
						}
					}
					if (obj != null && sStatus != 0 && tempStatus == 0
							&& obj.last_heavy_to_empty_TIME == 0) {
						obj.last_heavy_to_empty_TIME = dest_time;

//						System.out.println("in Area:"
//								+ GeneralConst.YYYY_MM_DD_HH_MM_SS
//										.format(new Date(obj.IN_TIME))
//								+ " out Area:"
//								+ GeneralConst.YYYY_MM_DD_HH_MM_SS
//										.format(new Date(obj.OUT_TIME))
//								+ " first_empty_to_heavy_TIME:"
//								+ GeneralConst.YYYY_MM_DD_HH_MM_SS
//										.format(new Date(
//												obj.first_empty_to_heavy_TIME))
//								+ " last_heavy_to_empty_TIME:"
//								+ GeneralConst.YYYY_MM_DD_HH_MM_SS
//										.format(new Date(
//												obj.last_heavy_to_empty_TIME))
//								+ " firstInAreaStatus:" + firstInAreaStatus
//								+ " firstInAreaStatus:" + firstInAreaStatus
//								+ " outArea:" + tempStatus);
					}
					slo = templo;
					sla = templa;
					sStatus = tempStatus;
					sTime1 = dest_time;
					preInArea = tempInArea;
					first = 1;
				}
				if (obj != null && obj.OUT_TIME == 0 && trackArr.length > 0) {
					obj.OUT_TIME = sTime1;
					System.out.println(" is still in Area:"
							+ GeneralConst.YYYY_MM_DD_HH_MM_SS.format(new Date(
									sTime1)));
				}
				 preKilo = 0;
				first = 0;

			}
		}
	}

	public boolean querySingleBusinessDataBs(Date sTime) {
		this.singleBusinessDataBsMap = null;
		this.singleBusinessDataBsMap = new HashMap();
		Calendar cal = Calendar.getInstance();
		cal.setTime(sTime);
		cal.add(Calendar.DAY_OF_MONTH, 1);
		Date eTime = new Date(cal.getTimeInMillis() - 1000);

		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			StatementHandle stmt = conn.prepareStatement("select * from single_business_data_bs where date_up>=? and date_up<=?");
			stmt.setTimestamp(1, new Timestamp(sTime.getTime()));
			stmt.setTimestamp(2, new Timestamp(eTime.getTime()));
			ResultSet sets = stmt.executeQuery();
			SingleBusinessDataBs singleBusinessDataBs = null;
			while (sets.next()) {
				singleBusinessDataBs = new SingleBusinessDataBs();
				singleBusinessDataBs.id = sets.getString("id");
				singleBusinessDataBs.dest_no = sets.getString("DISPATCH_CAR_NO");
				singleBusinessDataBs.service_no = sets.getString("SERVICE_NO");
				singleBusinessDataBs.date_down = GeneralConst.YYYY_MM_DD_HH_MM_SS.parse(sets.getString("DATE_DOWN")).getTime();
				singleBusinessDataBs.date_up = GeneralConst.YYYY_MM_DD_HH_MM_SS.parse(sets.getString("DATE_UP")).getTime();
				singleBusinessDataBsMap.put(singleBusinessDataBs.id, singleBusinessDataBs);
			}
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}
		return true;
	}

	private HashMap<Integer, CallCarTroubleAreaPlan> queryCallCarTroubleAreaPlan(
			Date sTime) {
		HashMap<Integer, CallCarTroubleAreaPlan> callCarTroubleAreaPlanMap = new HashMap<Integer, CallCarTroubleAreaPlan>();
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			StatementHandle stmt = conn
					.prepareStatement("select id,plan_name,start_time,end_time,start_date,end_date,area_id,status from call_car_trouble_area_plan where start_date<=? and end_date>=? and status<=2");
			stmt.setTimestamp(1, new Timestamp(sTime.getTime()));
			stmt.setTimestamp(2, new Timestamp(sTime.getTime()));
			StatementHandle carStat = conn
					.prepareStatement("select car_id from call_car_trouble_com_bind_car where id =?");
			ResultSet sets = stmt.executeQuery();
			CallCarTroubleAreaPlan callCarTroubleAreaPlan = null;
			while (sets.next()) {
				callCarTroubleAreaPlan = new CallCarTroubleAreaPlan();
				callCarTroubleAreaPlan.plan_id = sets.getInt("id");
				callCarTroubleAreaPlan.plan_name = sets.getString("plan_name");
				callCarTroubleAreaPlan.start_date = GeneralConst.YYYY_MM_DD_HH_MM_SS
						.parse(sets.getString("start_date"));
				callCarTroubleAreaPlan.end_date = GeneralConst.YYYY_MM_DD_HH_MM_SS
						.parse(sets.getString("end_date"));
				callCarTroubleAreaPlan.start_time = sets
						.getString("start_time");
				callCarTroubleAreaPlan.end_time = sets.getString("end_time");
				callCarTroubleAreaPlan.area_id = sets.getInt("area_id");
				callCarTroubleAreaPlan.status = sets.getInt("status");
				carStat.setInt(1, callCarTroubleAreaPlan.plan_id);
				ResultSet rs = carStat.executeQuery();
				while (rs.next()) {
					callCarTroubleAreaPlan.carMap.put(rs.getString("car_id"),
							rs.getString("car_id"));
				}
				rs.close();
				System.out.println("callCarTroubleAreaPlan.plan_id:"
						+ callCarTroubleAreaPlan.plan_id + " carNum:"
						+ callCarTroubleAreaPlan.carMap.size());
				callCarTroubleAreaPlanMap.put(sets.getInt("id"),
						callCarTroubleAreaPlan);
			}
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}
		return callCarTroubleAreaPlanMap;
	}

	public class CallCarTroubleAreaPlan {
		public int plan_id;
		public String plan_name;
		public String start_time;// 考核开始时间
		public String end_time;// 考核结束时间
		public Date start_date;// 考核开始日期
		public Date end_date;// 考核结束日期
		public int area_id;// 打车难区域区域编号
		public int status;
		public HashMap<String, SingleBusinessDataBs> singleBusinessDataBsMap = new HashMap<String, SingleBusinessDataBs>();
		public Hashtable carMap = new Hashtable();
	}

	public class SingleBusinessDataBs {
		public String id;
		public String service_no;
		public String dest_no;
		public long date_up;
		public long date_down;
	}

	protected Calendar getNextExecCalendar(int hour, int minute) {
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.HOUR_OF_DAY, hour);
		cal.set(Calendar.MINUTE, minute);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);

		if (cal.getTimeInMillis() < System.currentTimeMillis()) {
			cal.add(Calendar.DAY_OF_MONTH, 1);
		}

		return cal;
	}

	/**
	 * 把某个任务放入定时执行中，如果任务执行时间较长，应该产生新的任务放置到执行任务池中进行执行
	 * 
	 * @param timerTask
	 *            待周期性执行的任务
	 * @param delay
	 *            延迟执行时长，单位毫秒
	 * @param period
	 *            执行周期，单位毫秒
	 * @return
	 */
	public boolean scheduleTask(FleetyTimerTask timerTask, long delay,
			long period) {
		if (!this.isRunning()) {
			return false;
		}

		this.timer.schedule(timerTask, delay, period);
		return true;
	}

	private class ExecTask extends BasicTask {
		private Calendar anaDate = null;

		public ExecTask(Calendar anaDate) {
			this.anaDate = anaDate;
		}

		public boolean execute() throws Exception {
			CallCarTroubleAreaDataServer.this.loadOneDayData(this.anaDate
					.getTime());
			return true;
		}

		public String getDesc() {
			return "昨日轨迹数据分析";
		}

		public Object getFlag() {
			return "YesterdayTrackAnalysisServer";
		}
	}

	private class TimerTask extends FleetyTimerTask {

		int days = 1;

		TimerTask(int days) {
			this.days = days;
		}

		public void run() {
			Calendar cal = Calendar.getInstance();
			for (int i = days; i >= 1; i--) {
				cal = Calendar.getInstance();
				cal.add(Calendar.DAY_OF_MONTH, 0 - i);
				cal.set(Calendar.HOUR_OF_DAY, 0);
				cal.set(Calendar.MINUTE, 0);
				cal.set(Calendar.SECOND, 0);
				cal.set(Calendar.MILLISECOND, 0);

				System.out
						.println("Fire ExecTask YesterdayTrackAnalysisServer:"
								+ GeneralConst.YYYY_MM_DD_HH.format(cal
										.getTime()));
				addExecTask(new ExecTask(cal));
			}
		}
	}

	/**
	 * 把执行任务放入执行线程池中进行执行
	 * 
	 * @param task
	 *            待执行的任务
	 * @return true代表放入成功 false代表失败
	 */
	public boolean addExecTask(BasicTask task) {
		if (!this.isRunning()) {
			return false;
		}

		return this.pool.addTaskWithReturn(task, false);
	}

	public boolean scheduleTask(FleetyTimerTask timerTask, long delay) {
		if (!this.isRunning()) {
			return false;
		}

		this.timer.schedule(timerTask, delay);
		return true;
	}

	public void endAnalysisTrack(Date sDate) {
		if (this.callCarTroubleAreaPassMap == null) {
			return;
		}
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			conn.setAutoCommit(false);
			int count = 1;
			StatementHandle stmt = conn
					.prepareStatement("insert into plan_area_inout_info"
							+ "(id,plan_id,area_id,TAXI_NO,in_out_type,BUSINESS_ID,IN_TIME,OUT_TIME,first_empty_to_heavy_TIME,last_heavy_to_empty_TIME,"
							+ "stat_date,log_TIME) values(?,?,?,?,?,?,?,?,?,?,?,?)");
			System.out.println("this.callCarTroubleAreaPassMap.size::"
					+ this.callCarTroubleAreaPassMap.size());
			for (Iterator iterator = this.callCarTroubleAreaPassMap.values()
					.iterator(); iterator.hasNext();) {
				PlanAreaInoutInfo passInfo = (PlanAreaInoutInfo) iterator
						.next();
				stmt.setInt(1, (int) DbServer.getSingleInstance()
						.getAvaliableId(conn, "plan_area_inout_info", "id"));
				stmt.setInt(2, passInfo.plan_id);
				stmt.setInt(3, passInfo.area_id);
				stmt.setString(4, passInfo.TAXI_NO);
				stmt.setInt(5, passInfo.in_out_type);
				stmt.setInt(6, passInfo.BUSINESS_ID);
				stmt.setTimestamp(7, new Timestamp(passInfo.IN_TIME));
				stmt.setTimestamp(8, new Timestamp(passInfo.OUT_TIME));
				stmt.setTimestamp(9, new Timestamp(
						passInfo.first_empty_to_heavy_TIME));
				stmt.setTimestamp(10, new Timestamp(
						passInfo.last_heavy_to_empty_TIME));
				stmt.setTimestamp(11, new Timestamp(sDate.getTime()));
				stmt.setTimestamp(12, new Timestamp(new Date().getTime()));
				stmt.addBatch();
				if (count % 200 == 0) {
					stmt.executeBatch();
				}
				count++;
			}
			stmt.executeBatch();
			conn.commit();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
			this.callCarTroubleAreaPlanMap = null;
		}
	}

	public class PlanAreaInoutInfo {
		public String id;
		public int plan_id;
		public int area_id;
		public Timestamp stat_date;
		public String TAXI_NO;
		public int BUSINESS_ID=0;

		public int in_out_type;// 进出区域类型 1 空车进重车出 2重车进空车出 3重车进重车出 4空车进空车出

		public long IN_TIME;// 进区域时间
		public long OUT_TIME;// 出区域时间
		public long first_empty_to_heavy_TIME;// 区域内首次空变重时间
		public long last_empty_to_heavy_TIME;// 区域内首次空变重时间
		public long last_heavy_to_empty_TIME;// 末次重变空时间，可能已经出区域了，时间可能是在出区域后
	}

	public static double countDistance(double lo1, double la1, double lo2,
			double la2) {
		double radLat1 = rad(la1);
		double radLat2 = rad(la2);
		double a = radLat1 - radLat2;
		double b = rad(lo1) - rad(lo2);
		double s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2), 2)
				+ Math.cos(radLat1) * Math.cos(radLat2)
				* Math.pow(Math.sin(b / 2), 2)));
		s = s * EARTH_RADIUS;
		s = Math.round(s * 100000) / 100000.0;
		return s;
	}

	private static double EARTH_RADIUS = 6378.137;

	private static double rad(double d) {
		return d * Math.PI / 180.0;
	}

	private int getByte(int status, int statusOut) {
		int type = 5;
		if (status == 0 && statusOut == 0) {
			type = 4;
		} else if (status == 0 && statusOut!=0) {
			type = 1;

		} else if (status !=0 && statusOut == 0) {
			type = 2;

		} else if (status !=0 && statusOut!=0) {
			type = 3;
		}
		else
		{
			System.out.println("status:"+status+"statusOut:"+statusOut);
		}
		return type;
	}
}
