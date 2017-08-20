package com.fleety.server;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;

import server.db.DbServer;
import server.threadgroup.PoolInfo;
import server.threadgroup.ThreadPoolGroupServer;
import server.track.TrackServer;

import com.fleety.analysis.AnalysisServer;
import com.fleety.base.GeneralConst;
import com.fleety.base.InfoContainer;
import com.fleety.track.TrackIO;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.fleety.util.pool.thread.BasicTask;
import com.fleety.util.pool.thread.ITask;
import com.fleety.util.pool.thread.ThreadPool;
import com.fleety.util.pool.timer.FleetyTimerTask;

public class BusinessGuSuanAnalysisServer extends AnalysisServer {
	public final double degreeToMeter = 111110.0; // 单位m
	private TimerTask task = null;
	private ThreadPool pool = null;

	public boolean startServer() {
		super.startServer();
		if(!this.isRunning()){
			return this.isRunning();
		}		
		
		PoolInfo pInfo = new PoolInfo();
		pInfo.workersNumber = 5;
		pInfo.poolType = ThreadPool.SINGLE_TASK_LIST_POOL;
		pInfo.taskCapacity = 100000;
		try {
			this.pool = ThreadPoolGroupServer.getSingleInstance().createThreadPool("business_data_update_gusuandistance", pInfo);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		int hour = this.getIntegerPara("hour").intValue();
		int minute = this.getIntegerPara("minute").intValue();

		Calendar cal = this.getNextExecCalendar(hour, minute);
		if(cal.get(Calendar.DAY_OF_MONTH) != Calendar.getInstance().get(Calendar.DAY_OF_MONTH)){
			this.scheduleTask(new TimerTask(), 500);
		}
		long delay = cal.getTimeInMillis() - System.currentTimeMillis();
		this.isRunning = this.scheduleTask(this.task = new TimerTask(), delay, GeneralConst.ONE_DAY_TIME);
		
		return this.isRunning();
	}
	
	public void stopServer(){
		if(this.task != null){
			this.task.cancel();
		}
		super.stopServer();
	}
	
	private void countDistance(RecordInfo rInfo, InfoContainer[] infos){
		InfoContainer info;
		boolean isFirst=true;
		double preLo = -1,preLa = -1;
		
		for (int i = 0; i < infos.length; i++) {
			info = infos[i];
			double lo = info.getDouble(TrackIO.DEST_LO_FLAG).doubleValue();
			double la = info.getDouble(TrackIO.DEST_LA_FLAG).doubleValue();
			Date gpsTime = info.getDate(TrackIO.DEST_TIME_FLAG);
			if(gpsTime.after(rInfo.dateUp) && gpsTime.before(rInfo.dateDown)){
				if(!isFirst){
					double x = lo - preLo;
					double y = la - preLa;
					double tempDistance = Math.sqrt(x * x + y * y) * degreeToMeter;
					rInfo.gusuanDistance = rInfo.gusuanDistance + tempDistance;
				}else{
					isFirst = false;
				}
				preLo = lo;
				preLa = la;
			}
		}
		rInfo.gusuanDistance = rInfo.gusuanDistance/1000.0;
	}

	private void executeTask(Calendar anaDate) throws Exception{
		long t = System.currentTimeMillis();
		DbHandle conn = DbServer.getSingleInstance().getConnWithUseTime(0);
		try{
			Calendar statTime = Calendar.getInstance();
			statTime.setTimeInMillis(anaDate.getTimeInMillis());
			
			Calendar trackTime = Calendar.getInstance();
			trackTime.setTimeInMillis(anaDate.getTimeInMillis());
			
			Timestamp sTime = new Timestamp(anaDate.getTimeInMillis());
			
			String sql ="select * from single_business_data_bs where gusuan_distance>0 and recode_time >= to_date('"+GeneralConst.YYYY_MM_DD_HH_MM_SS.format(sTime)+"','yyyy-MM-dd HH24:mi:ss') and recode_time <= to_date('"+GeneralConst.YYYY_MM_DD.format(sTime)+" 23:59:59','yyyy-MM-dd HH24:mi:ss') ";

			StatementHandle stmt = conn.createStatement();
			ResultSet sets = stmt.executeQuery(sql);
			
			if(sets.next()){
				return ;
			}
			conn.closeStatement(stmt);
			
			sql ="select * from single_business_data_bs where recode_time >= to_date('"+GeneralConst.YYYY_MM_DD_HH_MM_SS.format(sTime)+"','yyyy-MM-dd HH24:mi:ss') and recode_time <= to_date('"+GeneralConst.YYYY_MM_DD.format(sTime)+" 23:59:59','yyyy-MM-dd HH24:mi:ss') order by car_no";
			stmt = conn.createStatement();
			
			ArrayList infoList = new ArrayList();
			
			RecordInfo rInfo = null;
			Calendar tempCal = Calendar.getInstance();
			int id=0;
			Date dateUp = null , dateDown = null;
			String carNo = "", tempCarNo="";
			InfoContainer[] infos = null;
			InfoContainer queryInfo = new InfoContainer();
			Calendar startTime = Calendar.getInstance();
			startTime.setTimeInMillis(trackTime.getTimeInMillis());
			startTime.add(Calendar.MINUTE, -60);
			
			Calendar endTime = Calendar.getInstance();
			endTime.setTimeInMillis(trackTime.getTimeInMillis());
			endTime.add(Calendar.DAY_OF_MONTH, 1);
			endTime.add(Calendar.MINUTE, 60);
			queryInfo.setInfo(TrackServer.START_DATE_FLAG,startTime.getTime());
			queryInfo.setInfo(TrackServer.END_DATE_FLAG, endTime.getTime());
			sets = stmt.executeQuery(sql);
			while(sets.next()){
				id = sets.getInt("id");
				carNo = sets.getString("car_no");
				if(tempCarNo.equals("") || !tempCarNo.equals(carNo)){
					tempCarNo = sets.getString("car_no");
					queryInfo.setInfo(TrackServer.DEST_NO_FLAG, tempCarNo);
					infos = TrackServer.getSingleInstance().getTrackInfo(queryInfo);
				}
				dateUp = sets.getTimestamp("DATE_UP");
				dateDown = sets.getTimestamp("DATE_DOWN");
				rInfo = new RecordInfo();
				rInfo.id = id;
				rInfo.carNo = carNo;
				rInfo.dateUp = dateUp;
				rInfo.dateDown = dateDown;
				rInfo.carNo = carNo;
				
				this.countDistance(rInfo, infos);
				infoList.add(rInfo);
				if(infoList.size()==500){
					ArrayList temp = new ArrayList();
					temp.addAll(infoList);
					infoList.clear();
					pool.addTask(new UpdateTask(temp));
				}
			}
			pool.addTask(new UpdateTask(infoList));
			
		}catch(Exception e){
			if(conn != null){
				conn.rollback();
			}
			throw e;
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
			
			System.out.println("Exec Duration:"+(System.currentTimeMillis() - t));
		}
	}
	

	private class RecordInfo{
		public int id=0;
		public Date dateUp=null;
		public Date dateDown = null;
		public double gusuanDistance = 0;
		public String carNo = "";
		
		public RecordInfo(){
			
		}
	}
	
	private class TimerTask extends FleetyTimerTask{
		public void run(){
			Calendar cal = Calendar.getInstance();
			cal.add(Calendar.DAY_OF_MONTH, -1);
			cal.set(Calendar.HOUR_OF_DAY, 0);
			cal.set(Calendar.MINUTE, 0);
			cal.set(Calendar.SECOND, 0);
			cal.set(Calendar.MILLISECOND, 0);
			
			System.out.println("Fire ExecTask BusinessGuSuanAnalysisServer:"+GeneralConst.YYYY_MM_DD_HH.format(cal.getTime()));
			BusinessGuSuanAnalysisServer.this.addExecTask(new ExecTask(cal));
		}
	}
	
	private class ExecTask extends BasicTask{
		private Calendar anaDate = null;
		public ExecTask(Calendar anaDate){
			this.anaDate = anaDate;
		}
		
		public boolean execute() throws Exception{
			BusinessGuSuanAnalysisServer.this.executeTask(this.anaDate);
			return true;
		}
		
		public String getDesc(){
			return "营运里程与估计对比统计服务";
		}
		public Object getFlag(){
			return "BusinessGuSuanAnalysisServer";
		}
	}
	
	private class UpdateTask implements ITask {
		private ArrayList list;

		public UpdateTask(ArrayList list) {
			this.list = list;
		}

		public boolean execute() throws Exception {
			if (this.list == null) {
				return false;
			}

			DbHandle conn = DbServer.getSingleInstance().getConnWithUseTime(0);
			try {
				StatementHandle stmt = conn.createStatement();
				String sql = "update single_business_data_bs set gusuan_distance=? where id=?";
				stmt = conn.prepareStatement(sql);
				RecordInfo rInfo = null;
				for(int i=0;i<list.size();i++){
					rInfo = (RecordInfo)list.get(i);
					stmt.setDouble(1, rInfo.gusuanDistance);
					stmt.setInt(2, rInfo.id);
					stmt.addBatch();
				}
				stmt.executeBatch();

			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				DbServer.getSingleInstance().releaseConn(conn);
			}
			return true;
		}

		public String getDesc() {
			return null;
		}

		public Object getFlag() {
			return null;
		}
	}
}
