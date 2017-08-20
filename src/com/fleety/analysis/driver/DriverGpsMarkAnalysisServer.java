package com.fleety.analysis.driver;

import java.sql.ResultSet;
import java.util.Calendar;

import server.db.DbServer;

import com.fleety.analysis.AnalysisServer;
import com.fleety.base.GeneralConst;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.fleety.util.pool.thread.BasicTask;
import com.fleety.util.pool.timer.FleetyTimerTask;

public class DriverGpsMarkAnalysisServer extends AnalysisServer {
	private TimerTask task = null;

	public boolean startServer() {
		super.startServer();
		if(!this.isRunning()){
			return this.isRunning();
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

	private void executeTask() throws Exception{
		long t = System.currentTimeMillis();
		DbHandle conn = DbServer.getSingleInstance().getConnWithUseTime(0);
		try{
			conn.setAutoCommit(false);
						
			StatementHandle stmt = conn.createStatement();
			ResultSet sets = stmt.executeQuery("select * from driver_mark_info where stat_time=to_date(to_char(sysdate,'yyyy-MM-dd')||' 00:00:00','yyyy-MM-dd hh24:mi:ss')");
			if(sets.next()){
				return ;
			}
			stmt.executeUpdate("insert into driver_mark_info(stat_time,service_no,driver_name,car_no,mdt_id,company_id,company,start_gps_mark,end_gps_mark,create_time) select to_date(to_char(sysdate,'yyyy-MM-dd')||' 00:00:00','yyyy-MM-dd hh24:mi:ss') stat_time, t1.service_no,t1.driver_name,t1.CAR_ID,t2.mdt_id,t1.term_id,t2.company_name,t1.gps_mark,t1.gps_mark,sysdate create_time from v_ana_driver_info t1,v_ana_dest_info t2  where t1.car_id=t2.dest_no and t1.car_id is not null");

			stmt.executeUpdate("update driver_mark_info  set end_gps_mark = (select gps_mark from v_ana_driver_info where service_no = driver_mark_info.service_no) where stat_time =to_date(to_char(sysdate - 1, 'yyyy-MM-dd') || ' 00:00:00','yyyy-MM-dd hh24:mi:ss')");
			conn.commit();
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

	
	private class TimerTask extends FleetyTimerTask{
		public void run(){
			DriverGpsMarkAnalysisServer.this.addExecTask(new ExecTask());
		}
	}
	
	private class ExecTask extends BasicTask{
		public boolean execute() throws Exception{
			DriverGpsMarkAnalysisServer.this.executeTask();
			return true;
		}
		
		public String getDesc(){
			return "驾驶员积分统计服务";
		}
		public Object getFlag(){
			return "DriverGpsMarkAnalysisServer";
		}
	}
}
