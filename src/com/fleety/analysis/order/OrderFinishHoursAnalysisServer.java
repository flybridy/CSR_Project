package com.fleety.analysis.order;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import server.db.DbServer;

import com.fleety.analysis.AnalysisServer;
import com.fleety.analysis.track.DestInfo;
import com.fleety.base.GeneralConst;
import com.fleety.server.GlobalUtilServer;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.fleety.util.pool.thread.BasicTask;
import com.fleety.util.pool.timer.FleetyTimerTask;

public class OrderFinishHoursAnalysisServer extends AnalysisServer {
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

	private void executeTask(Calendar anaDate) throws Exception{
		if(!this.isFinishDurationDataHourStat(anaDate)){
			this.addFinishDurationDataHourStat(anaDate);
		}
	}
	private void addFinishDurationDataHourStat(Calendar anaDate) throws SQLException{
		long t = System.currentTimeMillis();
		DbHandle conn = DbServer.getSingleInstance().getConnWithUseTime(0);
		try{
			conn.setAutoCommit(false);

			Calendar statTime = Calendar.getInstance();
			statTime.setTimeInMillis(anaDate.getTimeInMillis());
			
			Timestamp eTime = new Timestamp(anaDate.getTimeInMillis());
			anaDate.add(Calendar.DAY_OF_MONTH, -1);
			Timestamp sTime = new Timestamp(anaDate.getTimeInMillis());
			StatementHandle stmt = conn.prepareStatement("select to_date(created_time_t,'yyyy-MM-dd hh24:mi:ss') stat_time,sum((feedback_time-created_time)*24*60*60) duration,count(1) finish_num  from (select order_id,car_no,created_time,feedback_time, is_immediate,to_char(created_time,'yyyy-MM-dd hh24:')||(case when to_number(to_char(created_time,'mi'))>=30 then '30' else '00' end) created_time_t from taxi_order_list where car_no is not null and created_time between ? and ? ) group by created_time_t order by stat_time");
			stmt.setTimestamp(1, sTime);
			stmt.setTimestamp(2, eTime);
			ResultSet sets = stmt.executeQuery();
			
			String sql = "insert into ana_finish_duration_halfhour(id,stat_time,finish_num,duration,record_time) values(?,?,?,?,?)";
			StatementHandle stmt1 = conn.prepareStatement(sql);
			int count = 0;
			while(sets.next()){
				stmt1.setInt(1, (int)DbServer.getSingleInstance().getAvaliableId(conn, "ana_finish_duration_halfhour", "id"));
				stmt1.setTimestamp(2, sets.getTimestamp("stat_time"));
				stmt1.setInt(3, sets.getInt("finish_num"));
				stmt1.setInt(4, sets.getInt("duration"));
				stmt1.setTimestamp(5, new Timestamp(new Date().getTime()));
				stmt1.addBatch();
				count++;
				if(count % 200 == 0){
					stmt1.executeBatch();
				}
			}
			stmt1.executeBatch();
	
			conn.closeStatement(stmt);
			conn.closeStatement(stmt1);
			conn.commit();
		}catch(Exception e){
			conn.rollback();
			e.printStackTrace();
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
			
			System.out.println("Exec Duration:"+(System.currentTimeMillis() - t));
		}
	}
	private boolean isFinishDurationDataHourStat(Calendar anaDate){
		DbHandle conn = DbServer.getSingleInstance().getConnWithUseTime(0);
		try{
			Calendar statTime = Calendar.getInstance();
			statTime.setTimeInMillis(anaDate.getTimeInMillis());
			
			Timestamp sTime = new Timestamp(anaDate.getTimeInMillis());
			anaDate.add(Calendar.DAY_OF_MONTH, 1);
			Timestamp eTime = new Timestamp(anaDate.getTimeInMillis());
			
			StatementHandle stmt = conn.prepareStatement("select * from ana_finish_duration_halfhour where stat_time between ? and ? ");
			stmt.setTimestamp(1, sTime);
			stmt.setTimestamp(2, eTime);
			ResultSet sets = stmt.executeQuery();
			if(sets.next()){
				return true;
			}
			conn.closeStatement(stmt);
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
		return false;
	}
	
	private class TimerTask extends FleetyTimerTask{
		public void run(){
			Calendar cal = Calendar.getInstance();
			cal.add(Calendar.DAY_OF_MONTH, -1);
			cal.set(Calendar.HOUR_OF_DAY, 0);
			cal.set(Calendar.MINUTE, 0);
			cal.set(Calendar.SECOND, 0);
			cal.set(Calendar.MILLISECOND, 0);
			
			System.out.println("Fire ExecTask OrderFinishHoursAnalysisServer:"+GeneralConst.YYYY_MM_DD_HH.format(cal.getTime()));
			OrderFinishHoursAnalysisServer.this.addExecTask(new ExecTask(cal));
		}
	}
	
	private class ExecTask extends BasicTask{
		private Calendar anaDate = null;
		public ExecTask(Calendar anaDate){
			this.anaDate = anaDate;
		}
		
		public boolean execute() throws Exception{
			OrderFinishHoursAnalysisServer.this.executeTask(this.anaDate);
			return true;
		}
		
		public String getDesc(){
			return "电召业务完成时长分析服务";
		}
		public Object getFlag(){
			return "OrderFinishHoursAnalysisServer";
		}
	}
}
