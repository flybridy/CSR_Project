package com.fleety.analysis.order;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import server.db.DbServer;

import com.fleety.analysis.AnalysisServer;
import com.fleety.base.GeneralConst;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.fleety.util.pool.thread.BasicTask;
import com.fleety.util.pool.timer.FleetyTimerTask;

public class OrderDayAnasisServer extends AnalysisServer {
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
		long t = System.currentTimeMillis();
		DbHandle conn = DbServer.getSingleInstance().getConnWithUseTime(0);
		try{
			conn.setAutoCommit(false);

			Calendar statTime = Calendar.getInstance();
			statTime.setTimeInMillis(anaDate.getTimeInMillis());
			
			Timestamp sTime = new Timestamp(anaDate.getTimeInMillis());
			anaDate.add(Calendar.DAY_OF_MONTH, 1);
			Timestamp eTime = new Timestamp(anaDate.getTimeInMillis());
			
			StatementHandle stmt = conn.prepareStatement("select * from ana_order_day_stat where stat_time between ? and ?");
			stmt.setTimestamp(1, sTime);
			stmt.setTimestamp(2, eTime);
			ResultSet sets = stmt.executeQuery();
			if(sets.next()){
				return ;
			}
			conn.closeStatement(stmt);
			
			
			String sql ="select to_char(car_wanted_time, 'yyyy-MM-dd') day,decode(car_company, null, 0, car_company) car_company,count(*) order_total,sum(decode(car_no, null, 0, 1)) yougong_total,sum(case when (status = 2 or status = 3) then 1 else 0 end) as FINISH_TOTAL,sum(case when status = 5 then 1 else 0 end) as cancel_total,sum(case when status = 7 then 1 else 0 end) as fangkong_total from taxi_order_list where car_wanted_time>=? and car_wanted_time<? group by to_char(car_wanted_time, 'yyyy-MM-dd'), car_company";
			
			stmt = conn.prepareStatement(sql);
			stmt.setTimestamp(1, sTime);
			stmt.setTimestamp(2, eTime);
			
			RecordInfo rInfo;
			List<RecordInfo> list  = new ArrayList<RecordInfo>();
			sets = stmt.executeQuery();
			
			while(sets.next()){
				rInfo = new RecordInfo();
				rInfo.comId = sets.getInt("car_company");
				rInfo.totalNum = sets.getInt("order_total");
				rInfo.receiveNum = sets.getInt("yougong_total");
				rInfo.finishNum = sets.getInt("FINISH_TOTAL");
				rInfo.cancelNum = sets.getInt("cancel_total");
				rInfo.fangkongNum = sets.getInt("fangkong_total");
				list.add(rInfo);
			}
			sets.close();
			conn.closeStatement(stmt);
			
			sql = "insert into ana_order_day_stat(id,stat_time,car_company,total,yougong_num,finish_num,cancel_num,fangkong_num) values(?,?,?,?,?,?,?,?)";
			stmt = conn.prepareStatement(sql);
			for(int i=0;i<list.size();i++){
				rInfo = list.get(i);
				stmt.setInt(1, (int)DbServer.getSingleInstance().getAvaliableId(conn, "ana_order_day_stat", "id"));
				stmt.setTimestamp(2, new Timestamp(statTime.getTimeInMillis()));
				stmt.setInt(3, rInfo.comId);
				stmt.setInt(4, rInfo.totalNum);
				stmt.setInt(5, rInfo.receiveNum);
				
				stmt.setInt(6, rInfo.finishNum);
				stmt.setInt(7, rInfo.cancelNum);
				stmt.setInt(8, rInfo.fangkongNum);
				stmt.addBatch();
			}
			stmt.executeBatch();
	
			conn.closeStatement(stmt);
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
	

	private class RecordInfo{
		public int comId = 0;
		public int receiveNum = 0;
		public int finishNum = 0;
		public int totalNum = 0;
		public int cancelNum = 0;
		public int fangkongNum = 0;
		public int wugongNum = 0;
		
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
			
			System.out.println("Fire ExecTask OrderDayAnasisServer:"+GeneralConst.YYYY_MM_DD_HH.format(cal.getTime()));
			OrderDayAnasisServer.this.addExecTask(new ExecTask(cal));
		}
	}
	
	private class ExecTask extends BasicTask{
		private Calendar anaDate = null;
		public ExecTask(Calendar anaDate){
			this.anaDate = anaDate;
		}
		
		public boolean execute() throws Exception{
			OrderDayAnasisServer.this.executeTask(this.anaDate);
			return true;
		}
		
		public String getDesc(){
			return "电召订单日分析服务";
		}
		public Object getFlag(){
			return "OrderDayAnasisServer";
		}
	}
}
