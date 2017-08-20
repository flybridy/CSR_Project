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

public class OrderBidAnasisServer extends AnalysisServer {
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
			
			StatementHandle stmt = conn.prepareStatement("select * from ana_order_bid_day_stat where stat_time between ? and ?");
			stmt.setTimestamp(1, sTime);
			stmt.setTimestamp(2, eTime);
			ResultSet sets = stmt.executeQuery();
			if(sets.next()){
				return ;
			}
			conn.closeStatement(stmt);
			
			
			String sql ="select car_no,count(car_no) total_num,sum(decode(flag, -1, 0, 1)) bid_num from order_bid where car_no is not null and end_time>=? and end_time<? group by car_no ";
			
			stmt = conn.prepareStatement(sql);
			stmt.setTimestamp(1, sTime);
			stmt.setTimestamp(2, eTime);
			
			RecordInfo rInfo;
			List<RecordInfo> list  = new ArrayList<RecordInfo>();
			sets = stmt.executeQuery();
			
			while(sets.next()){
				rInfo = new RecordInfo();
				rInfo.carNo = sets.getString("car_no");
				rInfo.totalNum = sets.getInt("total_num");
				rInfo.bidNum = sets.getInt("bid_num");
				list.add(rInfo);
			}
			sets.close();
			conn.closeStatement(stmt);
			
			sql = "insert into ana_order_bid_day_stat(id,stat_time,car_no,total,bid_num) values(?,?,?,?,?)";
			stmt = conn.prepareStatement(sql);
			for(int i=0;i<list.size();i++){
				rInfo = list.get(i);
				stmt.setInt(1, (int)DbServer.getSingleInstance().getAvaliableId(conn, "ana_order_bid_day_stat", "id"));
				stmt.setTimestamp(2, new Timestamp(statTime.getTimeInMillis()));
				stmt.setString(3, rInfo.carNo);
				stmt.setInt(4, rInfo.totalNum);
				stmt.setInt(5, rInfo.bidNum);

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
		public String carNo;
		public int totalNum = 0;
		public int bidNum = 0;
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
			
			System.out.println("Fire ExecTask OrderBidAnasisServer:"+GeneralConst.YYYY_MM_DD_HH.format(cal.getTime()));
			OrderBidAnasisServer.this.addExecTask(new ExecTask(cal));
		}
	}
	
	private class ExecTask extends BasicTask{
		private Calendar anaDate = null;
		public ExecTask(Calendar anaDate){
			this.anaDate = anaDate;
		}
		
		public boolean execute() throws Exception{
			OrderBidAnasisServer.this.executeTask(this.anaDate);
			return true;
		}
		
		public String getDesc(){
			return "车辆应标响应率统计日分析服务";
		}
		public Object getFlag(){
			return "OrderBidAnasisServer";
		}
	}
}

