package com.fleety.analysis.order;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;

import server.db.DbServer;

import com.fleety.analysis.AnalysisServer;
import com.fleety.base.GeneralConst;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.fleety.util.pool.thread.BasicTask;
import com.fleety.util.pool.timer.FleetyTimerTask;

public class OrderServiceAnasisServer extends AnalysisServer {
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
			
			StatementHandle stmt = conn.prepareStatement("select * from ana_servicde_data_hour_stat where stat_time between ? and ?");
			stmt.setTimestamp(1, sTime);
			stmt.setTimestamp(2, eTime);
			ResultSet sets = stmt.executeQuery();
			if(sets.next()){
				return ;
			}
			conn.closeStatement(stmt);
			
			
			String sql ="select order_id,status,car_wanted_time from TAXI_ORDER_LIST where car_wanted_time>=? and car_wanted_time<?";
			
			stmt = conn.prepareStatement(sql);
			stmt.setTimestamp(1, sTime);
			stmt.setTimestamp(2, eTime);
			
			RecordInfo[] rInfoArr = new RecordInfo[24];
			for(int i=0;i<rInfoArr.length;i++){
				rInfoArr[i] = new RecordInfo();
			}
			
			RecordInfo rInfo;
			Calendar tempCal = Calendar.getInstance();
			int preOrderId = -1,orderId,status,hour;
			Date carWantedTime,recodeTime;
			sets = stmt.executeQuery();
			
			while(sets.next()){
				orderId = sets.getInt("order_id");
				status = sets.getInt("status");
				carWantedTime = sets.getTimestamp("car_wanted_time");
				
				tempCal.setTime(carWantedTime);
				hour = tempCal.get(Calendar.HOUR_OF_DAY);
				rInfo = rInfoArr[hour];
				
				if(preOrderId != orderId){
					rInfo.orderNum ++;
					if(status == 3){ 		//调派完成
						rInfo.successNum ++;
					}
				}

				preOrderId = orderId;
			}
			sets.close();
			conn.closeStatement(stmt);
			
			sql ="select recode_time from single_business_data_bs  where recode_time>=? and recode_time<?";
			
			stmt = conn.prepareStatement(sql);
			stmt.setTimestamp(1, sTime);
			stmt.setTimestamp(2, eTime);
			sets = stmt.executeQuery();
			while(sets.next()){
				recodeTime = sets.getTimestamp("recode_time");
				
				tempCal.setTime(recodeTime);
				hour = tempCal.get(Calendar.HOUR_OF_DAY);
				rInfo = rInfoArr[hour];
				rInfo.totalNum ++;
			}
			conn.closeStatement(stmt);
			
			sql = "insert into ana_servicde_data_hour_stat(id,stat_time,total_num,order_num,success_num) values(?,?,?,?,?)";
			stmt = conn.prepareStatement(sql);
			for(int i=0;i<rInfoArr.length;i++){
				stmt.setInt(1, (int)DbServer.getSingleInstance().getAvaliableId(conn, "ana_servicde_data_hour_stat", "id"));
				
				statTime.set(Calendar.HOUR_OF_DAY, i);
				stmt.setTimestamp(2, new Timestamp(statTime.getTimeInMillis()));
				stmt.setInt(3, rInfoArr[i].totalNum);
				stmt.setInt(4, rInfoArr[i].orderNum);
				stmt.setInt(5, rInfoArr[i].successNum);
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
		public int orderNum = 0;
		public int successNum = 0;
		public int totalNum = 0;
		
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
			
			System.out.println("Fire ExecTask OrderServiceAnasisServer:"+GeneralConst.YYYY_MM_DD_HH.format(cal.getTime()));
			OrderServiceAnasisServer.this.addExecTask(new ExecTask(cal));
		}
	}
	
	private class ExecTask extends BasicTask{
		private Calendar anaDate = null;
		public ExecTask(Calendar anaDate){
			this.anaDate = anaDate;
		}
		
		public boolean execute() throws Exception{
			OrderServiceAnasisServer.this.executeTask(this.anaDate);
			return true;
		}
		
		public String getDesc(){
			return "电召服务率分析";
		}
		public Object getFlag(){
			return "OrderServiceAnasisServer";
		}
	}
}
