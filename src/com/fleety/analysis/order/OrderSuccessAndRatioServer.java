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

public class OrderSuccessAndRatioServer extends AnalysisServer {
	private TimerTask task = null;
	private long preDispatchDuration = 15*60*1000;
	@Override
	public boolean startServer() {
		super.startServer();
		if(!this.isRunning()){
			return this.isRunning();
		}

		if(this.getIntegerPara("pre_dispatch_duration") != null){
			this.preDispatchDuration = this.getIntegerPara("pre_dispatch_duration").intValue()*60000;
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
			
			StatementHandle stmt = conn.prepareStatement("select * from ana_order_data_hour_stat where stat_time between ? and ?");
			stmt.setTimestamp(1, sTime);
			stmt.setTimestamp(2, eTime);
			ResultSet sets = stmt.executeQuery();
			if(sets.next()){
				return ;
			}
			conn.closeStatement(stmt);
			
			
			StringBuffer sqlBuff = new StringBuffer(1024);
			sqlBuff.append("select a.order_id,a.status,a.car_wanted_time,a.modified_time,b.flag,b.end_time from ");
			sqlBuff.append("(select * from TAXI_ORDER_LIST where car_wanted_time>=? and car_wanted_time<?) a");
			sqlBuff.append(" left join order_bid b on a.order_id=b.order_id order by order_id asc,end_time asc");
			
			stmt = conn.prepareStatement(sqlBuff.toString());
			stmt.setTimestamp(1, sTime);
			stmt.setTimestamp(2, eTime);
			
			RecordInfo[] rInfoArr = new RecordInfo[24];
			for(int i=0;i<rInfoArr.length;i++){
				rInfoArr[i] = new RecordInfo();
			}
			
			RecordInfo rInfo;
			Calendar tempCal = Calendar.getInstance();
			int preOrderId = -1,orderId,status,bidFlag,hour;
			Date carWantedTime,modifiedTime,bidActionTime;
			sets = stmt.executeQuery();
			
			while(sets.next()){
				orderId = sets.getInt("order_id");
				status = sets.getInt("status");
				bidFlag = sets.getInt("flag");
				carWantedTime = sets.getTimestamp("car_wanted_time");
				modifiedTime = sets.getTimestamp("modified_time");
				bidActionTime = sets.getTimestamp("end_time");
				
				tempCal.setTime(carWantedTime);
				hour = tempCal.get(Calendar.HOUR_OF_DAY);
				rInfo = rInfoArr[hour];
				
				if(preOrderId != orderId){
					rInfo.orderNum ++;
					if(status == 3){ 		//调派完成
						rInfo.successNum ++;
					}else if(status == 4){	//无供
						rInfo.noOfferNum ++;
						rInfo.failureDuration += Math.max(0, (modifiedTime.getTime() - carWantedTime.getTime() + this.preDispatchDuration)/1000);
					}else if(status == 5){	//取消
						rInfo.failureDuration += Math.max(0, (modifiedTime.getTime() - carWantedTime.getTime() + this.preDispatchDuration)/1000);
						rInfo.cancelNum ++;
					}else if(status == 6){	//调离
						rInfo.movedNum ++;
					}else if(status == 7){	//放空
						rInfo.userNoFinishNum ++;
					}else if(status == 11){	//完成后放空
						rInfo.driverNoFinishNum ++;
					}else{
						rInfo.otherOrderNum ++;
					}
				}
				
				if(status == 3 && bidFlag == 1){
					rInfo.dispatchSuccessDuration += Math.max(0, (bidActionTime.getTime() - carWantedTime.getTime() + this.preDispatchDuration)/1000);
				}
				
				if(bidFlag == -1){
					rInfo.briefTaxiNum ++;
				}
				if(bidActionTime != null && bidFlag != -1){
					rInfo.taxiBidNum ++;
				}
				
				
				preOrderId = orderId;
			}
			conn.closeStatement(stmt);
			
			String sql = "insert into ana_order_data_hour_stat(";
			sql += "id,stat_time,order_num,success_num,moved_num,no_offer_num,cancel_num,user_no_finish_num,driver_no_finish_num,other_order_num";
			sql += ",brief_taxi_num,taxi_bid_num,taxi_refuse_num,taxi_timeout_num,dispatch_success_duration,failure_duration";
			sql += ") values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			stmt = conn.prepareStatement(sql);
			for(int i=0;i<rInfoArr.length;i++){
				stmt.setInt(1, (int)DbServer.getSingleInstance().getAvaliableId(conn, "ana_order_data_hour_stat", "id"));
				
				statTime.set(Calendar.HOUR_OF_DAY, i);
				stmt.setTimestamp(2, new Timestamp(statTime.getTimeInMillis()));
				
				stmt.setInt(3, rInfoArr[i].orderNum);
				stmt.setInt(4, rInfoArr[i].successNum);
				stmt.setInt(5, rInfoArr[i].movedNum);
				stmt.setInt(6, rInfoArr[i].noOfferNum);
				stmt.setInt(7, rInfoArr[i].cancelNum);
				stmt.setInt(8, rInfoArr[i].userNoFinishNum);
				stmt.setInt(9, rInfoArr[i].driverNoFinishNum);
				stmt.setInt(10, rInfoArr[i].otherOrderNum);
				stmt.setInt(11, rInfoArr[i].briefTaxiNum);
				stmt.setInt(12, rInfoArr[i].taxiBidNum);
				stmt.setInt(13, rInfoArr[i].taxiRefuseNum);
				stmt.setInt(14, rInfoArr[i].briefTaxiNum-rInfoArr[i].taxiBidNum-rInfoArr[i].taxiRefuseNum);
				stmt.setInt(15, rInfoArr[i].dispatchSuccessDuration);
				stmt.setInt(16, rInfoArr[i].failureDuration);
				
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
		public int movedNum = 0;
		public int noOfferNum = 0;
		public int cancelNum = 0;
		public int userNoFinishNum = 0;
		public int driverNoFinishNum = 0;
		public int otherOrderNum = 0;
		public int briefTaxiNum = 0;
		public int taxiBidNum = 0;
		public int taxiRefuseNum = 0;
		public int dispatchSuccessDuration = 0;
		public int failureDuration = 0;
		
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
			
			System.out.println("Fire ExecTask OrderSuccessAndRatioServer:"+GeneralConst.YYYY_MM_DD_HH.format(cal.getTime()));
			OrderSuccessAndRatioServer.this.addExecTask(new ExecTask(cal));
		}
	}
	
	private class ExecTask extends BasicTask{
		private Calendar anaDate = null;
		public ExecTask(Calendar anaDate){
			this.anaDate = anaDate;
		}
		
		public boolean execute() throws Exception{
			OrderSuccessAndRatioServer.this.executeTask(this.anaDate);
			return true;
		}
		
		public String getDesc(){
			return "订单分析";
		}
		public Object getFlag(){
			return "OrderSuccessAndRatioServer";
		}
	}
}
