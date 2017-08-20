package com.fleety.analysis.order;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;

import server.db.DbServer;

import com.fleety.analysis.AnalysisServer;
import com.fleety.base.GeneralConst;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.fleety.util.pool.thread.BasicTask;
import com.fleety.util.pool.timer.FleetyTimerTask;

public class OrderTypeAnasisServer extends AnalysisServer {
	private TimerTask task = null;
	private int mobileWeixinOperator[] = {0};
	private int netOperator[] = {0};
	
	private static final int PHONE_TYPE = 0;
	private static final int MOBILE_APP_TYPE = 1;
	private static final int QQ_TYPE =2;
	private static final int WEIXIN_TYPE = 3;
	private static final int NET_TYPE = 4;

	public boolean startServer() {
		super.startServer();
		if(!this.isRunning()){
			return this.isRunning();
		}		
		
		int hour = this.getIntegerPara("hour").intValue();
		int minute = this.getIntegerPara("minute").intValue();
		
		String temp = this.getStringPara("mobile_and_weixin_operator");
		if(temp != null && !"".equals(temp)){
			String []arr = temp.split(",");
			mobileWeixinOperator = new int [arr.length];
			for(int i = 0;i<arr.length;i++){
				mobileWeixinOperator[i] = Integer.parseInt(arr[i]);
			}
		}
		
		temp = this.getStringPara("net_operator");
		if(temp != null && !"".equals(temp)){
			String []arr = temp.split(",");
			netOperator = new int [arr.length];
			for(int i = 0;i<arr.length;i++){
				netOperator[i] = Integer.parseInt(arr[i]);
			}
		}

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
			
			StatementHandle stmt = conn.prepareStatement("select * from ana_order_type_stat where stat_time between ? and ?");
			stmt.setTimestamp(1, sTime);
			stmt.setTimestamp(2, eTime);
			ResultSet sets = stmt.executeQuery();
			if(sets.next()){
				return ;
			}
			conn.closeStatement(stmt);
			
			
			String sql ="select * from TAXI_ORDER_LIST where car_wanted_time>=? and car_wanted_time<?";
			
			stmt = conn.prepareStatement(sql);
			stmt.setTimestamp(1, sTime);
			stmt.setTimestamp(2, eTime);
			
			RecordInfo[] rInfoArr = new RecordInfo[5];
			for(int i=0;i<rInfoArr.length;i++){
				rInfoArr[i] = new RecordInfo();
				rInfoArr[i].orderType = i;
			}
			
			RecordInfo rInfo;
			int status,operoterId;
			String specialRemark="",customerName="";
			sets = stmt.executeQuery();
			
			while(sets.next()){
				operoterId = sets.getInt("user_id");
				status = sets.getInt("status");
				specialRemark = sets.getString("special_remark");	
				customerName = sets.getString("customer_name");
				
				if("QQ约车订单".equals(specialRemark)){
					rInfo = rInfoArr[QQ_TYPE];
				}else if("微信约车订单".equals(specialRemark)){
					rInfo = rInfoArr[WEIXIN_TYPE];
				}else if(isPhoneOrder(operoterId)){
					if(customerName != null && customerName.indexOf("微信用户")>=0){
						rInfo = rInfoArr[WEIXIN_TYPE];
					}else{
						rInfo = rInfoArr[MOBILE_APP_TYPE];
					}
				}else if(isNetOrder(operoterId)){
					rInfo = rInfoArr[NET_TYPE];
				}else{
					rInfo = rInfoArr[PHONE_TYPE];
				}
				
				rInfo.totalNum++;
				switch(status){
					case 3:
						rInfo.successNum++;
						break;
					case 4:
						rInfo.wugongNum++;
						break;
					case 5:
						rInfo.cancelNum++;
						break;
					case 7:
						rInfo.fangkongNum++;
						break;
				}
			}
			conn.closeStatement(stmt);
			
			sql = "insert into ana_order_type_stat(id,stat_time,order_type,total_num,success_num,wugong_num,cancel_num,fangkong_num) values(?,?,?,?,?,?,?,?)";
			stmt = conn.prepareStatement(sql);
			for(int i=0;i<rInfoArr.length;i++){
				stmt.setInt(1, (int)DbServer.getSingleInstance().getAvaliableId(conn, "ana_order_type_stat", "id"));
				stmt.setTimestamp(2, new Timestamp(statTime.getTimeInMillis()));
				stmt.setInt(3, rInfoArr[i].orderType);
				stmt.setInt(4, rInfoArr[i].totalNum);
				stmt.setInt(5, rInfoArr[i].successNum);
				stmt.setInt(6, rInfoArr[i].wugongNum);
				stmt.setInt(7, rInfoArr[i].cancelNum);
				stmt.setInt(8, rInfoArr[i].fangkongNum);
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
	
	private boolean isPhoneOrder(int operatorId){
		boolean result = false;
		for(int i = 0;i<mobileWeixinOperator.length;i++){
			if(mobileWeixinOperator[i] == operatorId){
				result =  true;
				break;
			}
		}
		return result;
	}
	
	private boolean isNetOrder(int operatorId){
		boolean result = false;
		for(int i = 0;i<netOperator.length;i++){
			if(netOperator[i] == operatorId){
				result =  true;
				break;
			}
		}
		return result;
	}
	

	private class RecordInfo{
		public int orderType = 0;
		public int totalNum = 0;
		public int successNum = 0;
		public int wugongNum = 0;
		public int cancelNum = 0;
		public int fangkongNum = 0;
		
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
			
			System.out.println("Fire ExecTask OrderTypeAnasisServer:"+GeneralConst.YYYY_MM_DD_HH.format(cal.getTime()));
			OrderTypeAnasisServer.this.addExecTask(new ExecTask(cal));
		}
	}
	
	private class ExecTask extends BasicTask{
		private Calendar anaDate = null;
		public ExecTask(Calendar anaDate){
			this.anaDate = anaDate;
		}
		
		public boolean execute() throws Exception{
			OrderTypeAnasisServer.this.executeTask(this.anaDate);
			return true;
		}
		
		public String getDesc(){
			return "订单类型分析";
		}
		public Object getFlag(){
			return "OrderTypeAnasisServer";
		}
	}
}
