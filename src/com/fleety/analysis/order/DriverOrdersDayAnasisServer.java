package com.fleety.analysis.order;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;

import server.db.DbServer;
import server.track.TrackServer;

import com.fleety.analysis.AnalysisServer;
import com.fleety.base.GeneralConst;
import com.fleety.base.InfoContainer;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.fleety.util.pool.thread.BasicTask;
import com.fleety.util.pool.timer.FleetyTimerTask;

public class DriverOrdersDayAnasisServer extends AnalysisServer {
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
			
			Timestamp sTime = new Timestamp(anaDate.getTimeInMillis());
			
			String sql ="select * from DRIVER_ORDERS_STAT where STAT_TIME >= to_date('"+GeneralConst.YYYY_MM_DD.format(sTime)+"','yyyy-MM-dd') and STAT_TIME <= to_date('"+GeneralConst.YYYY_MM_DD.format(sTime)+" 23:59:59','yyyy-MM-dd HH24:mi:ss')";

			StatementHandle stmt = conn.createStatement();
			ResultSet sets = stmt.executeQuery(sql);
			
			if(sets.next()){
				return ;
			}
			
			sets.close();
			
			sql = "select * from taxi_order_list where car_no is not null and car_wanted_time>= to_date('"+GeneralConst.YYYY_MM_DD_HH_MM_SS.format(sTime)+"','yyyy-MM-dd HH24:mi:ss') and car_wanted_time <= to_date('"+GeneralConst.YYYY_MM_DD.format(sTime)+" 23:59:59','yyyy-MM-dd HH24:mi:ss') and order_id not in (select order_id from radio_order_info)order by car_no";
			
			sets = stmt.executeQuery(sql);
			OrderInfo orderInfo = null;
			ArrayList orderList = new ArrayList();
			while(sets.next()){
				orderInfo = new OrderInfo();
				orderInfo.carNo = sets.getString("car_no");
				orderInfo.carWantedTime = sets.getTimestamp("car_wanted_time");
				orderList.add(orderInfo);
			}
			sets.close();
			
			sql ="select * from single_business_data_bs where recode_time >= to_date('"+GeneralConst.YYYY_MM_DD_HH_MM_SS.format(sTime)+"','yyyy-MM-dd HH24:mi:ss') and recode_time <= to_date('"+GeneralConst.YYYY_MM_DD.format(sTime)+" 23:59:59','yyyy-MM-dd HH24:mi:ss') order by car_no,recode_time";
			stmt = conn.createStatement();
			
			ArrayList infoList = new ArrayList();
			
			RecordInfo rInfo = null;
			Calendar tempCal = Calendar.getInstance();
			Timestamp startTime = null , endTime = null;
			String carNo = "", tempServiceNo="",serviceNo;
			int taxiCompany = 0;
			sets = stmt.executeQuery(sql);
			while(sets.next()){
				carNo = sets.getString("car_no");
				serviceNo = sets.getString("service_no");
				taxiCompany = sets.getInt("taxi_company");
				startTime = sets.getTimestamp("recode_time");
				endTime = sets.getTimestamp("recode_time");
				if(tempServiceNo.equals("") || !tempServiceNo.equals(serviceNo)){
					if(!tempServiceNo.equals("")){
						for(int j=0;j<orderList.size();j++){
							orderInfo = (OrderInfo)orderList.get(j);
							if(orderInfo.carNo.equals(rInfo.carNo) && orderInfo.carWantedTime.after(rInfo.startTime) && orderInfo.carWantedTime.before(rInfo.endTime)){
								rInfo.orderNum++;
							}
						}
						if(rInfo.orderNum>0){
							infoList.add(rInfo);
						}
					}
					tempServiceNo = sets.getString("service_no");
					rInfo = new RecordInfo();
					rInfo.carNo = carNo;
					rInfo.serviceNo = serviceNo;
					rInfo.taxiCompany = taxiCompany;
					rInfo.startTime = startTime;
					rInfo.endTime = endTime;
				}else{
					rInfo.endTime = endTime;
				}
			}
			
			if(rInfo != null){
				for(int j=0;j<orderList.size();j++){
					orderInfo = (OrderInfo)orderList.get(j);
					if(orderInfo.carNo.equals(rInfo.carNo) && orderInfo.carWantedTime.after(rInfo.startTime) && orderInfo.carWantedTime.before(rInfo.endTime)){
						rInfo.orderNum++;
					}
				}
				if(rInfo.orderNum>0){
					infoList.add(rInfo);
				}
			}

			sql = "insert into DRIVER_ORDERS_STAT(id,CAR_NO,SERVICE_NO,TAXI_COMPANY,START_TIME,END_TIME,ORDER_NUM,stat_time) values(?,?,?,?,?,?,?,?)";
			stmt = conn.prepareStatement(sql);
			long id = DbServer.getSingleInstance().getAvaliableId(conn, "DRIVER_ORDERS_STAT", "id", true);
			for(int i = 0; i < infoList.size(); i ++)
			{
				rInfo = (RecordInfo) infoList.get(i);
				stmt.setLong(1, id ++);
				stmt.setString(2, rInfo.carNo);
				stmt.setString(3, rInfo.serviceNo);
				stmt.setInt(4, rInfo.taxiCompany);
				stmt.setTimestamp(5, rInfo.startTime);
				stmt.setTimestamp(6, rInfo.endTime);
				stmt.setInt(7, rInfo.orderNum);
				stmt.setTimestamp(8, sTime);
				stmt.addBatch();
				if(i % 200 ==0){
					stmt.executeBatch();
				}
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
		public String serviceNo = "";
		public String carNo = "";
		public int taxiCompany = 0;
		public Timestamp startTime = null;
		public Timestamp endTime = null;
		public RecordInfo(){
			
		}
	}
	
	private class OrderInfo{
		public String carNo = "";
		public Timestamp carWantedTime = null;
		public OrderInfo(){
			
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
			
			System.out.println("Fire ExecTask DriverOrdersDayAnasisServer:"+GeneralConst.YYYY_MM_DD_HH.format(cal.getTime()));
			DriverOrdersDayAnasisServer.this.addExecTask(new ExecTask(cal));
		}
	}
	
	private class ExecTask extends BasicTask{
		private Calendar anaDate = null;
		public ExecTask(Calendar anaDate){
			this.anaDate = anaDate;
		}
		
		public boolean execute() throws Exception{
			DriverOrdersDayAnasisServer.this.executeTask(this.anaDate);
			return true;
		}
		
		public String getDesc(){
			return "驾驶员接单数日统计分析服务";
		}
		public Object getFlag(){
			return "DriverOrdersDayAnasisServer";
		}
	}
}
