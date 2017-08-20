package com.fleety.server;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;

import server.db.DbServer;
import server.threadgroup.PoolInfo;
import server.threadgroup.ThreadPoolGroupServer;

import com.fleety.analysis.AnalysisServer;
import com.fleety.base.GeneralConst;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.fleety.util.pool.thread.BasicTask;
import com.fleety.util.pool.thread.ThreadPool;

public class BetchDriverAnasisServer extends AnalysisServer {

	private ThreadPool pool = null;
	private String threadPoolName = "BetchDriverAnasisServer_pool";
	
	public boolean startServer() {
		super.startServer();
		if(!this.isRunning()){
			return this.isRunning();
		}		
		
		
		
		System.out.println("--------------------start execute BetchDriverAnasisServer ----------------------------------");
		try {
			PoolInfo pInfo = new PoolInfo();
			pInfo.workersNumber = 1;
			pInfo.taskCapacity = 1000;
			pool = ThreadPoolGroupServer.getSingleInstance().createThreadPool(threadPoolName, pInfo);
			
			Calendar cal = Calendar.getInstance();
			cal.setTime(GeneralConst.YYYY_MM_DD.parse("2014-01-05"));
			cal.set(Calendar.HOUR_OF_DAY, 0);
			cal.set(Calendar.MINUTE, 0);
			cal.set(Calendar.SECOND, 0);
			cal.set(Calendar.MILLISECOND, 0);
			
			Calendar end = Calendar.getInstance();
			end.setTime(GeneralConst.YYYY_MM_DD.parse("2014-10-10"));
			end.set(Calendar.HOUR_OF_DAY, 0);
			end.set(Calendar.MINUTE, 0);
			end.set(Calendar.SECOND, 0);
			end.set(Calendar.MILLISECOND, 0);
			
			while(cal.getTime().before(end.getTime())){
				
//				this.executeTask(cal);
				Calendar temp = (Calendar)cal.clone();
				
				this.pool.addTask(new ExecuteTask(temp));
				
				cal.add(Calendar.DAY_OF_MONTH, 1);
				
				Thread.sleep(30 * 1000);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return true;
	}
	
	public void stopServer(){
		super.stopServer();
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
	
	private class ExecuteTask extends BasicTask {
		private Calendar cal = null;

		public ExecuteTask(Calendar cal) {
			this.cal = cal;
		}

		public boolean execute() throws Exception {
			executeTask(cal);
			return true;
		}
		
		private void executeTask(Calendar anaDate) throws Exception{
			System.out.println("--------------------start execute date:"+GeneralConst.YYYY_MM_DD.format(anaDate.getTime()));
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
				
				sql ="select * from single_business_data_bs where DATE_UP >= to_date('"+GeneralConst.YYYY_MM_DD_HH_MM_SS.format(sTime)+"','yyyy-MM-dd HH24:mi:ss') and DATE_UP <= to_date('"+GeneralConst.YYYY_MM_DD.format(sTime)+" 23:59:59','yyyy-MM-dd HH24:mi:ss') order by car_no,DATE_UP";
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

	}

}

