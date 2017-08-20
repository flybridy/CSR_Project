package com.fleety.analysis.order;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;

import server.db.DbServer;
import server.var.VarManageServer;

import com.fleety.analysis.AnalysisServer;
import com.fleety.analysis.track.task.CallCarTroubleAreaDataServer.SingleBusinessDataBs;
import com.fleety.base.GeneralConst;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.fleety.util.pool.thread.BasicTask;
import com.fleety.util.pool.timer.FleetyTimerTask;

public class DriverOrdersAllDayAnasisServer extends AnalysisServer {
	private TimerTask task = null;
	private long relate = 5*60 * 1000l;
	private boolean is_plat = false;
	private HashMap singleBusinessDataBsMap = null;
	public final static int STATUS_FANGKONG = 7;// 已放空
	public final static int FANGKONG_SJ = 1;// 司机放空

	public boolean startServer() {
		super.startServer();
		if(!this.isRunning()){
			return this.isRunning();
		}		
		
		int hour = this.getIntegerPara("hour").intValue();
		int minute = this.getIntegerPara("minute").intValue();
		String temp = this.getStringPara("relate");
		if(temp!=null&&!temp.equals("")){
			this.relate = Integer.valueOf(temp)*60*1000l;
		}
		temp = this.getStringPara("is_plat");
		if(temp!=null&&!temp.equals("")){
			this.is_plat = Boolean.valueOf(temp);
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
		String phone_acount = VarManageServer.getSingleInstance().getVarStringValue("phone_acount");
		long t = System.currentTimeMillis();
		DbHandle conn = DbServer.getSingleInstance().getConnWithUseTime(0);
		try{
			conn.setAutoCommit(false);
			
			Timestamp sTime = new Timestamp(anaDate.getTimeInMillis());
			
			String sql ="select * from driver_orders_all_stat where stat_time >= to_date('"+GeneralConst.YYYY_MM_DD.format(sTime)+"','yyyy-MM-dd') and stat_time <= to_date('"+GeneralConst.YYYY_MM_DD.format(sTime)+" 23:59:59','yyyy-MM-dd HH24:mi:ss')";

			StatementHandle stmt = conn.createStatement();
			ResultSet sets = stmt.executeQuery(sql);
			
			if(sets.next()){
				return ;
			}
			
			sets.close();
			
			sql = "select * from taxi_order_list where car_no is not null and car_wanted_time>= to_date('"+GeneralConst.YYYY_MM_DD_HH_MM_SS.format(sTime)+"','yyyy-MM-dd HH24:mi:ss') and car_wanted_time <= to_date('"+GeneralConst.YYYY_MM_DD.format(sTime)+" 23:59:59','yyyy-MM-dd HH24:mi:ss')" ;
			if(this.is_plat){
				sql += " and order_id not in (select  order_id from radio_order_info where  create_time between to_date('"+GeneralConst.YYYY_MM_DD_HH_MM_SS.format(sTime)+"','yyyy-MM-dd hh24:mi:ss') and to_date('"+GeneralConst.YYYY_MM_DD.format(sTime)+" 23:59:59','yyyy-MM-dd hh24:mi:ss'))";
			}
			sql += " order by car_no";
			sets = stmt.executeQuery(sql);
			OrderInfo orderInfo = null;
			ArrayList orderList = new ArrayList();
			while(sets.next()){
				orderInfo = new OrderInfo();
				orderInfo.carNo = sets.getString("car_no");
				orderInfo.carWantedTime = sets.getTimestamp("car_wanted_time");
				orderInfo.status = sets.getInt("status");
				orderInfo.fangkong_type = sets.getInt("fangkongtype");
				orderInfo.serviceNo = sets.getString("driver_id");
				orderList.add(orderInfo);
			}
			sets.close();
			//System.out.println("--------------------------------------------orderList::"+orderList.size());
			querySingleBusinessDataBs(new Date(anaDate.getTimeInMillis()));
			
			//System.out.println(" singleBusinessDataBsMap::"+singleBusinessDataBsMap.size());
			HashMap<String, RecordInfo> recordMap = new HashMap<String, RecordInfo>();
			RecordInfo recordInfo = null;
			String key = "";
			for (int i = 0; i < orderList.size(); i++) {
				orderInfo = (OrderInfo)orderList.get(i);
				if(orderInfo.serviceNo==null||orderInfo.serviceNo.equals("")){
					long dateUp = 0;
					String serviceNo = "";
					if(singleBusinessDataBsMap!=null){
						for (Iterator iterator2 = singleBusinessDataBsMap.keySet().iterator(); iterator2.hasNext();) {
							String id = (String)iterator2.next();
							SingleBusinessDataBs singleBusinessDataBs = (SingleBusinessDataBs)singleBusinessDataBsMap.get(id);
							if(singleBusinessDataBs==null){
								continue;
							}
							if(!singleBusinessDataBs.dest_no.equals(orderInfo.carNo)){
								continue;
							}
							dateUp = singleBusinessDataBs.date_up;
							if(Math.abs(orderInfo.carWantedTime.getTime()-dateUp) <= this.relate){
								serviceNo = singleBusinessDataBs.service_no;
							}
						}
					}
					key = orderInfo.carNo+"_"+orderInfo.serviceNo;
					if(recordMap.containsKey(key)){
						recordInfo = recordMap.get(key);
					}else {
						recordInfo = new RecordInfo();
					}
					recordMap.put(key, recordInfo);
					recordInfo.carNo = orderInfo.carNo;
					recordInfo.serviceNo = serviceNo;
					recordInfo.orderNum++;
				}else{
					key = orderInfo.carNo+"_"+orderInfo.serviceNo;
					if(recordMap.containsKey(key)){
						recordInfo = recordMap.get(key);
					}else {
						recordInfo = new RecordInfo();
					}
					recordMap.put(key, recordInfo);
					recordInfo.orderNum++;
					if(orderInfo.status==STATUS_FANGKONG){
						if(orderInfo.fangkong_type == FANGKONG_SJ){
							recordInfo.fangkongsj++;
						}else {
							recordInfo.fangkongck++;
						}
					}
					recordInfo.serviceNo = orderInfo.serviceNo;
					recordInfo.carNo = orderInfo.carNo;
				}
			}
			
			if(recordMap.size()<=0){
				System.out.println("未分析出记录");
				return;
			}else {
				System.out.println("recordMap::"+recordMap.size());
			}
			sql = "insert into driver_orders_all_stat(id,car_no,service_no,order_num,fangkongck,fangkongsj,stat_time) values(?,?,?,?,?,?,?)";
			stmt = conn.prepareStatement(sql);
			long id = DbServer.getSingleInstance().getAvaliableId(conn, "driver_orders_all_stat", "id");
			int i = 0;
			for (Iterator iterator = recordMap.values().iterator(); iterator.hasNext();) {
				RecordInfo rInfo = (RecordInfo) iterator.next();
				stmt.setLong(1, id);
				stmt.setString(2, rInfo.carNo);
				stmt.setString(3, rInfo.serviceNo);
				stmt.setInt(4, rInfo.orderNum);
				stmt.setInt(5, rInfo.fangkongck);
				stmt.setInt(6, rInfo.fangkongsj);
				stmt.setTimestamp(7, sTime);
				stmt.addBatch();
				if(i % 200 ==0){
					stmt.executeBatch();
				}
				i++;
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
	public boolean querySingleBusinessDataBs(Date sTime) {
		this.singleBusinessDataBsMap = null;
		this.singleBusinessDataBsMap = new HashMap();
		Calendar cal = Calendar.getInstance();
		cal.setTime(sTime);
		cal.add(Calendar.DAY_OF_MONTH, 1);
		Date eTime = new Date(cal.getTimeInMillis() - 1000);

		DbHandle conn = DbServer.getSingleInstance().getConnWithUseTime(0);
		try {
			StatementHandle stmt = conn.prepareStatement("select * from single_business_data_bs where date_up>=? and date_up<=?");
			stmt.setTimestamp(1, new Timestamp(sTime.getTime()));
			stmt.setTimestamp(2, new Timestamp(eTime.getTime()));
			ResultSet sets = stmt.executeQuery();
			SingleBusinessDataBs singleBusinessDataBs = null;
			while (sets.next()) {
				singleBusinessDataBs = new SingleBusinessDataBs();
				singleBusinessDataBs.id = sets.getString("id");
				singleBusinessDataBs.dest_no = sets.getString("DISPATCH_CAR_NO");
				singleBusinessDataBs.service_no = sets.getString("SERVICE_NO");
				singleBusinessDataBs.date_down = GeneralConst.YYYY_MM_DD_HH_MM_SS.parse(sets.getString("DATE_DOWN")).getTime();
				singleBusinessDataBs.date_up = GeneralConst.YYYY_MM_DD_HH_MM_SS.parse(sets.getString("DATE_UP")).getTime();
				singleBusinessDataBsMap.put(singleBusinessDataBs.id, singleBusinessDataBs);
			}
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}
		return true;
	}

	private class RecordInfo{
		public int orderNum = 0;
		public String serviceNo = "";
		public String carNo = "";
		public int taxiCompany = 0;
		public Timestamp startTime = null;
		public Timestamp endTime = null;
		public int fangkongsj = 0;
		public int fangkongck = 0;
		public RecordInfo(){
			
		}
	}
	
	private class OrderInfo{
		public String carNo = "";
		public Timestamp carWantedTime = null;
		public int status = -1;
		public int fangkong_type = -1;
		public String serviceNo = "";
		public OrderInfo(){
			
		}
	}
	public class SingleBusinessDataBs {
		public String id;
		public String service_no;
		public String dest_no;
		public long date_up;
		public long date_down;
	}
	
	private class TimerTask extends FleetyTimerTask{
		public void run(){
			Calendar cal = Calendar.getInstance();
			cal.add(Calendar.DAY_OF_MONTH, -1);
			cal.set(Calendar.HOUR_OF_DAY, 0);
			cal.set(Calendar.MINUTE, 0);
			cal.set(Calendar.SECOND, 0);
			cal.set(Calendar.MILLISECOND, 0);
			int i=0;
			while(i<=60){
				Calendar cal1 = Calendar.getInstance();
				cal1.setTimeInMillis(cal.getTimeInMillis());
				cal1.add(Calendar.DAY_OF_MONTH, i);
			System.out.println("Fire ExecTask DriverOrdersAllDayAnasisServer:"+GeneralConst.YYYY_MM_DD_HH.format(cal1.getTime()));
			DriverOrdersAllDayAnasisServer.this.addExecTask(new ExecTask(cal1));
			i++;
			}
		}
	}
	
	private class ExecTask extends BasicTask{
		private Calendar anaDate = null;
		public ExecTask(Calendar anaDate){
			this.anaDate = anaDate;
		}
		
		public boolean execute() throws Exception{
			try {
				DriverOrdersAllDayAnasisServer.this.executeTask(this.anaDate);
			} catch (Exception e) {
				e.printStackTrace();
			}
			return true;
		}
		
		public String getDesc(){
			return "驾驶员接单情况日统计分析服务";
		}
		public Object getFlag(){
			return "DriverOrdersAllDayAnasisServer";
		}
	}
}
