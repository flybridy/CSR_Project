package com.fleety.server.examine;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;

import server.db.DbServer;
import server.threadgroup.ThreadPoolGroupServer;

import com.fleety.base.GeneralConst;
import com.fleety.db.SqlDbServer;
import com.fleety.server.BasicServer;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.fleety.util.pool.timer.FleetyTimerTask;
import com.fleety.util.pool.timer.TimerPool;

public class DriverServiceSmsExamineServer extends BasicServer {
	private TimerTask task = null;
	private TimerPool timer = null;
	private long time = 10 * 60 * 1000l;
	private int totalNum = 0;//订单总数
	private int current = 0;//当前下发订单数
	private double sendRate = 0.5;//下发比例
	private int preTime = 30;
	private int nextTime = 60;
	private String sms_content = "";
	private int gateway = 0;

	private String status = "";
	private Calendar sysDate = Calendar.getInstance();
	private HashMap<Integer, OrderInfo> orderMapDx = null;//保存随机待选订单
	private HashMap<Integer, OrderInfo> orderMapDf = null;//保存随机已选订单
	public boolean isRun = false;
	
	private static DriverServiceSmsExamineServer singleInstance = null;

	public static DriverServiceSmsExamineServer getSingleInstance() {
		if (singleInstance == null) {
			synchronized (DriverServiceSmsExamineServer.class) {
				if (singleInstance == null) {
					singleInstance = new DriverServiceSmsExamineServer();
				}
			}
		}
		return singleInstance;
	}
	
	private DriverServiceSmsExamineServer(){
		
	}
	
	@Override
	public boolean startServer() {
		
		String temp = this.getStringPara("send_rate");
		if(temp != null && !temp.equals("")){
			sendRate = Double.valueOf(temp);
		}
		temp = this.getStringPara("time");
		if (temp != null && !temp.equals("")) {
			time = this.getIntegerPara("time").intValue()*60*1000l;
		}
		temp = this.getStringPara("pre_time");
		if (temp != null && !temp.equals("")) {
			preTime = this.getIntegerPara("pre_time").intValue();
		}
		temp = this.getStringPara("next_time");
		if (temp != null && !temp.equals("")) {
			nextTime = this.getIntegerPara("next_time").intValue();
		}
		sms_content = this.getStringPara("sms_content");
		if(sms_content==null||sms_content.equals("")){
			return false;
		}
		temp = this.getStringPara("gateway");
		if (temp != null && !temp.equals("")) {
			gateway = this.getIntegerPara("gateway").intValue();
		}
		status = this.getStringPara("status");

		Integer timerThreadNum = 1;
		this.timer = ThreadPoolGroupServer.getSingleInstance().createTimerPool(
				"DriverServiceSmsExamineServer_Timer",
				timerThreadNum.intValue(), false);
		if (this.timer != null) {
			this.isRunning = true;
		}
		isRun = true;
		this.isRunning = this.scheduleTask(this.task = new TimerTask(), 1000l,time);

		return true;
	}

	public void stopServer() {
		if (this.task != null) {
			this.task.cancel();
		}
		super.stopServer();
	}
	
	private class TimerTask extends FleetyTimerTask {

		public void run() {
			try {
				Calendar cal = Calendar.getInstance();
				cal.set(Calendar.HOUR_OF_DAY, 0);
				cal.set(Calendar.MINUTE, 0);
				cal.set(Calendar.SECOND, 0);
				cal.set(Calendar.MILLISECOND, 0);
				if(sysDate.getTimeInMillis()>=cal.getTimeInMillis()){
					current = 0;
					sysDate.setTimeInMillis(cal.getTimeInMillis());
				}
				loadOrderInfo();

				sendSMS();
			} catch (Exception e) {
				e.printStackTrace();
			}
			
		}

	}
	/**
	 * 加载有车牌号接单 并统计订单总数
	 * @throws Exception
	 */
	private void loadOrderInfo() throws Exception {
		orderMapDx = new HashMap<Integer, OrderInfo>();
		String orderStr = getSelectOrder();
		DbHandle conn = DbServer.getSingleInstance().getConn();
		OrderInfo oInfo;
		try {
			String sql = "select * from taxi_order_list where 1=1 ";
			
			sql += " and order_id not in (select  order_id from radio_order_info where  create_time between to_date('"+GeneralConst.YYYY_MM_DD.format((new Date().getTime()))+" 00:00:00','yyyy-MM-dd hh24:mi:ss')-1 and to_date('"+GeneralConst.YYYY_MM_DD.format((new Date().getTime()))+" 23:59:59','yyyy-MM-dd hh24:mi:ss'))";

			Calendar cal = Calendar.getInstance();
			cal.add(Calendar.MINUTE, -preTime);
			sql += " and car_wanted_time < to_date('"+GeneralConst.YYYY_MM_DD_HH_MM_SS.format(cal.getTime()) + "','yyyy-MM-dd hh24:mi:ss')";
			cal.add(Calendar.MINUTE, -(nextTime-preTime));
			sql += " and car_wanted_time >= to_date('"+GeneralConst.YYYY_MM_DD_HH_MM_SS.format(cal.getTime()) + "','yyyy-MM-dd hh24:mi:ss')";
			
			if(status!=null&&!status.equals("")){
				sql += " and status in ("+status+")";
				
			}
			sql += " order by car_wanted_time";
			StatementHandle stmt = conn.prepareStatement(sql);
			ResultSet sets = stmt.executeQuery();
			int i = 1;
			while (sets.next()) {
				oInfo = new OrderInfo();
				oInfo.setOrderId(sets.getInt("order_id"));
				oInfo.setDestNo(sets.getString("car_no"));
				oInfo.setDriver_id(sets.getString("driver_id"));
				oInfo.setPhone(sets.getString("phone"));
				oInfo.setUser_id(sets.getInt("user_id"));
				oInfo.setCarWantedTime(GeneralConst.YYYY_MM_DD_HH_MM_SS.parse(sets.getString("car_wanted_time")));
				if(orderStr.indexOf(","+oInfo.getOrderId()+",")<0){
					orderMapDx.put(i, oInfo);
					i++;
				}
			}
			sets.close();
			stmt.close();
			
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}
	}
	/**
	 * 得到已发送订单id拼接字符串
	 * @return
	 */
	private String getSelectOrder() throws Exception {
		String temp = "";
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			String sql = "select * from driver_sms_examine where 1=1 ";
			Calendar cal = Calendar.getInstance();
			cal.add(Calendar.MINUTE, -nextTime);
			sql += " and send_time >= to_date('"+GeneralConst.YYYY_MM_DD_HH_MM_SS.format(cal.getTime()) + "','yyyy-MM-dd hh24:mi:ss')";
			StatementHandle stmt = conn.createStatement();
			ResultSet sets = stmt.executeQuery(sql);
			while(sets.next()){
				temp += "," + sets.getInt("order_id");
			}
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
		if(temp.equals("")){
			return "";
		}
		return temp+",";
	}
	
	private void sendSMS() throws Exception {
		if(this.orderMapDx==null||this.orderMapDx.size()<=0){
			return;
		}
		DbHandle conn = SqlDbServer.getSingleInstance().getConn();
		Date sendTime =  new Date();
		try{
			conn.setAutoCommit(false);
			StatementHandle stmt = conn.prepareStatement("insert into TBL_SMS_SNDTMP(USER_MBLPHONE,sms_content,sms_operator,UCUID) values(?,?,?,?)");
			int i = 1;
			for (Iterator iterator = this.orderMapDx.values().iterator(); iterator.hasNext();) {
				OrderInfo orderInfo = (OrderInfo) iterator.next();
				stmt.setString(1, orderInfo.getPhone());
				stmt.setString(2, this.sms_content.replace("{0}", orderInfo.getDestNo()));
				stmt.setInt(3, gateway);
				stmt.setString(4, "6880");
				stmt.addBatch();
				if(i%200==0){
					stmt.executeBatch();
				}
				i++;
				System.out.println(this.sms_content.replace("{0}", orderInfo.getDestNo())+","+orderInfo.getOrderId());
			}
			stmt.executeBatch();
			conn.commit();
		}finally{
			SqlDbServer.getSingleInstance().releaseConn(conn);
		}
		saveRecord(sendTime);
	}
	public void saveRecord(Date sendTime) throws Exception{
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			conn.setAutoCommit(false);
			StatementHandle stmt = conn.prepareStatement("insert into driver_sms_examine(id,phone,dest_no,driver_id,sms_content,send_time,is_reply,order_id) values(?,?,?,?,?,?,?,?)");
			int i = 1;
			for (Iterator iterator = this.orderMapDx.values().iterator(); iterator.hasNext();) {
				OrderInfo orderInfo = (OrderInfo) iterator.next();
				stmt.setInt(1, (int) DbServer.getSingleInstance().getAvaliableId(conn, "driver_sms_examine", "id"));
				stmt.setString(2, orderInfo.getPhone());
				stmt.setString(3, orderInfo.getDestNo());
				stmt.setString(4, orderInfo.getDriver_id());
				stmt.setString(5, this.sms_content.replace("{0}", orderInfo.getDestNo()));
				stmt.setTimestamp(6, new Timestamp(sendTime.getTime()));
				stmt.setInt(7, 0);
				stmt.setInt(8, orderInfo.getOrderId());
				stmt.addBatch();
				if(i%200==0){
					stmt.executeBatch();
				}
				i++;
			}
			stmt.executeBatch();
			conn.commit();
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
	}
	/**
	 * 把某个任务放入定时执行中，如果任务执行时间较长，应该产生新的任务放置到执行任务池中进行执行
	 * 
	 * @param timerTask
	 *            待周期性执行的任务
	 * @param delay
	 *            延迟执行时长，单位毫秒
	 * @param period
	 *            执行周期，单位毫秒
	 * @return
	 */
	public boolean scheduleTask(FleetyTimerTask timerTask, long delay,
			long period) {
		if (!this.isRunning()) {
			return false;
		}

		this.timer.schedule(timerTask, delay, period);
		return true;
	}
	
	public boolean isRun() {
		return isRun;
	}

	public void setRun(boolean isRun) {
		this.isRun = isRun;
	}
}
