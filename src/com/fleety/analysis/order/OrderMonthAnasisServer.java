package com.fleety.analysis.order;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Calendar;
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

public class OrderMonthAnasisServer extends AnalysisServer {
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
			
			StatementHandle stmt = conn.prepareStatement("select * from ana_order_month_stat where stat_time between ? and ?");
			stmt.setTimestamp(1, sTime);
			stmt.setTimestamp(2, eTime);
			ResultSet sets = stmt.executeQuery();
			if(sets.next()){
				return ;
			}
			conn.closeStatement(stmt);
			
			
			String sql ="select car_company,car_no,to_char(car_wanted_time, 'hh24') hour,count(*) num from taxi_order_list t where car_wanted_time >=? and  car_wanted_time <? group by car_company, car_no, to_char(car_wanted_time, 'hh24')";
			
			stmt = conn.prepareStatement(sql);
			stmt.setTimestamp(1, sTime);
			stmt.setTimestamp(2, eTime);
			
			Map infoMap = new HashMap();
			
			RecordInfo rInfo;
			Calendar tempCal = Calendar.getInstance();
			int comId=0,hour=0,num=0;
			String carNo = "";
			sets = stmt.executeQuery();
			
			while(sets.next()){
				comId = sets.getInt("car_company");
				carNo = sets.getString("car_no");
				hour = sets.getInt("hour");
				num = sets.getInt("num");
				
				if(infoMap.containsKey(carNo)){
					rInfo = (RecordInfo)infoMap.get(carNo);
				}else{
					rInfo = new RecordInfo();
				}
				
				rInfo.comId = comId;
				rInfo.carNo = carNo;
				switch(hour){
				case 0:
					rInfo.hour0 = num;
					break;
				case 1:
					rInfo.hour1 = num;
					break;
				case 2:
					rInfo.hour2 = num;
					break;
				case 3:
					rInfo.hour3 = num;
					break;
				case 4:
					rInfo.hour4 = num;
					break;
				case 5:
					rInfo.hour5 = num;
					break;
				case 6:
					rInfo.hour6 = num;
					break;
				case 7:
					rInfo.hour7 = num;
					break;
				case 8:
					rInfo.hour8 = num;
					break;
				case 9:
					rInfo.hour9 = num;
					break;
				case 10:
					rInfo.hour10 = num;
					break;
				case 11:
					rInfo.hour11 = num;
					break;
				case 12:
					rInfo.hour12 = num;
					break;
				case 13:
					rInfo.hour13 = num;
					break;
				case 14:
					rInfo.hour14 = num;
					break;
				case 15:
					rInfo.hour15 = num;
					break;
				case 16:
					rInfo.hour16 = num;
					break;
				case 17:
					rInfo.hour17 = num;
					break;
				case 18:
					rInfo.hour18 = num;
					break;
				case 19:
					rInfo.hour19 = num;
					break;
				case 20:
					rInfo.hour20 = num;
					break;
				case 21:
					rInfo.hour21 = num;
					break;
				case 22:
					rInfo.hour22 = num;
					break;
				case 23:
					rInfo.hour23 = num;
					break;
				}
				infoMap.put(carNo, rInfo);
			}
			conn.closeStatement(stmt);
			
			
			sql = "insert into ana_order_month_stat(id,stat_time,org_id,dest_no,hour_0,hour_1,hour_2,hour_3,hour_4,hour_5,hour_6,hour_7,hour_8,hour_9,hour_10,hour_11,hour_12,hour_13,hour_14,hour_15,hour_16,hour_17,hour_18,hour_19,hour_20,hour_21,hour_22,hour_23,stat_type,total,car_type) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,?,?)";
			stmt = conn.prepareStatement(sql);
			Iterator it = infoMap.keySet().iterator();
			int count = 0;
			while(it.hasNext()){
				rInfo = (RecordInfo)infoMap.get(it.next());
				stmt.setInt(1, (int)DbServer.getSingleInstance().getAvaliableId(conn, "ana_order_month_stat", "id"));
				stmt.setTimestamp(2, new Timestamp(statTime.getTimeInMillis()));
				stmt.setInt(3, rInfo.comId);
				stmt.setString(4, rInfo.carNo);
				stmt.setInt(5, rInfo.hour0);
				stmt.setInt(6, rInfo.hour1);
				stmt.setInt(7, rInfo.hour2);
				stmt.setInt(8, rInfo.hour3);
				stmt.setInt(9, rInfo.hour4);
				stmt.setInt(10, rInfo.hour5);
				stmt.setInt(11, rInfo.hour6);
				stmt.setInt(12, rInfo.hour7);
				stmt.setInt(13, rInfo.hour8);
				stmt.setInt(14, rInfo.hour9);
				stmt.setInt(15, rInfo.hour10);
				stmt.setInt(16, rInfo.hour11);
				stmt.setInt(17, rInfo.hour12);
				stmt.setInt(18, rInfo.hour13);
				stmt.setInt(19, rInfo.hour14);
				stmt.setInt(20, rInfo.hour15);
				stmt.setInt(21, rInfo.hour16);
				stmt.setInt(22, rInfo.hour17);
				stmt.setInt(23, rInfo.hour18);
				stmt.setInt(24, rInfo.hour19);
				stmt.setInt(25, rInfo.hour20);
				stmt.setInt(26, rInfo.hour21);
				stmt.setInt(27, rInfo.hour22);
				stmt.setInt(28, rInfo.hour23);
				stmt.setInt(29, rInfo.getTotal());
				stmt.setInt(30, rInfo.getCarType());
				stmt.addBatch();
				count ++;
				if(count % 200 == 0){
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
		public int hour0 = 0;
		public int hour1 = 0;
		public int hour2 = 0;
		public int hour3 = 0;
		public int hour4 = 0;
		public int hour5 = 0;
		public int hour6 = 0;
		public int hour7 = 0;
		public int hour8 = 0;
		public int hour9 = 0;
		public int hour10 = 0;
		public int hour11 = 0;
		public int hour12 = 0;
		public int hour13 = 0;
		public int hour14 = 0;
		public int hour15 = 0;
		public int hour16 = 0;
		public int hour17 = 0;
		public int hour18 = 0;
		public int hour19 = 0;
		public int hour20 = 0;
		public int hour21 = 0;
		public int hour22 = 0;
		public int hour23 = 0;
		public int comId = 0;
		public String carNo = "";
		
		public RecordInfo(){
			
		}

		public int getTotal() {
			int total = this.hour0 + this.hour1 + this.hour2 + this.hour3
					+ this.hour4 + this.hour5 + this.hour6 + this.hour7
					+ this.hour8 + this.hour9 + this.hour10 + this.hour11
					+ this.hour12 + this.hour13 + this.hour14 + this.hour15
					+ this.hour16 + this.hour17 + this.hour18 + this.hour19
					+ this.hour20 + this.hour21 + this.hour22 + this.hour23;
			return total;
		}
		public int getCarType(){
			int carType=0;
			DestInfo destInfo=GlobalUtilServer.getDestInfo(carNo);
			if(destInfo!=null){
				carType=destInfo.carType;
			}
			return carType;
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
			
			System.out.println("Fire ExecTask OrderMonthAnasisServer:"+GeneralConst.YYYY_MM_DD_HH.format(cal.getTime()));
			OrderMonthAnasisServer.this.addExecTask(new ExecTask(cal));
		}
	}
	
	private class ExecTask extends BasicTask{
		private Calendar anaDate = null;
		public ExecTask(Calendar anaDate){
			this.anaDate = anaDate;
		}
		
		public boolean execute() throws Exception{
			OrderMonthAnasisServer.this.executeTask(this.anaDate);
			return true;
		}
		
		public String getDesc(){
			return "月调度业务时间分布分析服务";
		}
		public Object getFlag(){
			return "OrderMonthAnasisServer";
		}
	}
}
