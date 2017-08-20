package com.fleety.analysis.order;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Calendar;

import server.db.DbServer;

import com.fleety.analysis.AnalysisServer;
import com.fleety.base.GeneralConst;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.fleety.util.pool.thread.BasicTask;
import com.fleety.util.pool.timer.FleetyTimerTask;

public class BusinessDayAnasisServer extends AnalysisServer {
	private TimerTask task = null;
	private int oilExtraFee = 0;

	public boolean startServer() {
		super.startServer();
		if(!this.isRunning()){
			return this.isRunning();
		}		
		
		int hour = this.getIntegerPara("hour").intValue();
		int minute = this.getIntegerPara("minute").intValue();
		oilExtraFee = this.getIntegerPara("OIL_EXTRA_FEE").intValue();

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
			Calendar statTime = Calendar.getInstance();
			statTime.setTimeInMillis(anaDate.getTimeInMillis());
			
			Timestamp sTime = new Timestamp(anaDate.getTimeInMillis());
			anaDate.add(Calendar.DAY_OF_MONTH, 1);
			Timestamp eTime = new Timestamp(anaDate.getTimeInMillis());
			
			StatementHandle stmt = conn.prepareStatement("select * from ANA_BUSINESS_DAY_STAT where stat_time between ? and ?");
			stmt.setTimestamp(1, sTime);
			stmt.setTimestamp(2, eTime);
			ResultSet sets = stmt.executeQuery();
			if(sets.next()){
				return ;
			}
			conn.closeStatement(stmt);
			
			
//			String sql ="select count(distinct car_no) car_num,count(*) total_num,sum(sum+"+oilExtraFee+") business_price,sum(distance) run_km,sum(free_distance) free_km,sum((date_down-date_up)*1440) BUSINESS_TIMES,avg(waiting_hour*3600+waiting_minute*60+waiting_second) avg_wait_time,sum(waiting_hour*3600+waiting_minute*60+waiting_second)/3600 wait_time_total from single_business_data_bs where recode_time >=? and recode_time<? and date_down>=date_up";
//			合肥新增合乘统计
			String sql ="select t.* ,(select count(*) as operation_num from car where mdt_id>0) operation_num from (select count(distinct car_no) car_num,count(*) total_num,sum(sum+"+oilExtraFee+") business_price,sum(distance) run_km,sum(free_distance) free_km,sum((date_down-date_up)*1440) BUSINESS_TIMES,avg(waiting_hour*3600+waiting_minute*60+waiting_second) avg_wait_time,sum(waiting_hour*3600+waiting_minute*60+waiting_second)/3600 wait_time_total "
					+ ",COUNT (CASE WHEN allowshare = 0 THEN 1  ELSE null END) normal_order_total,COUNT (CASE WHEN allowshare = 1 THEN 1 ELSE null END) first_order_total,COUNT (CASE WHEN allowshare = 2 THEN 1 ELSE null END) second_order_total"
//					+ " from single_business_data_bs where recode_time >=? and recode_time<? and date_down>=date_up";
					//统一修改为按照上车时间为准，modified by Walter 20150211
//					+ " sum((select grade_type from grade where create_time=date_up)) grade_type " 
//					+ " count(distinct car_no) work_num, "
//					+ " count(distinct date_up) load_num "
					+ " from single_business_data_bs where date_up >=? and date_up<? and date_down>=date_up) t";
			
			
			stmt = conn.prepareStatement(sql);
			stmt.setTimestamp(1, sTime);
			stmt.setTimestamp(2, eTime);
			long st1 = System.currentTimeMillis();
			
			sets = stmt.executeQuery();
			long st2 = System.currentTimeMillis();
			
			System.out.println("BusinessDayAnasisServer查询耗时:"+(st2-st1));
			
			int carNum = 0,totalNum=0,dispatchOrderNum=0;
			double businessPrice = 0.0,dispatchPrice=0.0,runKm=0.0,freeKm=0.0,totalKm=0.0,avgTimes = 0.0,avgPrice= 0.0,priceParcent=0.0,freeParcent=0.0,businessParcent=0.0,businessTimes=0.0,avgWaitTime=0.0,totalWaitTime=0.0;
			int normal_order_total =0,first_order_total=0,second_order_total =0,operation_num=0;
			if(sets.next()){
				carNum = sets.getInt("car_num");//总车数
				totalNum = sets.getInt("total_num");//总车次
				businessPrice = sets.getDouble("business_price");//总营收
				runKm = sets.getDouble("run_km");//总营运里程
				freeKm = sets.getDouble("free_km");//总空驶里程
				businessTimes = sets.getDouble("BUSINESS_TIMES");//营运时间
				totalKm = runKm+freeKm;//总行驶里程
				avgTimes = (double)totalNum/(double)carNum;//平均单车差次
				avgPrice = businessPrice/totalNum;//平均单笔营收
				priceParcent = businessPrice*100/totalKm;//百公里营收
				freeParcent = freeKm*100/totalKm;//空驶率
				businessParcent = 100 - freeParcent;//公里利用率
				
				operation_num = sets.getInt("operation_num");//运营车数
				
				avgWaitTime = sets.getDouble("avg_wait_time");
				totalWaitTime = sets.getDouble("wait_time_total");
				normal_order_total=sets.getInt("normal_order_total");
				first_order_total=sets.getInt("first_order_total");
				second_order_total=sets.getInt("second_order_total");
				
			}
			sets.close();
			conn.closeStatement(stmt);
			
			sql ="select count(*) DISPATCH_ORDER_NUM,sum(sum+"+oilExtraFee+") DISPATCH_PRICE from single_business_data_bs where recode_time >=? and recode_time<? and is_tele_order=1";
			
			stmt = conn.prepareStatement(sql);
			stmt.setTimestamp(1, sTime);
			stmt.setTimestamp(2, eTime);
			sets = stmt.executeQuery();
			if(sets.next()){
				dispatchOrderNum = sets.getInt("DISPATCH_ORDER_NUM");//电调差次
				dispatchPrice = sets.getDouble("DISPATCH_PRICE");//电调营收
			}
			conn.closeStatement(stmt);
			
			sql = "insert into ANA_BUSINESS_DAY_STAT(id,stat_time,CAR_NUM,DISPATCH_ORDER_NUM,TOTAL_NUM,DISPATCH_PRICE,BUSINESS_PRICE,RUN_KM,BUSINESS_KM,KM_PARCENT,FREE_KM,FREE_PARCENT,BUSINESS_TIMES,PRICE_PARCENT,AVG_NUM,AVG_PRICE,RECODE_TIME,avg_wait_time,total_wait_time,normal_order_total,first_order_total,second_order_total,operation_num) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,sysdate,?,?,?,?,?,?)";
			stmt = conn.prepareStatement(sql);

			stmt.setInt(1, (int)DbServer.getSingleInstance().getAvaliableId(conn, "ANA_BUSINESS_DAY_STAT", "id"));
			statTime.set(Calendar.HOUR_OF_DAY, 0);
			statTime.set(Calendar.MINUTE, 0);
			statTime.set(Calendar.SECOND, 0);
			stmt.setTimestamp(2, new Timestamp(statTime.getTimeInMillis()));
			stmt.setInt(3, carNum);
			stmt.setInt(4, dispatchOrderNum);
			stmt.setInt(5, totalNum);
			stmt.setDouble(6, dispatchPrice);
			stmt.setDouble(7, businessPrice);
			stmt.setDouble(8, totalKm);
			stmt.setDouble(9, runKm);
			stmt.setDouble(10, businessParcent);
			stmt.setDouble(11, freeKm);
			stmt.setDouble(12, freeParcent);
			stmt.setDouble(13, businessTimes);
			stmt.setDouble(14, priceParcent);
			stmt.setDouble(15, avgTimes);
			stmt.setDouble(16, avgPrice);
			
			stmt.setDouble(17, avgWaitTime);
			stmt.setDouble(18, totalWaitTime);
			
			stmt.setInt(19, normal_order_total);
			stmt.setInt(20, first_order_total);
			stmt.setInt(21, second_order_total);
			
			stmt.setInt(22, operation_num);
			
			stmt.execute();
	
			conn.closeStatement(stmt);
		}catch(Exception e){
			e.printStackTrace();
			throw e;
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
			System.out.println("Exec Duration:"+(System.currentTimeMillis() - t));
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
			
			System.out.println("Fire ExecTask BusinessDayAnasisServer:"+GeneralConst.YYYY_MM_DD_HH.format(cal.getTime()));
			BusinessDayAnasisServer.this.addExecTask(new ExecTask(cal));
		}
	}
	
	private class ExecTask extends BasicTask{
		private Calendar anaDate = null;
		public ExecTask(Calendar anaDate){
			this.anaDate = anaDate;
		}
		
		public boolean execute() throws Exception{
			BusinessDayAnasisServer.this.executeTask(this.anaDate);
			return true;
		}
		
		public String getDesc(){
			return "计价器数据分析日报表服务";
		}
		public Object getFlag(){
			return "BusinessDayAnasisServer";
		}
	}
}
