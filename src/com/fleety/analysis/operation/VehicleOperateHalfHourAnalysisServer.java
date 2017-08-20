package com.fleety.analysis.operation;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import server.db.DbServer;
import server.var.VarManageServer;

import com.fleety.analysis.AnalysisServer;
import com.fleety.analysis.track.DestInfo;
import com.fleety.base.GeneralConst;
import com.fleety.base.InfoContainer;
import com.fleety.server.GlobalUtilServer;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.fleety.util.pool.thread.BasicTask;
import com.fleety.util.pool.timer.FleetyTimerTask;

public class VehicleOperateHalfHourAnalysisServer extends AnalysisServer {
	private TimerTask task = null;
	private SimpleDateFormat sdf             = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private SimpleDateFormat sdf2            = new SimpleDateFormat("yyyy-MM-dd HH:mm");
	private float fuelSurcharges = 0;

	public boolean startServer() {
		super.startServer();
		if(!this.isRunning()){
			return this.isRunning();
		}		
		fuelSurcharges =  Integer.parseInt(VarManageServer.getSingleInstance().getVarStringValue("fuel_surcharges"));
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
		if(!this.isVehicleOperateHalfHour(anaDate)){
			this.saveVehicleOperateHalfHour(anaDate);
		}
	}
	private void saveVehicleOperateHalfHour(Calendar anaDate) throws SQLException{
		long t = System.currentTimeMillis();
		Calendar statTime = Calendar.getInstance();
		statTime.setTimeInMillis(anaDate.getTimeInMillis());
		
		anaDate.add(Calendar.DAY_OF_MONTH, 1);
		Timestamp eTime = new Timestamp(anaDate.getTimeInMillis());
		anaDate.add(Calendar.DAY_OF_MONTH, 1);
		Timestamp sTime = new Timestamp(anaDate.getTimeInMillis());
		DbHandle conn = DbServer.getSingleInstance().getConnWithUseTime(0);
		try{
			conn.setAutoCommit(false);
			StringBuilder sql = new StringBuilder();
			sql.append(" select a.plate_no,a.recode_time_t workDate,a.company_id,d.company_name,a.waiting_hour,a.waiting_minute,a.waiting_second,a.totalIncome,a.work_times from ( ");
			sql.append(" select dispatch_car_no plate_no,recode_time_t,min(taxi_company) company_id,sum(waiting_hour) waiting_hour,sum(waiting_minute) waiting_minute,sum(waiting_second) waiting_second,(sum(sum)+"+fuelSurcharges+"*count(1)) totalIncome,count(1) work_times from (");
					sql.append(" select id,dispatch_car_no,taxi_company,waiting_hour,waiting_minute,waiting_second,sum,recode_time,to_char(recode_time,'yyyy-MM-dd hh24:')||(case when to_number(to_char(recode_time,'mi'))>=30 then '30' else '00' end) recode_time_t");
					sql.append(" from single_business_data_bs"); 
					sql.append(" where recode_time>=to_date('"+GeneralConst.YYYY_MM_DD_HH_MM_SS.format(eTime)+"','yyyy-MM-dd hh24:mi:ss')");
					sql.append(" and recode_time<to_date('"+GeneralConst.YYYY_MM_DD_HH_MM_SS.format(sTime)+"','yyyy-MM-dd hh24:mi:ss'))");
					sql.append(" group by dispatch_car_no,recode_time_t");
					sql.append(" order by recode_time_t) a");
			sql.append(" left join  (select TERM_ID,TERM_NAME as company_name from term ) d on a.company_id = d.TERM_ID ");
			System.out.println(sql.toString());
			StatementHandle stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql.toString());
			StatementHandle stmt1 = conn.prepareStatement("insert into ana_single_car_halfhour_stat(id,plate_no,company_id,company_name,waiting_hour,waiting_minute,waiting_second,total_income,work_date,analysis_time,work_times) values(?,?,?,?,?,?,?,?,?,?,?)");
			int recordNum=0;
			while(rs.next())
			{
				stmt1.setInt(1, (int)DbServer.getSingleInstance().getAvaliableId(conn, "ana_single_car_halfhour_stat", "id"));
				stmt1.setString(2, rs.getString("plate_no"));
				stmt1.setInt(3, rs.getInt("company_id"));
				stmt1.setString(4, rs.getString("company_name"));
				stmt1.setInt(5, rs.getInt("waiting_hour"));
				stmt1.setInt(6, rs.getInt("waiting_minute"));
				stmt1.setInt(7, rs.getInt("waiting_second"));
				stmt1.setFloat(8, rs.getFloat("totalIncome"));
				stmt1.setString(9, rs.getString("workDate"));
				stmt1.setDate(10, new java.sql.Date(new Date().getTime()));
				stmt1.setInt(11,rs.getInt("work_times"));
				stmt1.addBatch();
				if(recordNum%200==0){
					stmt1.executeBatch();
				}
				recordNum ++;
			}
			stmt1.executeBatch();
			conn.commit();
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
			System.out.println("Exec Duration:"+(System.currentTimeMillis() - t));
		}
	}
	private boolean isVehicleOperateHalfHour(Calendar anaDate){
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try{
			Calendar statTime = Calendar.getInstance();
			statTime.setTimeInMillis(anaDate.getTimeInMillis());
			
			Timestamp sTime = new Timestamp(anaDate.getTimeInMillis());
			anaDate.add(Calendar.DAY_OF_MONTH, -1);
			System.out.println(GeneralConst.YYYY_MM_DD_HH_MM_SS.format(sTime));
			StatementHandle stmt = conn.createStatement();
			StringBuilder sb = new StringBuilder();
			sb.append("select * from ana_single_car_halfhour_stat ")
			  .append(" where work_date = '").append(sdf2.format(sTime)).append("'");
			ResultSet sets = stmt.executeQuery(sb.toString());
			if(sets.next()){
				return true;
			}
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
		return false;
	}
	private class TimerTask extends FleetyTimerTask{
		public void run(){
			Calendar cal = Calendar.getInstance();
			cal.add(Calendar.DAY_OF_MONTH, -1);
			cal.set(Calendar.HOUR_OF_DAY, 0);
			cal.set(Calendar.MINUTE, 0);
			cal.set(Calendar.SECOND, 0);
			cal.set(Calendar.MILLISECOND, 0);
			
			System.out.println("Fire ExecTask VehicleOperateHalfHourAnalysisServer:"+GeneralConst.YYYY_MM_DD_HH.format(cal.getTime()));
			VehicleOperateHalfHourAnalysisServer.this.addExecTask(new ExecTask(cal));
		}
	}
	
	private class ExecTask extends BasicTask{
		private Calendar anaDate = null;
		public ExecTask(Calendar anaDate){
			this.anaDate = anaDate;
		}
		
		public boolean execute() throws Exception{
			VehicleOperateHalfHourAnalysisServer.this.executeTask(this.anaDate);
			return true;
		}
		
		public String getDesc(){
			return "单车营运数据统计分析服务";
		}
		public Object getFlag(){
			return "VehicleOperateHalfHourAnalysisServer";
		}
	}
}
