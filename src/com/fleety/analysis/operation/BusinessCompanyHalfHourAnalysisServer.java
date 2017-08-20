package com.fleety.analysis.operation;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import server.db.DbServer;
import server.var.VarManageServer;

import com.fleety.analysis.AnalysisServer;
import com.fleety.base.GeneralConst;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.fleety.util.pool.thread.BasicTask;
import com.fleety.util.pool.timer.FleetyTimerTask;

public class BusinessCompanyHalfHourAnalysisServer  extends AnalysisServer {
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
			sql.append("select to_date(a.recode_time_t,'yyyy-MM-dd HH24:mi:ss') workDate,a.company_id,b.company_name,a.distance+a.free_distance total_distance,a.distance,a.free_distance,a.total_price,a.total_num from (select recode_time_t, min(taxi_company) company_id,sum(distance) distance,sum(free_distance) free_distance, sum(sum) total_price,count(1) total_num from (select taxi_company,sum,distance,free_distance, recode_time,to_char(recode_time, 'yyyy-MM-dd hh24:') || (case when to_number(to_char(recode_time, 'mi')) >= 30 then '30' else '00' end) recode_time_t from single_business_data_bs where free_distance >=0 and recode_time >= to_date('"+GeneralConst.YYYY_MM_DD_HH_MM_SS.format(eTime)+"', 'yyyy-MM-dd hh24:mi:ss') and recode_time < to_date('"+GeneralConst.YYYY_MM_DD_HH_MM_SS.format(sTime)+"', 'yyyy-MM-dd hh24:mi:ss')) group by taxi_company, recode_time_t order by recode_time_t) a left join (select TERM_ID, TERM_NAME as company_name from term) b on a.company_id = b.TERM_ID");
			
			System.out.println(sql.toString());
			StatementHandle stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql.toString());
			StatementHandle stmt1 = conn.prepareStatement("insert into ANA_company_business_DAY_STAT(id,company_id,company_name,work_times,total_distance,work_distance,free_distance,total_income,ANALYSIS_TIME,create_date) values(?,?,?,?,?,?,?,?,?,sysdate)");
			int recordNum=0;
			while(rs.next())
			{
				stmt1.setInt(1, (int)DbServer.getSingleInstance().getAvaliableId(conn, "ANA_company_business_DAY_STAT", "id"));
				stmt1.setInt(2, rs.getInt("company_id"));
				stmt1.setString(3, rs.getString("company_name"));
				stmt1.setInt(4, rs.getInt("total_num"));
				stmt1.setInt(5, rs.getInt("total_distance"));
				stmt1.setInt(6, rs.getInt("distance"));
				stmt1.setInt(7, rs.getInt("free_distance"));
				stmt1.setFloat(8, rs.getFloat("total_price"));
				stmt1.setTimestamp(9, rs.getTimestamp("workDate"));
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
			sb.append("select * from ANA_company_business_DAY_STAT ")
			  .append(" where to_char(ANALYSIS_TIME,'yyyy-MM-dd') = '").append(new SimpleDateFormat("yyyy-MM-dd").format(sTime)).append("'");
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
			
			System.out.println("Fire ExecTask BusinessCompanyHalfHourAnalysisServer:"+GeneralConst.YYYY_MM_DD_HH.format(cal.getTime()));
			BusinessCompanyHalfHourAnalysisServer.this.addExecTask(new ExecTask(cal));
		}
	}
	
	private class ExecTask extends BasicTask{
		private Calendar anaDate = null;
		public ExecTask(Calendar anaDate){
			this.anaDate = anaDate;
		}
		
		public boolean execute() throws Exception{
			BusinessCompanyHalfHourAnalysisServer.this.executeTask(this.anaDate);
			return true;
		}
		
		public String getDesc(){
			return "企业营运数据统计分析服务（半小时）";
		}
		public Object getFlag(){
			return "BusinessCompanyHalfHourAnalysisServer";
		}
	}
}

