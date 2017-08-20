package com.fleety.analysis.sms;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;

import server.db.DbServer;
import server.track.TrackServer;
import server.var.VarManageServer;

import com.fleety.analysis.AnalysisServer;
import com.fleety.base.GeneralConst;
import com.fleety.base.InfoContainer;
import com.fleety.base.StrFilter;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.fleety.util.pool.thread.BasicTask;
import com.fleety.util.pool.timer.FleetyTimerTask;

public class SmsDayAnasisServer extends AnalysisServer {
	private TimerTask task = null;
	
	private  String DISPATCH_FALG = "徐州";
	private  String SJZC_FALG = "sjzc";
	private  String WYZC_FALG = "wyzc";
	

	public boolean startServer() {
		super.startServer();
		if(!this.isRunning()){
			return this.isRunning();
		}		
		
		String temp = VarManageServer.getSingleInstance().getStringPara("DISPATCH_FALG");
		if(StrFilter.hasValue(temp)){
			this.DISPATCH_FALG=temp;
		}
		temp = VarManageServer.getSingleInstance().getStringPara("SJZC_FALG");
		if(StrFilter.hasValue(temp)){
			this.SJZC_FALG=temp;
		}
		temp = VarManageServer.getSingleInstance().getStringPara("WYZC_FALG");
		if(StrFilter.hasValue(temp)){
			this.WYZC_FALG=temp;
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
			
			String sql ="select * from ana_sms_day_stat where STAT_TIME >= to_date('"+GeneralConst.YYYY_MM_DD.format(sTime)+"','yyyy-MM-dd') and STAT_TIME <= to_date('"+GeneralConst.YYYY_MM_DD.format(sTime)+" 23:59:59','yyyy-MM-dd HH24:mi:ss')";

			StatementHandle stmt = conn.createStatement();
			ResultSet sets = stmt.executeQuery(sql);
			
			if(sets.next()){
				return ;
			}
			
			sets.close();
			
			int total_num=0,suc_num=0,fail_num=0,dispatch_num=0,dispatch_suc=0,dispatch_fail=0,sjzc_num=0,sjzc_suc=0,sjzc_fail=0,
			wyzc_num=0,wyzc_suc=0,wyzc_fail=0 ;
			sql = "select id,status,source_flag from SMS_RECEIVE_RECORD where receive_time>= to_date('"+GeneralConst.YYYY_MM_DD_HH_MM_SS.format(sTime)+"','yyyy-MM-dd HH24:mi:ss') and receive_time <= to_date('"+GeneralConst.YYYY_MM_DD.format(sTime)+" 23:59:59','yyyy-MM-dd HH24:mi:ss') order by id";
			sets = stmt.executeQuery(sql);
			String source_flag= "";
			int status = 0;
			
			while(sets.next()){
				total_num++;
				source_flag=sets.getString("source_flag");
				status=sets.getInt("status");
				
				if(source_flag.equalsIgnoreCase(this.DISPATCH_FALG)){
					dispatch_num++;
					if(status==0){
						dispatch_suc++;
						suc_num++;
					}else if(status==1){
						dispatch_fail++;
						fail_num++;
					}
				}else if(source_flag.equals(this.SJZC_FALG)){
					sjzc_num++;
					if(status==0){
						sjzc_suc++;
						suc_num++;
					}else if(status==1){
						sjzc_fail++;
						fail_num++;
					}
				}else if(source_flag.equalsIgnoreCase(this.WYZC_FALG)){
					wyzc_num++;
					if(status==0){
						dispatch_suc++;
						suc_num++;
					}else if(status==1){
						dispatch_fail++;
						fail_num++;
					}
				}else{
					if(status==0){
						dispatch_suc++;
						suc_num++;
					}else if(status==1){
						dispatch_fail++;
						fail_num++;
					}
				}
			}
			sets.close();
			long id = DbServer.getSingleInstance().getAvaliableId(conn, "ANA_SMS_DAY_STAT", "id", true);
			sql = "insert into ANA_SMS_DAY_STAT(id,total_num,suc_num,fail_num,dispatch_num,dispatch_suc,dispatch_fail,sjzc_num,sjzc_suc,sjzc_fail,wyzc_num,wyzc_suc,wyzc_fail,stat_time,create_time) "
									+ " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,sysdate)";
			stmt = conn.prepareStatement(sql);
			stmt.setLong(1, id);
			stmt.setInt(2, total_num);
			stmt.setInt(3, suc_num);
			stmt.setInt(4, fail_num);
			stmt.setInt(5, dispatch_num);
			stmt.setInt(6, dispatch_suc);
			stmt.setInt(7, dispatch_fail);
			stmt.setInt(8, sjzc_num);
			stmt.setInt(9, sjzc_suc);
			stmt.setInt(10, sjzc_fail);
			stmt.setInt(11, wyzc_num);
			stmt.setInt(12, wyzc_suc);
			stmt.setInt(13, wyzc_fail);
			stmt.setTimestamp(14, new Timestamp(anaDate.getTimeInMillis()));
			stmt.execute();
			conn.commit();
			stmt.close();
			
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
	

	
	private class TimerTask extends FleetyTimerTask{
		public void run(){
			Calendar cal = Calendar.getInstance();
			cal.add(Calendar.DAY_OF_MONTH, -1);
			cal.set(Calendar.HOUR_OF_DAY, 0);
			cal.set(Calendar.MINUTE, 0);
			cal.set(Calendar.SECOND, 0);
			cal.set(Calendar.MILLISECOND, 0);
			
			System.out.println("Fire ExecTask SmsDayAnasisServer:"+GeneralConst.YYYY_MM_DD_HH.format(cal.getTime()));
			SmsDayAnasisServer.this.addExecTask(new ExecTask(cal));
		}
	}
	
	private class ExecTask extends BasicTask{
		private Calendar anaDate = null;
		public ExecTask(Calendar anaDate){
			this.anaDate = anaDate;
		}
		
		public boolean execute() throws Exception{
			SmsDayAnasisServer.this.executeTask(this.anaDate);
			return true;
		}
		
		public String getDesc(){
			return "短信发送数日统计分析服务";
		}
		public Object getFlag(){
			return "SmsDayAnasisServer";
		}
	}
}
