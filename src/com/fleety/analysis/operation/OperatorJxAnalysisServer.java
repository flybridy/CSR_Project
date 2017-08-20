package com.fleety.analysis.operation;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;

import server.db.DbServer;

import com.fleety.analysis.AnalysisServer;
import com.fleety.base.GeneralConst;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.fleety.util.pool.thread.BasicTask;
import com.fleety.util.pool.timer.FleetyTimerTask;

public class OperatorJxAnalysisServer extends AnalysisServer {
	private TimerTask task = null;
	private int createSource = 10;
	private int cancelSource = 4;
	private int fangkongSource = 4;
	private int modifySource = 4;
	

	public boolean startServer() {
		super.startServer();
		if(!this.isRunning()){
			return this.isRunning();
		}		
		
		int hour = this.getIntegerPara("hour").intValue();
		int minute = this.getIntegerPara("minute").intValue();
		createSource = this.getIntegerPara("CREATE_SOURCE").intValue();
		cancelSource = this.getIntegerPara("CANCEL_SOURCE").intValue();
		fangkongSource = this.getIntegerPara("FANGKONG_SOURCE").intValue();
		modifySource = this.getIntegerPara("MODIFY_SOURCE").intValue();

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
			statTime.set(Calendar.HOUR_OF_DAY, 0);
			statTime.set(Calendar.MINUTE, 0);
			statTime.set(Calendar.SECOND, 0);
			
			Timestamp sTime = new Timestamp(anaDate.getTimeInMillis());
			anaDate.add(Calendar.DAY_OF_MONTH, 1);
			Timestamp eTime = new Timestamp(anaDate.getTimeInMillis());
			
			StatementHandle stmt = conn.prepareStatement("select * from ANA_OPERATOR_JX_STAT where stat_time between ? and ?");
			stmt.setTimestamp(1, sTime);
			stmt.setTimestamp(2, eTime);
			ResultSet sets = stmt.executeQuery();
			if(sets.next()){
				return ;
			}
			conn.closeStatement(stmt);
			
			StringBuffer sql = new StringBuffer("select f.user_id,f.user_name,nvl(a.total, 0) as total,nvl(a.cancelTotal, 0) as cancelTotal,nvl(a.fangkongTotal, 0) as fangkongTotal,nvl(d.total, 0) as modifyTotal ");
			sql.append(" from (select * from operator where role = 1) f");
			sql.append(" left join (select user_id, sum(case when 1 = 1 then 1 end) as total,sum(case when status = 5 then 1 end) as cancelTotal,sum(case when status = 7 then 1 end) as fangkongTotal from taxi_order_list where created_time>=? and created_time<? group by user_id) a on a.user_id =f.user_id");
			sql.append(" left join (select action_userid,count(distinct order_history_id) as total from taxi_order_history where action = 7 and modified_time>=? and modified_time <? group by action_userid) d on f.user_id = d.action_userid");
			System.out.println("jxSql = "+sql.toString());
			stmt = conn.prepareStatement(sql.toString());
			stmt.setTimestamp(1, sTime);
			stmt.setTimestamp(2, eTime);
			stmt.setTimestamp(3, sTime);
			stmt.setTimestamp(4, eTime);
			sets = stmt.executeQuery();

			ArrayList infoList = new ArrayList();
			
			RecordInfo rInfo = null;
			while(sets.next()){
				rInfo = new RecordInfo();
				rInfo.userId = sets.getInt("user_id");
				rInfo.userName = sets.getString("user_name");
				rInfo.total = sets.getInt("total") * createSource;
				rInfo.cancalTotal = sets.getInt("cancelTotal") * cancelSource;
				rInfo.fangkongTotal = sets.getInt("fangkongTotal") * fangkongSource;
				rInfo.modifyTotal = sets.getInt("modifyTotal") * modifySource;
				infoList.add(rInfo);
			}
			sets.close();
			conn.closeStatement(stmt);
			
			
			String insertSql = "insert into ANA_OPERATOR_JX_STAT(id,stat_time,USER_ID,USER_NAME,TOTAL,CANCELTOTAL,FANGKONGTOTAL,MODIFYTOTAL,RECODE_TIME) values(?,?,?,?,?,?,?,?,sysdate)";
			stmt = conn.prepareStatement(insertSql);
			long id = DbServer.getSingleInstance().getAvaliableId(conn, "ANA_OPERATOR_JX_STAT", "id", true);
			for(int i = 0; i < infoList.size(); i ++)
			{
				rInfo = (RecordInfo) infoList.get(i);
				stmt.setLong(1, id++);
				stmt.setTimestamp(2, new Timestamp(statTime.getTimeInMillis()));
				stmt.setInt(3, rInfo.userId);
				stmt.setString(4, rInfo.userName);
				stmt.setInt(5, rInfo.total);
				stmt.setInt(6, rInfo.cancalTotal);
				stmt.setInt(7, rInfo.fangkongTotal);
				stmt.setInt(8, rInfo.modifyTotal);stmt.addBatch();
				if(i % 200 ==0){
					stmt.executeBatch();
				}
			}
			stmt.executeBatch();
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
		public int userId=0;
		public int total=0;
		public int cancalTotal=0;
		public int fangkongTotal=0;
		public int modifyTotal=0;
		public String userName = "";
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
			
			System.out.println("Fire ExecTask OperatorJxAnalysisServer:"+GeneralConst.YYYY_MM_DD_HH.format(cal.getTime()));
			OperatorJxAnalysisServer.this.addExecTask(new ExecTask(cal));
		}
	}
	
	private class ExecTask extends BasicTask{
		private Calendar anaDate = null;
		public ExecTask(Calendar anaDate){
			this.anaDate = anaDate;
		}
		
		public boolean execute() throws Exception{
			OperatorJxAnalysisServer.this.executeTask(this.anaDate);
			return true;
		}
		
		public String getDesc(){
			return "话务员绩效分析服务";
		}
		public Object getFlag(){
			return "BusinessDayAnasisServer";
		}
	}
}
