package com.fleety.analysis.grade;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;

import server.db.DbServer;

import com.fleety.analysis.AnalysisServer;
import com.fleety.base.GeneralConst;
import com.fleety.base.InfoContainer;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.fleety.util.pool.thread.BasicTask;
import com.fleety.util.pool.timer.FleetyTimerTask;

public class GradeCompanyAnasisServer extends AnalysisServer {
	private TimerTask task = null;
	@Override
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
			Timestamp nowTime = new Timestamp(System.currentTimeMillis());
			statTime.setTimeInMillis(anaDate.getTimeInMillis());
			
			Timestamp sTime = new Timestamp(anaDate.getTimeInMillis());
			anaDate.add(Calendar.DAY_OF_MONTH, 1);
			Timestamp eTime = new Timestamp(anaDate.getTimeInMillis());
			
			StatementHandle stmt = conn.prepareStatement("select * from ana_grade_company_daystat where grade_time between ? and ?");
			stmt.setTimestamp(1, sTime);
			stmt.setTimestamp(2, eTime);
			ResultSet sets = stmt.executeQuery();
			//判断是否已统计过
			if(sets.next()){
				return ;
			}
			conn.closeStatement(stmt);
	
			StringBuffer sqlBuff = new StringBuffer(1024);
			sqlBuff.append("select term_id,term_name,nvl(total,0) total,nvl(good, 0) good,nvl(notgood, 0) notgood,");
			sqlBuff.append(" nvl(verygood, 0) verygood,nvl(unknown, 0) unknown");
			sqlBuff.append(" from (select term_id,term_name,count(*) as total,sum(case grade_type when 0 then 1 else 0 end) as good,");
			sqlBuff.append(" sum(case grade_type when 1 then  1 else 0 end) as notgood,");
			sqlBuff.append(" sum(case grade_type when 2 then 1 else 0 end) as verygood,sum(case grade_type when 3 then 1 else 0 end) as unknown" );
            sqlBuff.append(" from (select g.*, t.term_id, t.term_name from (select * from grade where create_time between ? and ?");        
            sqlBuff.append(") g inner join car c  on c.car_id = g.car_no inner join term t on c.term_id = t.term_id) m group by term_id,term_name) gf ");         
			stmt = conn.prepareStatement(sqlBuff.toString());
			stmt.setTimestamp(1, sTime);
			stmt.setTimestamp(2, eTime);
			sets = stmt.executeQuery();
			ArrayList resultList = new ArrayList();
			InfoContainer info = null;
			while(sets.next())
			{   
				info = new InfoContainer();
				info.setInfo("term_id", sets.getInt("term_id"));
				info.setInfo("term_name", sets.getString("term_name"));
				info.setInfo("total", sets.getInt("total"));
				info.setInfo("good", sets.getInt("good"));
				info.setInfo("notgood", sets.getInt("notgood"));
				info.setInfo("verygood", sets.getInt("verygood"));
				info.setInfo("unknown", sets.getInt("unknown"));
				resultList.add(info);
			}
			conn.closeStatement(stmt);
			
			String sql = "insert into ana_grade_company_daystat("
					+ "id,comp_id,comp_name,satify_a,satify_b,satify_c,satify_d,"
					+ "grade_time,stat_time,total_grade)";
			sql += " values(?,?,?,?,?,?,?,?,?,?)";
			stmt = conn.prepareStatement(sql);
			long id = DbServer.getSingleInstance().getAvaliableId(conn, "ana_grade_company_daystat", "id", true);
			for(int i = 0; i < resultList.size(); i ++)
			{
				info = (InfoContainer) resultList.get(i);
				stmt.setLong(1, id ++);
				stmt.setInt(2, info.getInteger("term_id"));
				stmt.setString(3, info.getString("term_name"));
				stmt.setInt(4, info.getInteger("verygood"));
				stmt.setDouble(5, info.getInteger("good"));
				stmt.setInt(6, info.getInteger("notgood"));
				stmt.setInt(7, info.getInteger("unknown"));
				stmt.setTimestamp(8,sTime);
				stmt.setTimestamp(9, nowTime);
				stmt.setInt(10, info.getInteger("total"));
				stmt.addBatch();
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
	
	private class TimerTask extends FleetyTimerTask{
		public void run(){
			Calendar cal = Calendar.getInstance();
			cal.add(Calendar.DAY_OF_MONTH, -1);
			cal.set(Calendar.HOUR_OF_DAY, 0);
			cal.set(Calendar.MINUTE, 0);
			cal.set(Calendar.SECOND, 0);
			cal.set(Calendar.MILLISECOND, 0);
			
			System.out.println("Fire ExecTask GradeCompanyAnasisServer:"+GeneralConst.YYYY_MM_DD_HH.format(cal.getTime()));
			GradeCompanyAnasisServer.this.addExecTask(new ExecTask(cal));
		}
	}
	
	private class ExecTask extends BasicTask{
		private Calendar anaDate = null;
		public ExecTask(Calendar anaDate){
			this.anaDate = anaDate;
		}
		
		public boolean execute() throws Exception{
			GradeCompanyAnasisServer.this.executeTask(this.anaDate);
			return true;
		}
		
		public String getDesc(){
			return "企业的服务评价日统计服务";
		}
		public Object getFlag(){
			return "GradeCompanyAnasisServer";
		}
	}
}

