package com.fleety.analysis.operation;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;

import server.db.DbServer;

import com.fleety.analysis.AnalysisServer;
import com.fleety.base.GeneralConst;
import com.fleety.common.redis.Gps_Pos;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.fleety.util.pool.db.redis.RedisConnPoolServer;
import com.fleety.util.pool.db.redis.RedisTableBean;
import com.fleety.util.pool.thread.BasicTask;
import com.fleety.util.pool.timer.FleetyTimerTask;

public class GuzhangAnalysisServer extends AnalysisServer {
	private TimerTask task = null;

	private int times = 1;

	public boolean startServer() {
		super.startServer();
		if(!this.isRunning()){
			return this.isRunning();
		}
		
		if(this.getStringPara("times") != null){
			this.times = this.getIntegerPara("times").intValue();
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
			
			StatementHandle stmt = conn.prepareStatement("select * from ANA_GUZHANG_DAY_STAT where stat_time between ? and ?");
			stmt.setTimestamp(1, sTime);
			stmt.setTimestamp(2, eTime);
			ResultSet sets = stmt.executeQuery();
			if(sets.next()){
				return ;
			}
			conn.closeStatement(stmt);
			
			stmt = conn.createStatement();
			
			int gzNum = this.getGzNum();
			int carNum = 0;
			float parcent = 0f;
			
			String statSql = "select count(*) cars from v_ana_dest_info";
			stmt = conn.createStatement();
			sets = stmt.executeQuery(statSql);
			
			if(sets.next()){
				carNum = sets.getInt("cars");
			}
			conn.closeStatement(stmt);
			
			parcent = (float)(gzNum*100.0/carNum);
			
			String sql = "insert into ANA_GUZHANG_DAY_STAT(id,stat_time,gz_num,car_num,parcent) values(?,?,?,?,?)";
			stmt = conn.prepareStatement(sql);
			stmt.setInt(1, (int)DbServer.getSingleInstance().getAvaliableId(conn, "ANA_GUZHANG_DAY_STAT", "id"));
			stmt.setTimestamp(2, new Timestamp(statTime.getTimeInMillis()));
			stmt.setInt(3, gzNum);
			stmt.setInt(4, carNum);
			stmt.setFloat(5, parcent);
			
			stmt.executeUpdate();
	
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
	
	private int getGzNum(){
		int gzNum = 0;
		long currentDate = System.currentTimeMillis();
		double scale = 1000*60*60*24.0,days;
		Gps_Pos bean = new Gps_Pos();

		try {
			List list = RedisConnPoolServer.getSingleInstance().queryTableRecord(new RedisTableBean[]{bean});
			if(list != null && list.size()>0){
				for(int i=0;i<list.size();i++){
					bean = (Gps_Pos)list.get(i);
					days = (currentDate - bean.getSysDate().getTime())/scale;
					if(days >= times){
						gzNum++;
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return gzNum ;
	}

	private class TimerTask extends FleetyTimerTask{
		public void run(){
			Calendar cal = Calendar.getInstance();
			cal.add(Calendar.DAY_OF_MONTH, -1);
			cal.set(Calendar.HOUR_OF_DAY, 0);
			cal.set(Calendar.MINUTE, 0);
			cal.set(Calendar.SECOND, 0);
			cal.set(Calendar.MILLISECOND, 0);
			
			System.out.println("Fire ExecTask GuzhangAnalysisServer:"+GeneralConst.YYYY_MM_DD_HH.format(cal.getTime()));
			GuzhangAnalysisServer.this.addExecTask(new ExecTask(cal));
		}
	}
	
	private class ExecTask extends BasicTask{
		private Calendar anaDate = null;
		public ExecTask(Calendar anaDate){
			this.anaDate = anaDate;
		}
		
		public boolean execute() throws Exception{
			GuzhangAnalysisServer.this.executeTask(this.anaDate);
			return true;
		}
		
		public String getDesc(){
			return "故障率统计分析服务";
		}
		public Object getFlag(){
			return "GuzhangAnalysisServer";
		}
	}
}

