package com.fleety.analysis.alipay;

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

public class AlipayStatServer  extends AnalysisServer {
	private TimerTask task = null;
	private String statSql = "select to_char(sysdate - 1, 'yyyy-MM-dd') stat_time,count(*) total,sum(decode(dispatch_order_id, 0, 1, 0)) yang_zhao,sum(decode(dispatch_order_id, 0, 0, 1)) dian_zhao,sum(decode(user_id,888,0,222222,0,333333,0,444444,0,null,0,1)) ivr,sum(decode(user_id, 888, 1, 222222, 1, 333333, 1, 444444, 1, 0)) no_ivr from alipay_trade_order_info t1 left join taxi_order_list t2 on t1.dispatch_order_id = t2.order_id where t1.status in (4, 5) and to_char(create_time, 'yyyy-MM-dd') = to_char(sysdate - 1, 'yyyy-MM-dd')";

	public boolean startServer() {
		super.startServer();
		if(!this.isRunning()){
			return this.isRunning();
		}

		if(this.getStringPara("stat_sql") != null){
			this.statSql = this.getStringPara("stat_sql");
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
			
			StatementHandle stmt = conn.prepareStatement("select * from ANA_ALIPAY_DAY_STAT where stat_time between ? and ?");
			stmt.setTimestamp(1, sTime);
			stmt.setTimestamp(2, eTime);
			ResultSet sets = stmt.executeQuery();
			if(sets.next()){
				return ;
			}
			conn.closeStatement(stmt);
			
			stmt = conn.createStatement();
			
			RecordInfo rInfo = null;
			sets = stmt.executeQuery(statSql);
			
			if(sets.next()){
				rInfo = new RecordInfo();
				rInfo.totalNum = sets.getInt("total");
				rInfo.dzNum = sets.getInt("dian_zhao");
				rInfo.yzNum = sets.getInt("yang_zhao");
				rInfo.ivrNum = sets.getInt("ivr");
				rInfo.otherNum = sets.getInt("no_ivr");
				rInfo.statTime = sets.getString("stat_time");
			}
			conn.closeStatement(stmt);
			
			String sql = "insert into ANA_ALIPAY_DAY_STAT(id,stat_time,TOTAL_NUM,DIANZHAO_NUM,YANGZHAO_NUM,IVR_NUM,OTHER_NUM) values(?,?,?,?,?,?,?)";
			stmt = conn.prepareStatement(sql);
			stmt.setInt(1, (int)DbServer.getSingleInstance().getAvaliableId(conn, "ANA_ALIPAY_DAY_STAT", "id"));
			stmt.setTimestamp(2, new Timestamp(GeneralConst.YYYY_MM_DD.parse(rInfo.statTime).getTime()));
			stmt.setInt(3, rInfo.totalNum);
			stmt.setInt(4, rInfo.dzNum);
			stmt.setInt(5, rInfo.yzNum);
			stmt.setInt(6, rInfo.ivrNum);
			stmt.setInt(7, rInfo.otherNum);
			
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
	

	private class RecordInfo{
		public int totalNum = 0;
		public int dzNum = 0;
		public int yzNum = 0;
		public int ivrNum = 0;
		public int otherNum = 0;
		public String statTime = "";
		
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
			
			System.out.println("Fire ExecTask AlipayStatServer:"+GeneralConst.YYYY_MM_DD_HH.format(cal.getTime()));
			AlipayStatServer.this.addExecTask(new ExecTask(cal));
		}
	}
	
	private class ExecTask extends BasicTask{
		private Calendar anaDate = null;
		public ExecTask(Calendar anaDate){
			this.anaDate = anaDate;
		}
		
		public boolean execute() throws Exception{
			AlipayStatServer.this.executeTask(this.anaDate);
			return true;
		}
		
		public String getDesc(){
			return "支付宝交易统计服务";
		}
		public Object getFlag(){
			return "AlipayStatServer";
		}
	}
}
