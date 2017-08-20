package com.fleety.analysis.driverchange;

import java.io.OutputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import oracle.jdbc.OracleConnection;
import oracle.sql.BLOB;
import oracle.sql.CLOB;
import server.cluster.GISMarkClusterInstance;
import server.cluster.GISMarkClusterInstance.Cluster;
import server.cluster.GISMarkClusterInstance.PointInfo;
import server.db.DbServer;

import com.fleety.analysis.AnalysisServer;
import com.fleety.base.GeneralConst;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.fleety.util.pool.thread.BasicTask;
import com.fleety.util.pool.timer.FleetyTimerTask;

public class DriverChangeLocationAnalysisServer extends AnalysisServer {
	private TimerTask task = null;
	private int statKilo = 1000;
	private int minClusterNum = 10;
	@Override
	public boolean startServer() {
		super.startServer();
		if(!this.isRunning()){
			return this.isRunning();
		}

		if(this.getIntegerPara("stat_kilo") != null){
			this.statKilo = this.getIntegerPara("stat_kilo").intValue();
		}
		if(this.getIntegerPara("min_cluster_num") != null){
			this.minClusterNum = this.getIntegerPara("min_cluster_num").intValue();
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

			GISMarkClusterInstance instance,instance1 = new GISMarkClusterInstance();
			instance1.setGridSizeWithMi(this.statKilo);
			GISMarkClusterInstance instance2 = new GISMarkClusterInstance();
			instance2.setGridSizeWithMi(this.statKilo);
			
			long splitTime = anaDate.getTimeInMillis()+GeneralConst.ONE_DAY_TIME/2;
			
			long time;
			String id;
			HashMap mapping = new HashMap();
			StatementHandle stmt = conn.prepareStatement("select OLD_SERVICE_NO,lo,la,time from DRIVER_CHANGE_RECORD where time between ? and ?");
			stmt.setTimestamp(1, new Timestamp(anaDate.getTimeInMillis()));
			stmt.setTimestamp(2, new Timestamp(anaDate.getTimeInMillis()+GeneralConst.ONE_DAY_TIME));
			ResultSet sets = stmt.executeQuery();
			while(sets.next()){
				time = sets.getTimestamp("time").getTime();
				if(time<splitTime){
					instance = instance1;
				}else{
					instance = instance2;
				}
				
				id = sets.getString("OLD_SERVICE_NO");
				instance.addPoint(id, sets.getDouble("lo"), sets.getDouble("la"));
			}
			stmt.close();

			this.countCluster(anaDate, conn, instance1.getClusterPoint(),1);
			this.countCluster(anaDate, conn, instance2.getClusterPoint(),2);
			
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
	
	private void countCluster(Calendar anaDate,DbHandle conn,List clusterList,int changeType) throws Exception{
		
		Map<Long,String> clobs = new HashMap<Long,String>();
		StatementHandle stmt = conn.prepareStatement("insert into ana_driver_change_location(id,lo,la,info,stat_time,driver_num,change_type) values(?,?,?,empty_clob(),?,?,?)");
		long rid,count = 0;
		Cluster cluster;
		PointInfo pInfo;
		StringBuffer strBuff = new StringBuffer(1024*10);
		conn.setAutoCommit(false);  
		for(Iterator itr = clusterList.iterator();itr.hasNext();){
			cluster = (Cluster)itr.next();
			if(cluster.pList.size() > this.minClusterNum){
				strBuff.delete(0, strBuff.length());
				for(Iterator itr1 = cluster.pList.iterator();itr1.hasNext();){
					pInfo = (PointInfo)itr1.next();
					strBuff.append(pInfo.id+","+pInfo.lo+","+pInfo.la+";");
				}
				rid = DbServer.getSingleInstance().getAvaliableId(conn, "ana_driver_change_location", "id");
				stmt.setLong(1, rid);
				stmt.setDouble(2, cluster.clo);
				stmt.setDouble(3, cluster.cla);
				clobs.put(rid,strBuff.toString());
				stmt.setTimestamp(4, new Timestamp(anaDate.getTimeInMillis()));
				stmt.setInt(5, cluster.pList.size());
				stmt.setInt(6, changeType);
				stmt.addBatch();
				count ++;
				if(count >= 100){
					stmt.executeBatch();
					count = 0;
				}
			}
		}
		if(count > 0){
			stmt.executeBatch();
			
		}
		conn.commit();
		try {
			conn.setAutoCommit(false);
			StatementHandle prepapstmt = conn
					.prepareStatement("select info from ana_driver_change_location where id=? for update");
			
			StatementHandle prepapstmt2 = conn
					.prepareStatement("update ana_driver_change_location set info=? where id=?");
			
			
			ResultSet rs;
			Iterator<Long> ids = clobs.keySet().iterator();
			while(ids.hasNext()){
				Long updateid = ids.next();
				prepapstmt.setLong(1, updateid);
				rs = prepapstmt.executeQuery();
				   if(rs.next()){  
			            //获取clob对象，此处的clob是oracle.sql.Clob  
			            CLOB clob = (CLOB)rs.getClob(1) ;  
			            clob.putString(1, clobs.get(updateid));  
			            prepapstmt2.setLong(2, updateid); 
			            prepapstmt2.setObject(1, clob);  	
			            prepapstmt2.executeUpdate() ;  
			        }  
			}

			conn.commit();
		} catch (Exception e) {
			try {
				conn.rollback();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
			// TODO Auto-generated catch block
			e.printStackTrace();
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
			
			System.out.println("Fire ExecTask DriverChangeLocationAnalysisServer:"+GeneralConst.YYYY_MM_DD_HH.format(cal.getTime()));
			DriverChangeLocationAnalysisServer.this.addExecTask(new ExecTask(cal));
		}
	}
	
	private class ExecTask extends BasicTask{
		private Calendar anaDate = null;
		public ExecTask(Calendar anaDate){
			this.anaDate = anaDate;
		}
		
		public boolean execute() throws Exception{
			DriverChangeLocationAnalysisServer.this.executeTask(this.anaDate);
			return true;
		}
		
		public String getDesc(){
			return "驾驶员交接班地点统计";
		}
		public Object getFlag(){
			return "DriverChangeAnalysisServer";
		}
	}
	
	
}
