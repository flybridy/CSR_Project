package com.fleety.analysis.order;

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

public class OrderCompanyAnasisServer extends AnalysisServer {
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
	
	private HashMap getCarCompanyMap()
	{
		HashMap ccMap = new HashMap();
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			StatementHandle stmt = conn.createStatement();
			ResultSet sets = stmt.executeQuery("select term_id,count(car_id)total from car where term_id is not null and mdt_id>0 group by term_id");
			while(sets.next())
			{
				ccMap.put(new Integer(sets.getInt("term_id")), new Integer(sets.getInt("total")));
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}
		return ccMap;
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
			
			StatementHandle stmt = conn.prepareStatement("select * from ana_order_company_stat where stat_time between ? and ?");
			stmt.setTimestamp(1, sTime);
			stmt.setTimestamp(2, eTime);
			ResultSet sets = stmt.executeQuery();
			if(sets.next()){
				return ;
			}
			conn.closeStatement(stmt);
	
			HashMap ccMap = this.getCarCompanyMap();
			StringBuffer sqlBuff = new StringBuffer(1024);
			sqlBuff.append("select car_company,count(order_id)total from taxi_order_list where car_company is not null and car_wanted_time ");
			sqlBuff.append(" between ? and ? group by car_company");
			stmt = conn.prepareStatement(sqlBuff.toString());
			stmt.setTimestamp(1, sTime);
			stmt.setTimestamp(2, eTime);
			sets = stmt.executeQuery();
			ArrayList resultList = new ArrayList();
			InfoContainer info = null;
			int companyId = 0, totalOrder = 0, totalCar = 0;
			double avgOrder = 0;
			while(sets.next())
			{
				info = new InfoContainer();
				companyId = sets.getInt("car_company");
				totalOrder = sets.getInt("total");
				totalCar = (Integer)ccMap.get(new Integer(companyId));
				if(totalCar > 0){
					avgOrder = (totalOrder*1.0)/totalCar;
				}
				info.setInfo("company", new Integer(companyId));
				info.setInfo("total_order", new Integer(totalOrder));
				info.setInfo("total_car", new Integer(totalCar));
				info.setInfo("avg_order", new Double(avgOrder));
				resultList.add(info);
			}
			conn.closeStatement(stmt);
			
			String sql = "insert into ana_order_company_stat(";
			sql += "id,company_id,order_total,car_total,car_avg,stat_time";
			sql += ") values(?,?,?,?,?,?)";
			stmt = conn.prepareStatement(sql);
			long id = DbServer.getSingleInstance().getAvaliableId(conn, "ana_order_company_stat", "id", true);
			for(int i = 0; i < resultList.size(); i ++)
			{
				info = (InfoContainer) resultList.get(i);
				stmt.setLong(1, id ++);
				stmt.setInt(2, info.getInteger("company"));
				stmt.setInt(3, info.getInteger("total_order"));
				stmt.setInt(4, info.getInteger("total_car"));
				stmt.setDouble(5, info.getDouble("avg_order"));
				stmt.setTimestamp(6, sTime);
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
			
			System.out.println("Fire ExecTask OrderCompanyAnasisServer:"+GeneralConst.YYYY_MM_DD_HH.format(cal.getTime()));
			OrderCompanyAnasisServer.this.addExecTask(new ExecTask(cal));
		}
	}
	
	private class ExecTask extends BasicTask{
		private Calendar anaDate = null;
		public ExecTask(Calendar anaDate){
			this.anaDate = anaDate;
		}
		
		public boolean execute() throws Exception{
			OrderCompanyAnasisServer.this.executeTask(this.anaDate);
			return true;
		}
		
		public String getDesc(){
			return "公司电召业务统计分析";
		}
		public Object getFlag(){
			return "OrderCompanyAnasisServer";
		}
	}
}

