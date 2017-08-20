package com.fleety.analysis.order;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import server.db.DbServer;
import com.fleety.analysis.AnalysisServer;
import com.fleety.analysis.track.task.key_area.KeyAreaInfo;
import com.fleety.analysis.track.task.key_area.KeyAreaInfoUtil;
import com.fleety.base.GeneralConst;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.fleety.util.pool.timer.FleetyTimerTask;

public class KeyAreaOrderStatServer extends AnalysisServer 
{
	private TimerTask task = null;
	public boolean startServer()
	{
		super.startServer();
		if(!this.isRunning){
			return this.isRunning;
		}
		int hour = this.getIntegerPara("hour").intValue();
		int minute = this.getIntegerPara("minute").intValue();

		Calendar cal = this.getNextExecCalendar(hour, minute);
		if(cal.get(Calendar.DAY_OF_MONTH) != Calendar.getInstance().get(Calendar.DAY_OF_MONTH)){
			this.scheduleTask(new TimerTask(), 500);
		}
		long delay = cal.getTimeInMillis() - System.currentTimeMillis();
		this.isRunning = this.scheduleTask(this.task = new TimerTask(), delay, GeneralConst.ONE_DAY_TIME);
		
		return this.isRunning;
	}

	public void stopServer()
	{
		if(this.task != null)
		{
			this.task.cancel();
		}
		this.isRunning = false;
	}
	private class TimerTask extends FleetyTimerTask
	{
		public void run() 
		{
			HashMap orderMap = KeyAreaOrderStatServer.this.statOrderCount();
			KeyAreaOrderStatServer.this.logDB(orderMap);
		}
		
	}
	
	private HashMap statOrderCount()
	{
		HashMap resultMap = new HashMap();
		ArrayList orderList = new ArrayList();
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try 
		{
			String sql = "select to_char(car_wanted_time,'hh24') as hour,order_id,cust_longitude lo,cust_latitude la "
				+ " from taxi_order_list where car_wanted_time >= ? and car_wanted_time <= ?";
			
			Calendar calStart = Calendar.getInstance();
			calStart.add(Calendar.DAY_OF_MONTH, -1);
			calStart.set(Calendar.HOUR_OF_DAY, 0);
			calStart.set(Calendar.MINUTE, 0);
			calStart.set(Calendar.SECOND, 0);
			
			Calendar calEnd = Calendar.getInstance();
			calEnd.add(Calendar.DAY_OF_MONTH, -1);
			calEnd.set(Calendar.HOUR_OF_DAY, 23);
			calEnd.set(Calendar.MINUTE, 59);
			calEnd.set(Calendar.SECOND, 59);
			
			StatementHandle pstmt = conn.prepareStatement(sql);
			pstmt.setTimestamp(1, new java.sql.Timestamp(calStart.getTimeInMillis()));
			pstmt.setTimestamp(2, new java.sql.Timestamp(calEnd.getTimeInMillis()));
			ResultSet sets = pstmt.executeQuery();
			OrderInfo orderInfo = null;
			
			while(sets.next())
			{
				orderInfo = new OrderInfo();
				orderInfo.hour = sets.getString("hour");
				orderInfo.orderId = sets.getInt("order_id");
				orderInfo.lo = sets.getDouble("lo");
				orderInfo.la = sets.getDouble("la");
				orderList.add(orderInfo);
			}
			
			resultMap=KeyAreaInfoUtil.getAreaInfo();
			Iterator it = resultMap.keySet().iterator();
			KeyAreaInfo keyArea=null;
			Integer areaId=null;
			while(it.hasNext())
			{
				areaId = (Integer)it.next();
				keyArea = (KeyAreaInfo) resultMap.get(areaId);
				for(int i = 0; i < orderList.size(); i ++)
				{
					orderInfo = (OrderInfo) orderList.get(i);
					if(keyArea.isInArea(orderInfo.lo, orderInfo.la)){
						Integer num = (Integer) keyArea.statMap.get(orderInfo.hour);
						if(num == null){
							keyArea.statMap.put(orderInfo.hour, 1);
						}else{
							keyArea.statMap.put(orderInfo.hour, ++num);
						}
					}
				}
			}
			
		} 
		catch (SQLException e) 
		{
			e.printStackTrace();
		}
		finally
		{
			DbServer.getSingleInstance().releaseConn(conn);
		}
		
		return resultMap;
	}
	
	private void logDB(HashMap orderMap)
	{
		if(orderMap == null || orderMap.size() == 0){
			return;
		}
		
		DbHandle conn = DbServer.getSingleInstance().getConn();
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DAY_OF_MONTH, -1);
		try 
		{
			StatementHandle pstmt = conn.prepareStatement("update key_area_car_stat set order_num=? "
					+ " where to_char(stat_time,'yyyy-mm-dd')=? and hour = ? and area_id=?");
			Iterator it = orderMap.keySet().iterator();
			String hour = "";
			int orderNum = 0,areaId = 0;
			KeyAreaInfo keyArea;
			Iterator it1 ;
			while(it.hasNext())
			{
				areaId = (Integer) it.next();
				keyArea = (KeyAreaInfo) orderMap.get(areaId);
				it1 = keyArea.statMap.keySet().iterator();
				while(it1.hasNext())
				{
					hour = (String) it1.next();
					orderNum = (Integer)keyArea.statMap.get(hour);
					pstmt.setInt(1, orderNum);
					pstmt.setString(2, GeneralConst.YYYY_MM_DD.format(cal.getTime()));
					pstmt.setString(3, filterHour(hour));
					pstmt.setInt(4, areaId);
					pstmt.addBatch();
				}
			}
			pstmt.executeBatch();
			System.out.println("KeyAreaOrderStatServer end log db! result size:" + orderMap.size());
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
			System.out.println("KeyAreaOrderStatServer log db fail!");
		} 
		finally 
		{
			DbServer.getSingleInstance().releaseConn(conn);
		}
		
	}
	
	private String filterHour(String hour)
	{
		if(hour.startsWith("0")){
			hour = hour.substring(1, hour.length());
		}
		return hour;
	}
	
	private class OrderInfo
	{
		private int orderId;
		private double lo;
		private double la;
		private String hour;
	}
}
