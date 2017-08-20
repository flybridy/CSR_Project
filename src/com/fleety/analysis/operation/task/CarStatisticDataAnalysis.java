package com.fleety.analysis.operation.task;

import java.sql.ResultSet;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;

import server.db.DbServer;

import com.fleety.analysis.AnalysisServer;
import com.fleety.analysis.operation.IOperationAnalysis;
import com.fleety.base.GeneralConst;
import com.fleety.base.InfoContainer;
import com.fleety.base.StrFilter;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;

public class CarStatisticDataAnalysis implements IOperationAnalysis{
	
	private HashMap          resultMapping  = null;
	
	@Override
	public boolean startAnalysisOperation(AnalysisServer parentServer,InfoContainer statInfo)
	{
		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
		Date eTime = statInfo.getDate(STAT_END_TIME_DATE);
		
		//
		this.resultMapping = null;
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			StatementHandle stmt = conn.createStatement();
			StringBuilder sb = new StringBuilder();
			sb.append("select count(*) as sum from ANA_CAR_STATISTIC_V_TABLE")
					.append(" where time='")
					.append(GeneralConst.YYYY_MM_DD.format(sTime)).append("'");
			ResultSet sets = stmt.executeQuery(sb.toString());
			if (sets.next()) {
				int sum = sets.getInt("sum");
				if(sum == 0)
					this.resultMapping = new HashMap();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}

		if (this.resultMapping == null) {
			System.out.println("Not Need Analysis:" + this.toString());
		} else {
			System.out.println("Start Analysis:" + this.toString());
		}

		return this.resultMapping != null;
	}

	@Override
	public void analysisDestOperation(AnalysisServer parentServer, InfoContainer statInfo)
	{
		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
		Date eTime = statInfo.getDate(STAT_END_TIME_DATE);
		String startTime=GeneralConst.YYYY_MM_DD.format(sTime);
		String endTime=GeneralConst.YYYY_MM_DD_HH_MM_SS.format(eTime);
		
		String startHighPeak1="0630";
		String endHighPeak1="0900";
		String startHighPeak2="1630";
		String endHighPeak2="1900";
		
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try{
			StringBuilder sql = new StringBuilder();
			
			sql.append(" select ");
			sql.append(" to_char(car_wanted_time,'yyyy-MM-dd')||car_no as id,");
			sql.append(" car_no,");
			sql.append(" sum(case when 1=1 then 1 end )as total,");
			sql.append(" to_char(car_wanted_time,'yyyy-MM-dd') as time,");
			sql.append(" to_date(to_char(car_wanted_time,'yyyy-MM-dd'),'yyyy-MM-dd') as timeDate,");
			sql.append(" sum(case when is_immediate=0 then 1  else 0 end )as BOOK_TOTAL,");
			sql.append(" sum(case when (status=2 or status=3) then 1 else 0 end )as finish_total,");
			sql.append(" sum(case when is_immediate=0 and (status=2 or status=3) then 1  else 0 end )as finish_book_total,");
			sql.append(" sum(case when auto_send=1 and (status=2 or status=3) then 1  else 0 end )as finish_assign_total,");
			sql.append(" sum(case when (to_char(car_wanted_time,'HH24mi')>='"+startHighPeak1+"' and to_char(car_wanted_time,'HH24mi')<='"+endHighPeak1+"')");
			sql.append(" or (to_char(car_wanted_time,'HH24mi')>='"+startHighPeak2+"' and to_char(car_wanted_time,'HH24mi')<='"+endHighPeak2+"') then 1 else 0 end )");
			sql.append(" as high_peak_total,");
			sql.append(" sum(case when ((to_char(car_wanted_time,'HH24mi')>='"+startHighPeak1+"' and to_char(car_wanted_time,'HH24mi')<='"+endHighPeak1+"')");
			sql.append(" or (to_char(car_wanted_time,'HH24mi')>='"+startHighPeak2+"' and to_char(car_wanted_time,'HH24mi')<='"+endHighPeak2+"')) and is_immediate=0 then 1 else 0 end )");
			sql.append(" as high_peak_book_total,");
			sql.append(" sum(case when ((to_char(car_wanted_time,'HH24mi')>='"+startHighPeak1+"' and to_char(car_wanted_time,'HH24mi')<='"+endHighPeak1+"')");
			sql.append(" or (to_char(car_wanted_time,'HH24mi')>='"+startHighPeak2+"' and to_char(car_wanted_time,'HH24mi')<='"+endHighPeak2+"')) and  (status=2 or status=3) then 1 else 0 end )");
			sql.append(" as high_peak_finish_total,");
			sql.append(" sum(case when ((to_char(car_wanted_time,'HH24mi')>='"+startHighPeak1+"' and to_char(car_wanted_time,'HH24mi')<='"+endHighPeak1+"')");
			sql.append(" or (to_char(car_wanted_time,'HH24mi')>='"+startHighPeak2+"' and to_char(car_wanted_time,'HH24mi')<='"+endHighPeak2+"')) and is_immediate=0  and (status=2 or status=3) then 1 else 0 end )");
			sql.append(" as high_peak_finish_book_total,");
			sql.append(" sum(case when ((to_char(car_wanted_time,'HH24mi')>='"+startHighPeak1+"' and to_char(car_wanted_time,'HH24mi')<='"+endHighPeak1+"')");
			sql.append(" or (to_char(car_wanted_time,'HH24mi')>='"+startHighPeak2+"' and to_char(car_wanted_time,'HH24mi')<='"+endHighPeak2+"')) and auto_send=1  and (status=2 or status=3) then 1 else 0 end )");
			sql.append(" as high_peak_finish_assign_total ");
			sql.append(" from (select * from taxi_order_list where car_wanted_time>=to_date('"+startTime+"','yyyy-mm-dd') and car_wanted_time<=to_date('"+endTime+"','yyyy-mm-dd hh24:mi:ss')) group by to_char(car_wanted_time,'yyyy-MM-dd'),car_no");
			
			System.out.println(sql.toString());
			StatementHandle stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql.toString());
			while(rs.next())
			{
				String id = rs.getString("id");
				CarStatisticInfo cInfo = new CarStatisticInfo();
				cInfo.id = id;
				cInfo.car_no=rs.getString("car_no");
				cInfo.time=rs.getString("time");
				cInfo.total=rs.getInt("total");		
				cInfo.book_total=rs.getInt("book_total");
				cInfo.finish_total=rs.getInt("finish_total");
				cInfo.finish_book_total=rs.getInt("finish_book_total");
				cInfo.finish_assign_total=rs.getInt("finish_assign_total");				
				cInfo.high_peak_total=rs.getInt("high_peak_total");
				cInfo.high_peak_book_total=rs.getInt("high_peak_book_total");
				cInfo.high_peak_finish_total=rs.getInt("high_peak_finish_total");
				cInfo.high_peak_finish_book_total=rs.getInt("high_peak_finish_book_total");
				cInfo.high_peak_finish_assign_total=rs.getInt("high_peak_finish_assign_total");
				
				resultMapping.put(id, cInfo);
			}
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
	}
	
	@Override
	public void endAnalysisOperation(AnalysisServer parentServer,InfoContainer statInfo) {
		if(this.resultMapping == null){ 
			return ;
		}
		
		int recordNum = 0;
		String innerId = null;
		CarStatisticInfo cInfo;
		Date sDate = statInfo.getDate(STAT_START_TIME_DATE);
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try{
			conn.setAutoCommit(false);
			// inset into databases
			StatementHandle stmt = conn
					.prepareStatement("insert into ana_car_statistic_v_table "
							+ " (id, car_no, time, timedate, "
							+ " total, book_total, finish_total, finish_book_total, "
							+ " finish_assign_total, high_peak_total, high_peak_book_total, "
							+ " high_peak_finish_total, high_peak_finish_book_total, "
							+ " high_peak_finish_assign_total) "
							+ " values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
			for(Iterator itr = this.resultMapping.keySet().iterator();itr.hasNext();){
				innerId = (String)itr.next();
				cInfo = (CarStatisticInfo)this.resultMapping.get(innerId);
				stmt.setString(1, cInfo.id);
				stmt.setString(2, StrFilter.getNotNullString(cInfo.car_no));
				stmt.setString(3, cInfo.time);
				stmt.setDate(4, new java.sql.Date(GeneralConst.YYYY_MM_DD.parse(cInfo.time).getTime()));
				stmt.setInt(5, cInfo.total);
				stmt.setInt(6, cInfo.book_total);
				stmt.setInt(7, cInfo.finish_total);
				stmt.setInt(8, cInfo.finish_book_total);
				stmt.setInt(9, cInfo.finish_assign_total);
				stmt.setInt(10, cInfo.high_peak_total);
				stmt.setInt(11, cInfo.high_peak_book_total);
				stmt.setInt(12, cInfo.high_peak_finish_total);
				stmt.setInt(13, cInfo.high_peak_finish_book_total);
				stmt.setInt(14, cInfo.high_peak_finish_assign_total);
				
				stmt.addBatch();
				recordNum ++;
			}
			stmt.executeBatch();
			conn.commit();
		}catch(Exception e){
			e.printStackTrace();
			if(conn != null){
				try{
					conn.rollback();
				}catch(Exception ee){
					ee.printStackTrace();
				}
			}
			recordNum = 0;
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
		System.out.println("Finish company operate data Analysis:"+this.toString()+" recordNum="+recordNum);
	}
	
	private class CarStatisticInfo
	{
		public String id;
		public String car_no="";
		public String time="";
		
		public int total=0;
		public int book_total=0;
		public int finish_total=0;
		public int finish_book_total=0;
		public int finish_assign_total=0;
		
		public int high_peak_total=0;
		public int high_peak_book_total=0;
		public int high_peak_finish_total=0;
		public int high_peak_finish_book_total=0;
		public int high_peak_finish_assign_total=0;
	}

}
