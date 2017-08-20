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
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;

public class TeamPerformDataAnalysis implements IOperationAnalysis{
	
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
			sb.append("select count(*) as sum from ANA_TEAM_PERFORM_V_TABLE")
					.append(" where day='")
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
			
			sql.append("select team.id || team.name || company.term_id || company.term_name ||orders.day || orders.com_total || orders.com_no_mdt ||orders_h.com_total || orders_h.com_no_mdt || orders.move_total ||orders_h.move_total as inner_id,");
			sql.append(" team.id as team_id, team.name as team_name, company.term_id, company.term_name,decode(orders.day,null,'"+startTime+"',orders.day) as day,");
			sql.append(" nvl(orders.com_total, 0) as com_total,");
			sql.append(" nvl(orders.com_no_mdt, 0) as com_no_mdt,");
			sql.append(" nvl(orders_h.com_total, 0) as com_total_h,");
			sql.append(" nvl(orders_h.com_no_mdt, 0) as com_no_mdt_h,");
			sql.append(" count(distinct car.car_id) as car_count,");
			sql.append(" nvl(orders.move_total, 0) as move_total,");
			sql.append(" nvl(orders.assign_total, 0) as assign_total,");
			sql.append(" nvl(orders_h.move_total, 0) as move_total_h,");
			sql.append(" nvl(orders_h.assign_total, 0) as assign_total_h");
			sql.append(" from car_team team");
			sql.append(" left join term company on team.term_id = company.term_id");
			sql.append(" left join (select o.car_team,");
			sql.append(" to_char(o.car_wanted_time, 'yyyy-MM-dd') as day,");
			sql.append(" count(distinct o.order_id) as com_total,");
			sql.append(" count(decode(o.status, 6, o.order_id, null)) as move_total,");
			sql.append(" count(decode(o.auto_send, 1, o.order_id, null)) as assign_total,");
			sql.append(" count(c.car_id) as com_no_mdt");
			sql.append(" from (select * from taxi_order_list where car_wanted_time>=to_date('"+startTime+"','yyyy-mm-dd') and car_wanted_time<=to_date('"+endTime+"','yyyy-mm-dd hh24:mi:ss')) o");
			sql.append(" left join car c on o.car_no = c.car_id");
			sql.append(" and c.mdt_id <= 0");
			sql.append("  where o.car_no is not null");
			sql.append(" group by o.car_team, to_char(o.car_wanted_time, 'yyyy-MM-dd')) orders on team.id = orders.car_team");
			sql.append(" left join (select o.car_team,");
			sql.append(" to_char(o.car_wanted_time, 'yyyy-MM-dd') as day,");
			sql.append(" count(distinct o.order_id) as com_total,");
			sql.append(" count(decode(o.status, 6, o.order_id, null)) as move_total,");
			sql.append(" count(decode(o.auto_send, 1, o.order_id, null)) as assign_total,");
			sql.append(" count(c1.car_id) as com_no_mdt");
			sql.append(" from (select * from taxi_order_list where car_wanted_time>=to_date('"+startTime+"','yyyy-mm-dd') and car_wanted_time<=to_date('"+endTime+"','yyyy-mm-dd hh24:mi:ss')) o");
			sql.append(" left join car c on o.car_no = c.car_id");
			sql.append(" left join car c1 on o.car_no = c1.car_id");
			sql.append(" and c1.mdt_id <= 0");
			sql.append(" where o.car_no is not null");
			sql.append(" and ((to_char(o.car_wanted_time, 'HH24mi') >= '"+startHighPeak1+"' and");
			sql.append(" to_char(o.car_wanted_time, 'HH24mi') <= '"+endHighPeak1+"') or");
			sql.append(" (to_char(o.car_wanted_time, 'HH24mi') >= '"+startHighPeak2+"' and");
			sql.append(" to_char(o.car_wanted_time, 'HH24mi') <= '"+endHighPeak2+"'))");
			sql.append(" group by o.car_team, to_char(o.car_wanted_time, 'yyyy-MM-dd')) orders_h on team.id =orders_h.car_team  and orders.day = orders_h.day");
			sql.append(" left join (select car.team_id, car.car_id");
			sql.append(" from car car, mdt_info mdt");
			sql.append(" where car.mdt_id = mdt.mdt_id");
			sql.append(" and car.mdt_id > 0");
			sql.append(" and mdt.gateway_id >= 1) car on car.team_id = team.id");
			sql.append(" group by team.id,");
			sql.append(" team.name,");
			sql.append(" company.term_id,");
			sql.append(" company.term_name,");
			sql.append(" orders.day,");
			sql.append(" orders.com_total,");
			sql.append(" orders.com_no_mdt,");
			sql.append(" orders_h.com_total,");
			sql.append(" orders_h.com_no_mdt,");
			sql.append(" orders.move_total,");
			sql.append(" orders_h.move_total,");
			sql.append(" orders.assign_total,");
			sql.append(" orders_h.assign_total");
			
			System.out.println(sql.toString());
			StatementHandle stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql.toString());
			while(rs.next())
			{
				String innerId = rs.getString("inner_id");
				TeamPerFormInfo cInfo = new TeamPerFormInfo();
				cInfo.innerId = innerId;
				cInfo.teamId=rs.getInt("team_id");
				cInfo.teamName=rs.getString("team_name");
				cInfo.termId=rs.getInt("term_id");
				cInfo.termName=rs.getString("term_name");
				cInfo.day=rs.getString("day");
				cInfo.comTotal=rs.getInt("com_total");
				cInfo.comNoMdt=rs.getInt("com_no_mdt");
				cInfo.comTotalH=rs.getInt("com_total_h");
				cInfo.comNoMdtH=rs.getInt("com_no_mdt_h");
				cInfo.carCount=rs.getInt("car_count");
				cInfo.moveTotal=rs.getInt("move_total");
				cInfo.assignTotal=rs.getInt("assign_total");
				cInfo.moveTotalH=rs.getInt("move_total_h");
				cInfo.assignTotalH=rs.getInt("assign_total_h");
				resultMapping.put(innerId, cInfo);
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
		TeamPerFormInfo cInfo;
		Date sDate = statInfo.getDate(STAT_START_TIME_DATE);
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try{
			conn.setAutoCommit(false);
			// inset into databases
			StatementHandle stmt = conn
					.prepareStatement("insert into ana_team_perform_v_table "
							+ " (inner_id, team_id, team_name, term_id, term_name, "
							+ " day, com_total, com_no_mdt, com_total_h, com_no_mdt_h, "
							+ " car_count, move_total, assign_total, "
							+ " move_total_h, assign_total_h) "
							+ " values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
			for(Iterator itr = this.resultMapping.keySet().iterator();itr.hasNext();){
				innerId = (String)itr.next();
				cInfo = (TeamPerFormInfo)this.resultMapping.get(innerId);
				stmt.setString(1, cInfo.innerId);
				stmt.setInt(2, cInfo.teamId);
				stmt.setString(3, cInfo.teamName);
				stmt.setInt(4, cInfo.termId);
				stmt.setString(5, cInfo.termName);
				stmt.setString(6, cInfo.day);
				stmt.setInt(7, cInfo.comTotal);
				stmt.setInt(8, cInfo.comNoMdt);
				stmt.setInt(9, cInfo.comTotalH);
				stmt.setInt(10, cInfo.comNoMdtH);
				stmt.setInt(11, cInfo.carCount);
				stmt.setInt(12, cInfo.moveTotal);
				stmt.setInt(13, cInfo.assignTotal);
				stmt.setInt(14, cInfo.moveTotalH);
				stmt.setInt(15, cInfo.assignTotalH);
				
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
	
	private class TeamPerFormInfo
	{
		public String innerId;
		public int teamId=0;
		public String teamName="";
		public int termId=0;
		public String termName="";
		public String day="";
		public int comTotal=0;
		public int comNoMdt=0;
		public int comTotalH=0;
		public int comNoMdtH=0;
		public int carCount=0;
		public int moveTotal=0;
		public int assignTotal=0;
		public int moveTotalH=0;
		public int assignTotalH=0;
	}

}
