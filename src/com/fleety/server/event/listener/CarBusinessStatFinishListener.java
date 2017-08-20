package com.fleety.server.event.listener;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

import server.db.DbServer;

import com.fleety.base.GeneralConst;
import com.fleety.base.InfoContainer;
import com.fleety.base.StrFilter;
import com.fleety.base.event.Event;
import com.fleety.base.event.EventListenerAdapter;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;

public class CarBusinessStatFinishListener extends EventListenerAdapter {
	
	private SimpleDateFormat sdf2            = new SimpleDateFormat("yyyy-MM-dd");

	private Date recountStartDate = null;// 闭区间，重新计算的起始日期，如果起始日期和结束日期中任一为空，不做重新计算
	private Date recountEndDate = null;// 闭区间，重新计算的结束日期，如果起始日期和结束日期中任一为空，不做重新计算
	private int minTimes = 0;// filter最小营运次数,如果是0不考虑此参数
	private int maxTimes = 0;// filter最大营运次数,如果是0不考虑此参数
	private int minTotalMile = 0;// filter最小总里程,如果是0不考虑此参数
	private int maxTotalMile = 0;// filter最大总里程,如果是0不考虑此参数
	private int minTotalIncome = 0;// filter最小总里程,如果是0不考虑此参数
	private int maxTotalIncome = 0;// filter最大总里程,如果是0不考虑此参数
	private float manageCost = 0;

	public void init() {
		try {
			String temp = (String) this.getPara("recount_start_date");
			if (StrFilter.hasValue(temp)) {
				this.recountStartDate = GeneralConst.YYYY_MM_DD.parse(temp);
			}
			temp = (String) this.getPara("recount_end_date");
			if (StrFilter.hasValue(temp)) {
				this.recountEndDate = GeneralConst.YYYY_MM_DD.parse(temp);
			}
			temp = (String) this.getPara("min_times");
			if (StrFilter.hasValue(temp)) {
				this.minTimes = Integer.parseInt(temp);
			}
			temp = (String) this.getPara("max_times");
			if (StrFilter.hasValue(temp)) {
				this.maxTimes = Integer.parseInt(temp);
			}
			temp = (String) this.getPara("min_total_mile");
			if (StrFilter.hasValue(temp)) {
				this.minTotalMile = Integer.parseInt(temp);
			}
			temp = (String) this.getPara("max_total_mile");
			if (StrFilter.hasValue(temp)) {
				this.maxTotalMile = Integer.parseInt(temp);
			}
			temp = (String) this.getPara("min_total_income");
			if (StrFilter.hasValue(temp)) {
				this.minTotalIncome = Integer.parseInt(temp);
			}
			temp = (String) this.getPara("max_total_income");
			if (StrFilter.hasValue(temp)) {
				this.maxTotalIncome = Integer.parseInt(temp);
			}
			temp = (String) this.getPara("manager_cost");
			if (StrFilter.hasValue(temp)) {
				this.manageCost = Integer.parseInt(temp);
			}

			if (this.recountStartDate != null
					&& this.recountEndDate != null
					&& this.recountEndDate.getTime() >= this.recountStartDate
							.getTime()) {
				long tempTime = this.recountStartDate.getTime();
				while (tempTime <= this.recountEndDate.getTime()) {
					this.countDistanceByDay(new Date(tempTime));
					tempTime = tempTime + GeneralConst.ONE_DAY_TIME;
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void eventHappen(Event event) {
		try {
			Date sDate = (Date) event.getEventPara();
			countDistanceByDay(sDate);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	/**
	 * 统计某天的数据
	 * 
	 * @param date
	 */
	private void countDistanceByDay(Date date) {
		DbHandle conn=null;
		try{
			conn=DbServer.getSingleInstance().getConn();
			if(conn == null){
				System.out.println("无法获取数据库连接");
				return ;
			}
			
			conn.setAutoCommit(false);
			this.clearDistanceByDay(date,conn);
			
			StatementHandle stmt=conn.createStatement();
			StringBuffer buff=new StringBuffer();

			buff.append("select area_id,")
			    .append("       count(*) as vehicle_numbers,")
			    .append("       round(sum(work_times) / count(*), 2) as work_times,")
			    .append("       round(sum(work_times_first) / count(*), 2) as work_times_first,")
			    .append("       round(sum(work_times_second) / count(*), 2) as work_times_second,")
			    .append("       round(sum(work_times_third) / count(*), 2) as work_times_third,")
			    .append("       round(sum(total_distance) / count(*), 2) as total_distance,")
			    .append("       round(sum(work_distance) / count(*), 2) as work_distance,")
			    .append("       round(sum(work_distance_first) / count(*), 2) as work_distance_first,")
			    .append("       round(sum(work_distance_second) / count(*), 2) as work_distance_second,")
			    .append("       round(sum(work_distance_third) / count(*), 2) as work_distance_third,")
			    .append("       round(sum(work_income_first) / count(*), 2) as work_income_first,")
			    .append("       round(sum(work_income_second) / count(*), 2) as work_income_second,")
			    .append("       round(sum(work_income_third) / count(*), 2) as work_income_third,")
			    .append("       round(sum(free_distance) / count(*), 2) as free_distance,")
			    .append("       round(((sum(waiting_hour) + (sum(waiting_minute)/60) + (sum(waiting_second)/3600)) / count(*)),2) as waiting_time,")
			    .append("       round(sum(total_income) / count(*), 2) as total_income,")
			    .append("       round(sum(work_income) / count(*), 2) as work_income,")
			    .append("       round(sum(work_income_day) / count(*), 2) as work_income_day,")
			    .append("       round(sum(work_income_night) / count(*), 2) as work_income_night,")
			    .append("       round(sum(fuel_income) / count(*), 2) as fuel_income,")
//			    .append("       as manager_cost,")
			    .append("       work_date as work_date")
			    .append(" from (")
			    .append("    select * from ANA_SINGLE_CAR_DAY_STAT m left join v_ana_dest_info n on m.plate_no = n.dest_no")
			    .append(" ) a")
//			    .append(" from ANA_SINGLE_CAR_DAY_STAT a")
			    .append(" where 1 = 1 ")
			    .append("");
			
			if(this.minTimes>0){
				buff.append(" and work_times>"+this.minTimes);
			}
			if(this.maxTimes>0){
				buff.append(" and work_times<"+this.maxTimes);
			}
			if(this.minTotalMile>0){
				buff.append(" and total_distance>"+this.minTotalMile);
			}
			if(this.maxTotalMile>0){
				buff.append(" and total_distance<"+this.maxTotalMile);
			}
			if(this.minTotalIncome>0){
				buff.append(" and total_income>"+this.minTotalIncome);
			}
			if(this.maxTotalIncome>0){
				buff.append(" and total_income<"+this.maxTotalIncome);
			}
			if(date != null)
				buff.append(" and work_date = '").append(sdf2.format(date)).append("'");
			buff.append(" group by area_id,work_date");
//			System.out.println(buff.toString());
			//查数据
			ResultSet rs = stmt.executeQuery(buff.toString());
			//写数据
			String sql="insert into ANA_SPECIAL_TOPIC_VEHICLE(id,area_id,vehicle_numbers,work_times,work_times_first,work_times_second,work_times_third,total_distance,work_distance,work_distance_first,work_distance_second,work_distance_third,free_distance,waiting_time,total_income,work_income,work_income_day,work_income_night,fuel_income,manager_cost,work_date,analysis_time,work_income_first,work_income_second,work_income_third) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			StatementHandle psmt=conn.prepareStatement(sql);
			while(rs.next()){
				//业务处理
				psmt.setInt(1, (int)DbServer.getSingleInstance().getAvaliableId(conn, "ANA_SPECIAL_TOPIC_VEHICLE", "id"));
				psmt.setInt(2, rs.getInt("area_id"));
				psmt.setInt(3, rs.getInt("vehicle_numbers"));
				psmt.setFloat(4, rs.getFloat("work_times"));
				psmt.setFloat(5, rs.getFloat("work_times_first"));
				psmt.setFloat(6, rs.getFloat("work_times_second"));
				psmt.setFloat(7, rs.getFloat("work_times_third"));
				psmt.setFloat(8, rs.getFloat("total_distance"));
				psmt.setFloat(9, rs.getFloat("work_distance"));
				psmt.setFloat(10, rs.getFloat("work_distance_first"));
				psmt.setFloat(11, rs.getFloat("work_distance_second"));
				psmt.setFloat(12, rs.getFloat("work_distance_third"));
				psmt.setFloat(13, rs.getFloat("free_distance"));
				psmt.setFloat(14, rs.getFloat("waiting_time"));
				psmt.setFloat(15, rs.getFloat("total_income"));
				psmt.setFloat(16, rs.getFloat("work_income"));
				psmt.setFloat(17, rs.getFloat("work_income_day"));
				psmt.setFloat(18, rs.getFloat("work_income_night"));
				psmt.setFloat(19, rs.getFloat("fuel_income"));
				psmt.setFloat(20, manageCost);
				psmt.setDate(21, new java.sql.Date(sdf2.parse(rs.getString("work_date")).getTime()));
				psmt.setDate(22, new java.sql.Date(new Date().getTime()));
				psmt.setFloat(23, rs.getFloat("work_income_first"));
				psmt.setFloat(24, rs.getFloat("work_income_second"));
				psmt.setFloat(25, rs.getFloat("work_income_third"));
				psmt.addBatch();
			}
			psmt.executeBatch();
			conn.commit();
		}catch(Exception ex){
			try {
				conn.rollback();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			ex.printStackTrace();
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}		
	}

	/**
	 * 清空某天的数据
	 * 
	 * @param date
	 */
	private void clearDistanceByDay(Date date,DbHandle conn) throws Exception{
		String sql="delete from ANA_SPECIAL_TOPIC_VEHICLE where work_date=?";
		StatementHandle stmtHandle=conn.prepareCall(sql);
		stmtHandle.setDate(1, new java.sql.Date(date.getTime()));
		stmtHandle.execute();
	}
}
