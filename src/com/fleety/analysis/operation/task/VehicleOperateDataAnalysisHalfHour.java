package com.fleety.analysis.operation.task;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;

import server.db.DbServer;
import server.var.VarManageServer;

import com.fleety.analysis.AnalysisServer;
import com.fleety.analysis.operation.IOperationAnalysis;
import com.fleety.base.GeneralConst;
import com.fleety.base.InfoContainer;
import com.fleety.base.event.Event;
import com.fleety.server.event.GlobalEventCenter;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;

public class VehicleOperateDataAnalysisHalfHour implements IOperationAnalysis{
	
	private HashMap          vehicleMapping  = null;
	private SimpleDateFormat sdf             = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private SimpleDateFormat sdf2            = new SimpleDateFormat("yyyy-MM-dd HH:mm");
	private float fuelSurcharges = 0;
	
	@Override
	public boolean startAnalysisOperation(AnalysisServer parentServer,InfoContainer statInfo)
	{
		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
		Date eTime = statInfo.getDate(STAT_END_TIME_DATE);
		fuelSurcharges = Integer.parseInt(VarManageServer.getSingleInstance()
				.getVarStringValue("fuel_surcharges"));
		
		this.vehicleMapping = null;
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try{
			StatementHandle stmt = conn.createStatement();
			StringBuilder sb = new StringBuilder();
			sb.append("select count(*) as sum from ana_single_car_halfhour_stat ")
			  .append(" where work_date = '").append(sdf2.format(sTime)).append("'");
			ResultSet sets = stmt.executeQuery(sb.toString());
			if(sets.next()){
				int sum = sets.getInt("sum");
				if(sum == 0)
					this.vehicleMapping = new HashMap();
			}
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
		
		if(this.vehicleMapping == null){
			System.out.println("Not Need Analysis:"+this.toString());
		}else{
			System.out.println("Start Analysis:"+this.toString());
		}
		
//		return true;
		return this.vehicleMapping != null;
	}

	@Override
	public void analysisDestOperation(AnalysisServer parentServer, InfoContainer statInfo)
	{
		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
		Date eTime = statInfo.getDate(STAT_END_TIME_DATE);
		System.out.println(GeneralConst.YYYY_MM_DD_HH_MM_SS.format(sTime));
		System.out.println(GeneralConst.YYYY_MM_DD_HH_MM_SS.format(eTime));
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try{
			StringBuilder sql = new StringBuilder();
			sql.append(" select a.plate_no,a.recode_time_t workDate,a.company_id,d.company_name,a.waiting_hour,a.waiting_minute,a.waiting_second,a.totalIncome,a.work_times from ( ");
			sql.append(" select dispatch_car_no plate_no,recode_time_t,min(taxi_company) company_id,sum(waiting_hour) waiting_hour,sum(waiting_minute) waiting_minute,sum(waiting_second) waiting_second,(sum(sum)+"+fuelSurcharges+"*count(1)) totalIncome,count(1) work_times from (");
					sql.append(" select id,dispatch_car_no,taxi_company,waiting_hour,waiting_minute,waiting_second,sum,recode_time,to_char(recode_time,'yyyy-MM-dd hh24:')||(case when to_number(to_char(recode_time,'mi'))>=30 then '30' else '00' end) recode_time_t");
					sql.append(" from single_business_data_bs"); 
					sql.append(" where recode_time>=to_date('"+GeneralConst.YYYY_MM_DD_HH_MM_SS.format(sTime)+"','yyyy-MM-dd hh24:mi:ss')");
					sql.append(" and recode_time<=to_date('"+GeneralConst.YYYY_MM_DD_HH_MM_SS.format(eTime)+"','yyyy-MM-dd hh24:mi:ss'))");
					sql.append(" group by dispatch_car_no,recode_time_t");
					sql.append(" order by recode_time_t) a");
			sql.append(" left join  (select TERM_ID,TERM_NAME as company_name from term ) d on a.company_id = d.TERM_ID ");
			System.out.println(sql.toString());
			StatementHandle stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql.toString());
			while(rs.next())
			{
				String plateNo = rs.getString("plate_no");
				VehicleOperateInfo vInfo = new VehicleOperateInfo();
				vInfo.plateNo = plateNo;
				vInfo.companyId = rs.getInt("company_id");
				vInfo.companyName = rs.getString("company_name");
				vInfo.waitingHour = rs.getInt("waiting_hour");
				vInfo.waitingMinute = rs.getInt("waiting_minute");
				vInfo.waitingSecond = rs.getInt("waiting_second");
				vInfo.totalIncome = rs.getFloat("totalIncome");
				vInfo.workDate = rs.getString("workDate");
				vInfo.work_times = rs.getInt("work_times");
				vehicleMapping.put(plateNo+rs.getString("workDate"), vInfo);
			}
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
	}
	
	@Override
	public void endAnalysisOperation(AnalysisServer parentServer,InfoContainer statInfo) {
		if(this.vehicleMapping == null){ 
			return ;
		}
		int recordNum = 0;
		String plateNo = "";
		VehicleOperateInfo vehicleOperateInfo;
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try{
			conn.setAutoCommit(false);
			StatementHandle stmt = conn.prepareStatement("insert into ana_single_car_halfhour_stat(id,plate_no,company_id,company_name,waiting_hour,waiting_minute,waiting_second,total_income,work_date,analysis_time,work_times) values(?,?,?,?,?,?,?,?,?,?,?)");
			for(Iterator itr = this.vehicleMapping.keySet().iterator();itr.hasNext();){
				plateNo = (String)itr.next();
				vehicleOperateInfo = (VehicleOperateInfo)this.vehicleMapping.get(plateNo);
				stmt.setInt(1, (int)DbServer.getSingleInstance().getAvaliableId(conn, "ana_single_car_halfhour_stat", "id"));
				stmt.setString(2, vehicleOperateInfo.plateNo);
				stmt.setInt(3, vehicleOperateInfo.companyId);
				stmt.setString(4, vehicleOperateInfo.companyName);
				stmt.setInt(5, vehicleOperateInfo.waitingHour);
				stmt.setInt(6, vehicleOperateInfo.waitingMinute);
				stmt.setInt(7, vehicleOperateInfo.waitingSecond);
				stmt.setFloat(8, vehicleOperateInfo.totalIncome);
				stmt.setString(9, vehicleOperateInfo.workDate);
				stmt.setDate(10, new java.sql.Date(new Date().getTime()));
				stmt.setInt(11,vehicleOperateInfo.work_times);
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
		System.out.println("Finish vehicle operate data Analysis:"+this.toString()+" recordNum="+recordNum);
	}
	
	private class VehicleOperateInfo
	{
		public String plateNo;
		public int    companyId;
		public String companyName;
		public int    waitingHour;
		public int    waitingMinute;
		public int    waitingSecond;
		public float  totalIncome;
		public Date   analysisTime;
		public String workDate;
		public int work_times;
	}
			
}
