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

public class VehicleICIsZeroAnalysisForDay implements IOperationAnalysis{
	
	private HashMap          vehicleMapping  = null;
	
	@Override
	public boolean startAnalysisOperation(AnalysisServer parentServer,InfoContainer statInfo)
	{
		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
		this.vehicleMapping = null;
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try{
			StatementHandle stmt = conn.createStatement();
			StringBuilder sb = new StringBuilder();
			sb.append("select count(*) as sum from single_iczero_car_day_stat ")
			  .append(" where 1=1 and stat_time >= to_date('").append(GeneralConst.YYYY_MM_DD_HH_MM_SS.format(sTime)).append("','yyyy-MM-dd hh24:mi:ss')")
			  .append(" and stat_time <= to_date('").append(GeneralConst.YYYY_MM_DD.format(sTime)+" 23:59:59").append("','yyyy-MM-dd hh24:mi:ss')");
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
		
		return this.vehicleMapping != null;
	}

	@Override
	public void analysisDestOperation(AnalysisServer parentServer, InfoContainer statInfo)
	{
		if(this.vehicleMapping == null){
			return ;
		}
		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
		
		DbHandle conn = DbServer.getSingleInstance().getConnWithUseTime(0);
		try{
			StringBuilder sql = new StringBuilder();
			sql.append("select s.id,s.service_no,s.dispatch_car_no dest_no,s.mdt_id,v.company_id,v.company_name from single_business_data_bs s left join v_ana_dest_info v on v.dest_no = s.dispatch_car_no where 1=1 ")
			   .append(" and date_up >= to_date('").append(GeneralConst.YYYY_MM_DD_HH_MM_SS.format(sTime)).append("','yyyy-MM-dd hh24:mi:ss')")
			   .append(" and date_up <= to_date('").append(GeneralConst.YYYY_MM_DD.format(sTime)+" 23:59:59").append("','yyyy-MM-dd hh24:mi:ss')");
			System.out.println(sql.toString());
			StatementHandle stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql.toString());
			VehicleOperateInfo vInfo = null;
			while(rs.next())
			{
				String dest_no = rs.getString("dest_no");
				if(vehicleMapping.containsKey(dest_no)){
					vInfo = (VehicleOperateInfo)vehicleMapping.get(dest_no);
				}else{
					vInfo = new VehicleOperateInfo();
				}
				vInfo.dest_no = dest_no;
				vInfo.companyId = rs.getInt("company_id");
				vInfo.companyName = rs.getString("company_name");
				vInfo.stat_time = new Timestamp(sTime.getTime());
				String serviceNo = rs.getString("service_no");
				if(serviceNo.equals("00000000")){
					vInfo.num++;
				}
				vehicleMapping.put(dest_no, vInfo);
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
		String destNo = "";
		VehicleOperateInfo vInfo;
		DbHandle conn = DbServer.getSingleInstance().getConnWithUseTime(0);
		try{
			conn.setAutoCommit(false);
			StatementHandle stmt = conn.prepareStatement("insert into single_iczero_car_day_stat(id,dest_no,company_id,company_name,stat_time,num) values(?,?,?,?,?,?)");
			for(Iterator itr = this.vehicleMapping.keySet().iterator();itr.hasNext();){
				destNo = (String)itr.next();
				vInfo = (VehicleOperateInfo)this.vehicleMapping.get(destNo);
				if(vInfo.num<=0){
					continue;
				}
				stmt.setInt(1, (int)DbServer.getSingleInstance().getAvaliableId(conn, "single_iczero_car_day_stat", "id"));
				stmt.setString(2, vInfo.dest_no);
				stmt.setInt(3, vInfo.companyId);
				stmt.setString(4, vInfo.companyName);
				stmt.setTimestamp(5, vInfo.stat_time);
				stmt.setInt(6, vInfo.num);
				stmt.addBatch();
				if(recordNum%200==0){
					stmt.executeBatch();
				}
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
		System.out.println("Finish VehicleICIsZeroAnalysisForDay Analysis:"+this.toString()+" recordNum="+recordNum);
	}
	
	private class VehicleOperateInfo
	{
		public String dest_no;
		public int    companyId;
		public String companyName;
		public Timestamp stat_time;
		public int    num = 0;
	}
			
}
