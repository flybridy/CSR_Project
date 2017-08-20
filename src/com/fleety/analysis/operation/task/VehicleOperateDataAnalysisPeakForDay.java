package com.fleety.analysis.operation.task;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;

import server.db.DbServer;
import server.var.VarManageServer;

import com.fleety.analysis.AnalysisServer;
import com.fleety.analysis.operation.IOperationAnalysis;
import com.fleety.base.GeneralConst;
import com.fleety.base.InfoContainer;
import com.fleety.util.pool.db.DbConnPool;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;

public class VehicleOperateDataAnalysisPeakForDay implements IOperationAnalysis{
	
	private HashMap vehicleMapping  = null;
	private HashMap peakParaMapping  = null;
	private float fuelSurcharges  = 0.0F;
	
	@Override
	public boolean startAnalysisOperation(AnalysisServer parentServer,InfoContainer statInfo)
	{
		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
		Date eTime = statInfo.getDate(STAT_END_TIME_DATE);
		
		fuelSurcharges = Integer.parseInt(VarManageServer.getSingleInstance().getVarStringValue("fuel_surcharges"));
		this.vehicleMapping = null;
		this.peakParaMapping = null;
		this.loadPeakPara(parentServer, statInfo);
		if(peakParaMapping==null){
			return false;
		}
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try{
			StatementHandle stmt = conn.createStatement();
			StringBuilder sb = new StringBuilder();
			sb.append("select * from ana_single_car_day_peak_stat ")
			  .append(" where work_date >= to_date('").append(GeneralConst.YYYY_MM_DD_HH_MM_SS.format(sTime)).append("','yyyy-MM-dd hh24:mi:ss')")
			  .append(" and   work_date <= to_date('").append(GeneralConst.YYYY_MM_DD_HH_MM_SS.format(eTime)).append("','yyyy-MM-dd hh24:mi:ss')");
			ResultSet sets = stmt.executeQuery(sb.toString());
			if(!sets.next()){
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
		if(peakParaMapping == null){
			return;
		}
		try {
			for (Iterator iterator = peakParaMapping.values().iterator(); iterator.hasNext();) {
				PeakParaManage peakPara = (PeakParaManage) iterator.next();
				analysisPeakType(peakPara);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	private void analysisPeakType(PeakParaManage peakPara) throws Exception{
		Date sTime = peakPara.startTime;
		Date eTime = peakPara.endTime;
		String peakType = peakPara.peak_type;
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try{
			StringBuilder sql = new StringBuilder();
			sql.append("select a.*,d.company_name from (")
			   .append(" select dispatch_car_no as plate_no,taxi_company as company_id,")
			   .append("  count(*) as work_times,")
			   .append("  sum(distance+free_distance) as total_distance,")
			   .append("  sum(decode(sign(distance),1,distance,-1,0,distance)) as work_distance,")
			   .append("  sum(free_distance) as free_distance,")
			   .append("  sum(waiting_hour) as waiting_hour,")
			   .append("  sum(waiting_minute) as waiting_minute,")
			   .append("  sum(waiting_second) as waiting_second,")
			   .append("  sum(sum) as work_income")
			   .append(" from single_business_data_bs ")
			   .append(" where dispatch_car_no is not null ")
			   .append("       and date_up >= to_date('").append(GeneralConst.YYYY_MM_DD_HH_MM_SS.format(sTime)).append("','yyyy-mm-dd hh24:mi:ss')")
			   .append("       and date_up <= to_date('").append(GeneralConst.YYYY_MM_DD_HH_MM_SS.format(eTime)).append("','yyyy-mm-dd hh24:mi:ss')")
			   .append(" group by dispatch_car_no,taxi_company")
			   .append(") a ")
			   .append("left join ")
			   .append(" (")
			   .append("   select TERM_ID,TERM_NAME as company_name from term")
			   .append(" ) d on a.company_id = d.TERM_ID ");
			System.out.println("@@@VehicleOperateDataAnalysisPeakForDay::::::"+sql.toString());
			VehicleOperatePeakInfo vInfo = null;
			DbConnPool.StatementHandle stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql.toString());
			int count1 = 0;
			while(rs.next())
			{
				String plateNo = rs.getString("plate_no");
				vInfo = new VehicleOperatePeakInfo();
				vInfo.peakType = peakType;
				vInfo.plateNo = plateNo;
				vInfo.companyId = rs.getInt("company_id");
				vInfo.companyName = rs.getString("company_name");
				vInfo.workTimes = rs.getInt("work_times");
				vInfo.totalDistance = rs.getFloat("total_distance");
				vInfo.workDistance = rs.getFloat("work_distance");
			    vInfo.freeDistance = rs.getFloat("free_distance");
				vInfo.waitingHour = rs.getInt("waiting_hour");
				vInfo.waitingMinute = rs.getInt("waiting_minute");
				vInfo.waitingSecond = rs.getInt("waiting_second");
				vInfo.workIncome = rs.getFloat("work_income");
				vInfo.fuelIncome = fuelSurcharges * vInfo.workTimes;
				vInfo.totalIncome = vInfo.workIncome + vInfo.fuelIncome;
				vInfo.workDate = GeneralConst.YYYY_MM_DD.parse(GeneralConst.YYYY_MM_DD.format(sTime)+" 00:00:00");
				vehicleMapping.put(plateNo+"_"+peakType, vInfo);
				count1 ++;
			}
			System.out.println("@@@analysisPeakType::::" + count1);
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
	}
	private void loadPeakPara(AnalysisServer parentServer,InfoContainer statInfo){
		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
		SimpleDateFormat sdf = new SimpleDateFormat("HH:mm");
		peakParaMapping = new HashMap();
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			PeakParaManage peakPara = null;
			StatementHandle stmt = conn.createStatement();
			ResultSet sets = stmt.executeQuery("select * from peak_para_manage");
			while (sets.next()) {
				peakPara = new PeakParaManage();
				peakPara.peak_type = sets.getString("peak_type");
				String startTime = sets.getString("start_time");
				System.out.println("startTime::" +sets.getString("start_time").trim().substring(11, 19));
				System.out.println("endTime::" +sets.getString("end_time").trim().substring(11, 19));
				peakPara.startTime = GeneralConst.YYYY_MM_DD_HH_MM_SS.parse(GeneralConst.YYYY_MM_DD.format(sTime)+" "+sets.getString("start_time").trim().substring(11, 19));
				peakPara.endTime = GeneralConst.YYYY_MM_DD_HH_MM_SS.parse(GeneralConst.YYYY_MM_DD.format(sTime)+" "+sets.getString("end_time").trim().substring(11, 19));
				peakParaMapping.put(peakPara.peak_type, peakPara);
			}
		} catch (Exception e) {
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
		String temp = "";
		VehicleOperatePeakInfo vehicleOperatePeakInfo;
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try{
			conn.setAutoCommit(false);
			StatementHandle stmt = conn.prepareStatement("insert into ana_single_car_day_peak_stat(id,plate_no,company_id,company_name,work_times,total_distance,work_distance,free_distance,waiting_hour,waiting_minute,waiting_second,total_income,work_income,fuel_income,work_date,analysis_time,peak_type) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
			for(Iterator itr = this.vehicleMapping.keySet().iterator();itr.hasNext();){
				temp = (String)itr.next();
				vehicleOperatePeakInfo = (VehicleOperatePeakInfo)this.vehicleMapping.get(temp);
				stmt.setInt(1, (int)DbServer.getSingleInstance().getAvaliableId(conn, "ana_single_car_day_peak_stat", "id"));
				stmt.setString(2, vehicleOperatePeakInfo.plateNo);
				stmt.setInt(3, vehicleOperatePeakInfo.companyId);
				stmt.setString(4, vehicleOperatePeakInfo.companyName);
				stmt.setInt(5, vehicleOperatePeakInfo.workTimes);
				stmt.setFloat(6, vehicleOperatePeakInfo.totalDistance);
				stmt.setFloat(7, vehicleOperatePeakInfo.workDistance);
				stmt.setFloat(8, vehicleOperatePeakInfo.freeDistance);
				stmt.setInt(9, vehicleOperatePeakInfo.waitingHour);
				stmt.setInt(10, vehicleOperatePeakInfo.waitingMinute);
				stmt.setInt(11, vehicleOperatePeakInfo.waitingSecond);
				stmt.setFloat(12, vehicleOperatePeakInfo.totalIncome);
				stmt.setFloat(13, vehicleOperatePeakInfo.workIncome);
				stmt.setFloat(14, vehicleOperatePeakInfo.fuelIncome);
				stmt.setTimestamp(15, new Timestamp(vehicleOperatePeakInfo.workDate.getTime()));
				stmt.setTimestamp(16, new Timestamp(new Date().getTime()));
				stmt.setString(17, vehicleOperatePeakInfo.peakType);
				stmt.addBatch();
				recordNum ++;
				if(recordNum%200==0){
					stmt.executeBatch();
				}
			}
			System.out.println("@@@endAnalysisOperation:::"+recordNum);
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
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
	}
	
	private class VehicleOperatePeakInfo
	{
		public String plateNo;
		public int    companyId;
		public String companyName;
		public String peakType;
		public int    workTimes;//营运次数
		public float  totalDistance;//行驶里程
		public float  workDistance;//营运里程
		public float  freeDistance;//空驶里程
		public int    waitingHour;//等候时
		public int    waitingMinute;//等候分
		public int    waitingSecond;//等候秒
		public float  totalIncome;//总收入
		public float  workIncome;//营运收入
		public float  fuelIncome;//燃油附加收入
		public Date   workDate;//营运时间
		public Date   analysisTime;//分析时间
	}
	private	class PeakParaManage{
		public String peak_type;
		public Date startTime;
		public Date endTime;
	}
}
