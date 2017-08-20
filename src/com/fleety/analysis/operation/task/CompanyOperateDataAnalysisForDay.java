package com.fleety.analysis.operation.task;

import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;

import server.db.DbServer;
import server.var.VarManageServer;

import com.fleety.analysis.AnalysisServer;
import com.fleety.analysis.operation.IOperationAnalysis;
import com.fleety.base.InfoContainer;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;

public class CompanyOperateDataAnalysisForDay implements IOperationAnalysis{
	
	private HashMap          companyMapping  = null;
	private int              duration        = 60*1000;
	private SimpleDateFormat sdf             = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private SimpleDateFormat sdf2            = new SimpleDateFormat("yyyy-MM-dd");
	private float            fuelSurcharges  = 0;
	
	@Override
	public boolean startAnalysisOperation(AnalysisServer parentServer,InfoContainer statInfo)
	{
		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
		Date eTime = statInfo.getDate(STAT_END_TIME_DATE);

		fuelSurcharges = Integer.parseInt(VarManageServer.getSingleInstance()
				.getVarStringValue("fuel_surcharges"));
		//
		this.companyMapping = null;
		DbHandle conn = DbServer.getSingleInstance().getConnWithUseTime(0);
		try {
			StatementHandle stmt = conn.createStatement();
			StringBuilder sb = new StringBuilder();
			sb.append("select count(*) as sum from ANA_COMPANY_DAY_STAT ")
					.append(" where work_date = '")
					.append(sdf2.format(sTime)).append("'");
			ResultSet sets = stmt.executeQuery(sb.toString());
			if (sets.next()) {
				int sum = sets.getInt("sum");
				if(sum == 0)
					this.companyMapping = new HashMap();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}

		if (this.companyMapping == null) {
			System.out.println("Not Need Analysis:" + this.toString());
		} else {
			System.out.println("Start Analysis:" + this.toString());
		}

		return this.companyMapping != null;
	}

	@Override
	public void analysisDestOperation(AnalysisServer parentServer, InfoContainer statInfo)
	{
		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
		Date eTime = statInfo.getDate(STAT_END_TIME_DATE);
		DbHandle conn = DbServer.getSingleInstance().getConnWithUseTime(0);
		try{
			StringBuilder sql = new StringBuilder();
			sql.append("select a.*,b.*,c.*,d.company_name,e.car_num from (")
			   .append(" select taxi_company as company_id,")
			   .append("  count(distinct(dispatch_car_no)) as work_car_number,")
			   .append("  count(*) as work_times,")
			   .append("  sum(distance+free_distance) as total_distance,")
			   .append("  sum(decode(sign(distance),1,distance,-1,0,distance)) as work_distance,")
			   .append("  sum(free_distance) as free_distance,")
			   .append("  sum(waiting_hour) as waiting_hour,")
			   .append("  sum(waiting_minute) as waiting_minute,")
			   .append("  sum(waiting_second) as waiting_second,")
			   .append("  sum(sum) as work_income")
			   .append(" from SINGLE_BUSINESS_DATA_BS ")
			   .append(" where taxi_company is not null ")
			   //.append("       and recode_time >= to_date('").append(sdf.format(sTime)).append("','yyyy-mm-dd hh24:mi:ss')")
			   //.append("       and recode_time <= to_date('").append(sdf.format(eTime)).append("','yyyy-mm-dd hh24:mi:ss')")//shijian
			   //modify by mike.li on 2014-07-01 统一采用以上车时间为准 begin
			   .append("       and date_up >= to_date('").append(sdf.format(sTime)).append("','yyyy-mm-dd hh24:mi:ss')")
			   .append("       and date_up < to_date('").append(sdf.format(eTime)).append("','yyyy-mm-dd hh24:mi:ss')")//shijian
			   //modify by mike.li on 2014-07-01 end
			   .append(" group by taxi_company")
			   .append(") a ")
			   .append("left join ")
			   .append(" (")
			   .append("   select TERM_ID,TERM_NAME as company_name from term")
			   .append(" ) d on a.company_id = d.TERM_ID ")
			   .append("left join ")
			   .append(" (")
			   .append("   select car_company,count(*) as telcall_times,")
			   .append("          sum(case when status=3 then 1 else 0 end) as telcall_finish_times")
			   .append("   from taxi_order_list ")
			   .append("   where car_company is not null ")
			   .append("         and created_time >= to_date('").append(sdf.format(sTime)).append("','yyyy-mm-dd hh24:mi:ss')")
			   .append("         and created_time < to_date('").append(sdf.format(eTime)).append("','yyyy-mm-dd hh24:mi:ss')")
			   .append("   group by car_company")
			   .append(" ) b on a.company_id = b.car_company ")
			   .append("left join ")
			   .append(" (")
			   .append("   select term_id as company_id,count(*) as service_evaluate_times,")
			   .append("          sum(case when grade_type = 0 then 1 else 0 end) as satisfisfy_times,")
			   .append("          sum(case when grade_type = 1 then 1 else 0 end) as unsatisfy_times,")
			   .append("          sum(case when grade_type = 2 then 1 else 0 end) as highlySatisfisfy_times,")
			   .append("          sum(case when grade_type = 3 then 1 else 0 end) as unJudge_times")
			   .append("   from (select a.*,b.term_id from grade a,car b where a.car_no is not null and b.term_id is not null and a.car_no = b.car_id ) ")
			   .append("   where create_time >= to_date('").append(sdf.format(sTime)).append("','yyyy-mm-dd hh24:mi:ss')")
			   .append("         and create_time < to_date('").append(sdf.format(eTime)).append("','yyyy-mm-dd hh24:mi:ss')")
			   .append("   group by term_id")
			   .append(" ) c on a.company_id = c.company_id ")
			   .append(" left join (select term_id,count(1) car_num from car group by term_id) e on a.company_id=e.term_id ");
			System.out.println(sql.toString());
			long st1 = System.currentTimeMillis();
			StatementHandle stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql.toString());
			long st2 = System.currentTimeMillis();
			System.out.println("CompanyOperateDataAnalysisForDay查询耗时:"+(st2-st1));
			
			while(rs.next())
			{
				Integer companyId = rs.getInt("company_id");
				CompanyOperateInfo cInfo = new CompanyOperateInfo();
				cInfo.companyId = companyId;
				cInfo.companyName = rs.getString("company_name");
				cInfo.workCarNumber = rs.getInt("work_car_number");
				cInfo.carNumber = rs.getInt("car_num");
				cInfo.workTimes = rs.getInt("work_times");
				cInfo.totalDistance = rs.getFloat("total_distance");
				cInfo.workDistance = rs.getFloat("work_distance");
			    cInfo.freeDistance = rs.getFloat("free_distance");
				cInfo.waitingHour = rs.getInt("waiting_hour");
				cInfo.waitingMinute = rs.getInt("waiting_minute");
				cInfo.waitingSecond = rs.getInt("waiting_second");
				cInfo.workIncome = rs.getFloat("work_income");
				cInfo.telcallTimes = rs.getInt("telcall_times");
				cInfo.telcallFinishTimes = rs.getInt("telcall_finish_times");
				cInfo.serviceEvaluateTimes = rs.getInt("service_evaluate_times");
				cInfo.satisfisfyTimes = rs.getInt("satisfisfy_times");
				cInfo.unsatisfyTimes = rs.getInt("unsatisfy_times");
				cInfo.highlySatisfisfyTimes = rs.getInt("highlySatisfisfy_times");
				cInfo.unJudgeTimes = rs.getInt("unJudge_times");
				companyMapping.put(companyId, cInfo);
			}
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
	}
	
	@Override
	public void endAnalysisOperation(AnalysisServer parentServer,InfoContainer statInfo) {
		if(this.companyMapping == null){ 
			return ;
		}
		
		int recordNum = 0;
		Integer companyId = null;
		CompanyOperateInfo companyOperateInfo;
		Date sDate = statInfo.getDate(STAT_START_TIME_DATE);
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try{
			conn.setAutoCommit(false);
			// inset into databases
			StatementHandle stmt = conn
					.prepareStatement("insert into ANA_COMPANY_DAY_STAT(id,company_id,company_name,car_number,work_car_number,work_times,total_distance,work_distance,free_distance,waiting_hour,waiting_minute,waiting_second,total_income,work_income,fuel_income,telcall_times,telcall_finish_times,service_evaluate_times,satisfisfy_times,unsatisfy_times,highlySatisfisfy_times,unJudge_times,work_date,analysis_time) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
			for(Iterator itr = this.companyMapping.keySet().iterator();itr.hasNext();){
				companyId = (Integer)itr.next();
				companyOperateInfo = (CompanyOperateInfo)this.companyMapping.get(companyId);
				stmt.setInt(1, (int)DbServer.getSingleInstance().getAvaliableId(conn, "ANA_COMPANY_DAY_STAT", "id"));
				stmt.setInt(2, companyId);
				stmt.setString(3, companyOperateInfo.companyName);
				stmt.setInt(4, companyOperateInfo.carNumber);
				stmt.setInt(5, companyOperateInfo.workCarNumber);
				stmt.setInt(6, companyOperateInfo.workTimes);
				stmt.setFloat(7, companyOperateInfo.totalDistance);
				stmt.setFloat(8, companyOperateInfo.workDistance);
				stmt.setFloat(9, companyOperateInfo.freeDistance);
				stmt.setInt(10, companyOperateInfo.waitingHour);
				stmt.setInt(11, companyOperateInfo.waitingMinute);
				stmt.setInt(12, companyOperateInfo.waitingSecond);
				stmt.setFloat(13, companyOperateInfo.workIncome + fuelSurcharges *  companyOperateInfo.workTimes);
				stmt.setFloat(14, companyOperateInfo.workIncome);
				stmt.setFloat(15, fuelSurcharges *  companyOperateInfo.workTimes);
				stmt.setInt(16, companyOperateInfo.telcallTimes);
				stmt.setInt(17, companyOperateInfo.telcallFinishTimes);
				stmt.setInt(18, companyOperateInfo.serviceEvaluateTimes);
				stmt.setInt(19, companyOperateInfo.satisfisfyTimes);
				stmt.setInt(20, companyOperateInfo.unsatisfyTimes);
				stmt.setInt(21, companyOperateInfo.highlySatisfisfyTimes);
				stmt.setInt(22, companyOperateInfo.unJudgeTimes);
				stmt.setString(23, sdf2.format(sDate));
				stmt.setDate(24, new java.sql.Date(new Date().getTime()));
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
	
	private class CompanyOperateInfo
	{
		public int    companyId;
		public String companyName;
		public int    carNumber;
		public int    workCarNumber;
		public int    workTimes;
		public float  totalDistance;
		public float  workDistance;
		public float  freeDistance;
		public int    waitingHour;
		public int    waitingMinute;
		public int    waitingSecond;
		public float  totalIncome;
		public float  workIncome;
		public float  fuelIncome;
		public int    telcallTimes;
		public int    telcallFinishTimes;
		public int    serviceEvaluateTimes;
		public int    satisfisfyTimes;
		public int    unsatisfyTimes;
		public int    highlySatisfisfyTimes;
		public int    unJudgeTimes;
		public Date   analysisTime;
		public String workDate;
		public Date   startTime;
		public Date   endTime;
	}

}
