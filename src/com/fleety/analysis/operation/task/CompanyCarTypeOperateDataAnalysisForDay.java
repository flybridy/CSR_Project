package com.fleety.analysis.operation.task;

import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import server.db.DbServer;

import com.fleety.analysis.AnalysisServer;
import com.fleety.analysis.operation.IOperationAnalysis;
import com.fleety.base.InfoContainer;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;

public class CompanyCarTypeOperateDataAnalysisForDay implements IOperationAnalysis{
	
	private List companyCtypeMap = null;
	private int duration = 60*1000;
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");

	@Override
	public boolean startAnalysisOperation(AnalysisServer parentServer,
			InfoContainer statInfo) {
		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
		Date eTime = statInfo.getDate(STAT_END_TIME_DATE);
		//
		this.companyCtypeMap = null;
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			StatementHandle stmt = conn.createStatement();
			StringBuilder sb = new StringBuilder();
			sb.append("select count(*) as sum from ANA_COMPANY_CARTYPE_DAY_STAT ")
					.append(" where date_time = to_date('")
					.append(sdf2.format(sTime)).append("','yyyy-mm-dd')");
			ResultSet sets = stmt.executeQuery(sb.toString());
			if (sets.next()) {
				int sum = sets.getInt("sum");
				if(sum == 0)
					this.companyCtypeMap = new ArrayList();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}

		if (this.companyCtypeMap == null) {
			System.out.println("ANA_COMPANY_CARTYPE_DAY_STAT Not Need Analysis:" + this.toString());
		} else {
			System.out.println("ANA_COMPANY_CARTYPE_DAY_STAT Start Analysis:" + this.toString());
		}

		return this.companyCtypeMap != null;		
	}

	@Override
	public void analysisDestOperation(AnalysisServer parentServer,
			InfoContainer statInfo) {
		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
		Date eTime = statInfo.getDate(STAT_END_TIME_DATE);
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try{
			StringBuilder sql = new StringBuilder();
			sql.append("select term_id,type_id,date_time,sum(total_distance) total_distance,sum(work_distance) as work_distance,sum(work_income) work_income,sum(work_car_number) work_car_number " +
					"from(" +
					"select t.term_id,c.type_id,to_date(to_char(s.date_up,'yyyy-mm-dd'),'yyyy-mm-dd hh24:mi:ss') date_time,sum(distance+free_distance) total_distance,sum(decode(sign(distance),1,distance,-1,0,distance)) as work_distance,sum(sum) as work_income,count(distinct(dispatch_car_no)) as work_car_number " +
					"from term t " +
					"left join car c on t.term_id=c.term_id " +
					"left join SINGLE_BUSINESS_DATA_BS s on c.car_id=s.car_no " +
					"where 1=1 ")
			.append(" and to_date(to_char(s.date_up,'yyyy-mm-dd'),'yyyy-mm-dd hh24:mi:ss') >= to_date('").append(sdf.format(sTime)).append("','yyyy-mm-dd hh24:mi:ss')")
			.append(" and to_date(to_char(s.date_up,'yyyy-mm-dd'),'yyyy-mm-dd hh24:mi:ss') <= to_date('").append(sdf.format(eTime)).append("','yyyy-mm-dd hh24:mi:ss')")
			.append(" group by t.term_id,c.type_id,s.date_up")
			.append(")t where 1=1 group by term_id,type_id,date_time");
			System.out.println(sql.toString());
			StatementHandle stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql.toString());
			while(rs.next())
			{
				CompanyCTypeOperateInfo cInfo = new CompanyCTypeOperateInfo();
				cInfo.companyId=rs.getInt("term_id");
				cInfo.typeId=rs.getInt("type_id");
				cInfo.workDate=rs.getDate("date_time");
				cInfo.totalDistance=rs.getFloat("total_distance");
				cInfo.workDistance=rs.getFloat("work_distance");
				cInfo.workIncome=rs.getFloat("work_income");
				cInfo.workCarNumber=rs.getInt("work_car_number");
				companyCtypeMap.add(cInfo);
			}
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
	}

	@Override
	public void endAnalysisOperation(AnalysisServer parentServer,
			InfoContainer statInfo) {
		if(this.companyCtypeMap == null){ 
			return ;
		}
		
		int recordNum = 0;
		CompanyCTypeOperateInfo companyOperateInfo;
		Date sDate = statInfo.getDate(STAT_START_TIME_DATE);
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try{
			conn.setAutoCommit(false);
			StatementHandle stmt = conn.prepareStatement("insert into ANA_COMPANY_CARTYPE_DAY_STAT(id,term_id,type_id,date_time,total_distance,work_distance,work_income,work_car_number,analysis_time)values(?,?,?,?,?,?,?,?,?)");
			Iterator it= companyCtypeMap.iterator();
			while(it.hasNext()){
				CompanyCTypeOperateInfo c=(CompanyCTypeOperateInfo)it.next();
				stmt.setInt(1, (int)DbServer.getSingleInstance().getAvaliableId(conn, "ANA_COMPANY_DAY_STAT", "id"));
				stmt.setInt(2, c.companyId);
				stmt.setInt(3, c.typeId);
				stmt.setDate(4, (java.sql.Date) c.workDate);
				stmt.setFloat(5, c.totalDistance);
				stmt.setFloat(6, c.workDistance);
				stmt.setFloat(7, c.workIncome);
				stmt.setInt(8, c.workCarNumber);
				stmt.setDate(9, new java.sql.Date(new Date().getTime()));
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
	private class CompanyCTypeOperateInfo
	{
		public int    companyId;
		public String companyName;
		public int	  typeId;
		public String carType;
		public int    carNumber;
		public int    workCarNumber;
		public float  totalDistance;
		public float  workDistance;
		public float  freeDistance;
		public float  workIncome;
		public Date   workDate;
		public Date   analysisTime;
	}
}
