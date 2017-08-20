package com.fleety.analysis.operation.task;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import server.db.DbServer;
import server.var.VarManageServer;
import com.fleety.analysis.AnalysisServer;
import com.fleety.analysis.operation.IOperationAnalysis;
import com.fleety.base.GeneralConst;
import com.fleety.base.InfoContainer;
import com.fleety.base.StrFilter;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.fleety.util.pool.db.redis.RedisConnPoolServer;

public class DriverOperateDataAnalysisForDay implements IOperationAnalysis{
	
	private HashMap          driverMapping  = null;
	private int              duration        = 60*1000;
	private SimpleDateFormat sdf             = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private SimpleDateFormat sdf2            = new SimpleDateFormat("yyyy-MM-dd");
	private float            fuelSurcharges  = 0;
	
	@Override
	public boolean startAnalysisOperation(AnalysisServer parentServer,InfoContainer statInfo)
	{
		// 读取燃油附加费信息
		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
		Date eTime = statInfo.getDate(STAT_END_TIME_DATE);

		fuelSurcharges = Integer.parseInt(VarManageServer.getSingleInstance()
				.getVarStringValue("fuel_surcharges"));
		//
		this.driverMapping = null;
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			StatementHandle stmt = conn.createStatement();
			StringBuilder sb = new StringBuilder();
			sb.append("select count(*) as sum from ANA_SINGLE_DRIVER_DAY_STAT ")
					.append(" where work_date = '")
					.append(sdf2.format(sTime)).append("'");
			ResultSet sets = stmt.executeQuery(sb.toString());
			if (sets.next()) {
				int sum = sets.getInt("sum");
				if(sum == 0){
					this.driverMapping = new HashMap();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}

		if (this.driverMapping == null) {
			System.out.println("Not Need Analysis:" + this.toString());
		} else {
			System.out.println("Start Analysis:" + this.toString());
		}

		return this.driverMapping != null;
	}

	@Override
	public void analysisDestOperation(AnalysisServer parentServer, InfoContainer statInfo)
	{
		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
		Date eTime = statInfo.getDate(STAT_END_TIME_DATE);
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try{
			StringBuilder sql = new StringBuilder();
			sql.append("select * from (")
			   .append(" select SERVICE_NO as driver_id,taxi_company as company_id,")
			   .append("  count(*) as work_times,")
			   .append("  sum(distance+free_distance) as total_distance,")
			   .append("  sum(decode(sign(distance),1,distance,-1,0,distance)) as work_distance,")
			   .append("  sum(free_distance) as free_distance,")
			   .append("  sum(waiting_hour) as waiting_hour,")
			   .append("  sum(waiting_minute) as waiting_minute,")
			   .append("  sum(waiting_second) as waiting_second,")
			   .append(" sum(abs(date_down -date_up) * 24 * 60 * 60) work_time_seconds,")
			   .append("  sum(sum) as work_income")
			   .append(" from SINGLE_BUSINESS_DATA_BS ")
			   .append(" where SERVICE_NO is not null ")
			   .append("       and recode_time >= to_date('").append(sdf.format(sTime)).append("','yyyy-mm-dd hh24:mi:ss')")
			   .append("       and recode_time <= to_date('").append(sdf.format(eTime)).append("','yyyy-mm-dd hh24:mi:ss')")//shijian
			   .append(" group by SERVICE_NO,taxi_company")
			   .append(") a ")
			   .append("left join ")
			   .append(" (")
			   .append("   select TERM_ID,TERM_NAME as company_name from term")
			   .append(" ) d on a.company_id = d.TERM_ID ")
			   .append("left join ")
			   .append(" (")
			   .append("   select SERVICE_NO,DRIVER_NAME,GRADE as driver_grade from driver_info")
			   .append(" ) d on a.driver_id = d.SERVICE_NO ")
//			   .append("left join ")
//			   .append(" (")
//			   .append(" select driver_id,online_minutes from ANA_DRIVER_STATUS_DAY_STAT")
//			   .append(" where driver_id is not null and work_date = '").append(sdf2.format(sTime)).append("'")
//			   .append(" ) e on a.driver_id = e.driver_id ")
			   .append("left join ")
			   .append(" (")
			   .append("   select driver_id,count(*) as telcall_times,")
			   .append("          sum(case when status=3 then 1 else 0 end) as telcall_finish_times")
			   .append("   from taxi_order_list ")
			   .append("   where driver_id is not null ")
			   .append("         and created_time >= to_date('").append(sdf.format(sTime)).append("','yyyy-mm-dd hh24:mi:ss')")
			   .append("         and created_time <= to_date('").append(sdf.format(eTime)).append("','yyyy-mm-dd hh24:mi:ss')")
			   .append("   group by driver_id")
			   .append(" ) b on a.driver_id = b.driver_id ")
			   .append("left join ")
			   .append(" (")
			   .append("   select driver_id,count(*) as service_evaluate_times,")
			   .append("          sum(case when grade_type = 0 then 1 else 0 end) as satisfisfy_times,")
			   .append("          sum(case when grade_type = 1 then 1 else 0 end) as unsatisfy_times,")
			   .append("          sum(case when grade_type = 2 then 1 else 0 end) as highlySatisfisfy_times,")
			   .append("          sum(case when grade_type = 3 then 1 else 0 end) as unJudge_times")
			   .append("   from grade where driver_id is not null ")
			   .append("                    and create_time >= to_date('").append(sdf.format(sTime)).append("','yyyy-mm-dd hh24:mi:ss')")
			   .append("                    and create_time <= to_date('").append(sdf.format(eTime)).append("','yyyy-mm-dd hh24:mi:ss')")
			   .append("   group by driver_id")
			   .append(" ) c on a.driver_id = c.driver_id ")
			   .append("left join ")
			   .append(" v_ana_driver_info f on a.driver_id = f.SERVICE_NO");

			StatementHandle stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql.toString());
			while(rs.next())
			{
				String driverId = rs.getString("driver_id");
				DriverOperateInfo dInfo = new DriverOperateInfo();
				dInfo.driverId = driverId;
				dInfo.driverName = rs.getString("driver_name");
				dInfo.driverGrade = rs.getInt("driver_grade");
//				dInfo.durationTime = rs.getFloat("online_minutes");
				dInfo.companyId = rs.getInt("company_id");
				dInfo.companyName = rs.getString("company_name");
				dInfo.workTimes = rs.getInt("work_times");
				dInfo.totalDistance = rs.getFloat("total_distance");
				dInfo.workDistance = rs.getFloat("work_distance");
			    dInfo.freeDistance = rs.getFloat("free_distance");
				dInfo.waitingHour = rs.getInt("waiting_hour");
				dInfo.waitingMinute = rs.getInt("waiting_minute");
				dInfo.waitingSecond = rs.getInt("waiting_second");
				dInfo.workIncome = rs.getFloat("work_income");
				dInfo.telcallTimes = rs.getInt("telcall_times");
				dInfo.telcallFinishTimes = rs.getInt("telcall_finish_times");
				dInfo.serviceEvaluateTimes = rs.getInt("service_evaluate_times");
				dInfo.satisfisfyTimes = rs.getInt("satisfisfy_times");
				dInfo.unsatisfyTimes = rs.getInt("unsatisfy_times");
				dInfo.highlySatisfisfyTimes = rs.getInt("highlySatisfisfy_times");
				dInfo.unJudgeTimes = rs.getInt("unJudge_times");
				dInfo.plateNo = rs.getString("CAR_ID");
				dInfo.businessTime = rs.getInt("work_time_seconds");
				driverMapping.put(driverId, dInfo);
			}
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
	}
	
	@Override
	public void endAnalysisOperation(AnalysisServer parentServer,InfoContainer statInfo) {
		if(this.driverMapping == null){ 
			return ;
		}
		
		int recordNum = 0;
		String driverNo = "";
		DriverOperateInfo driverOperateInfo;
		Date sDate = statInfo.getDate(STAT_START_TIME_DATE);
		DbHandle conn = DbServer.getSingleInstance().getConnWithUseTime(120000);
		try{
			conn.setAutoCommit(false);
			// inset into databases
			StatementHandle stmt = conn
					.prepareStatement("insert into ANA_SINGLE_DRIVER_DAY_STAT(id,driver_id,driver_name,company_id,company_name,driver_grade,duration_time,work_times,total_distance,work_distance,free_distance,waiting_hour,waiting_minute,waiting_second,total_income,work_income,fuel_income,telcall_times,telcall_finish_times,service_evaluate_times,satisfisfy_times,unsatisfy_times,highlySatisfisfy_times,unJudge_times,work_date,analysis_time,plate_no,business_time) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
			for(Iterator itr = this.driverMapping.keySet().iterator();itr.hasNext();){
				driverNo = (String)itr.next();
				driverOperateInfo = (DriverOperateInfo)this.driverMapping.get(driverNo);
				stmt.setInt(1, (int)DbServer.getSingleInstance().getAvaliableId(conn, "ANA_SINGLE_DRIVER_DAY_STAT", "id"));
				stmt.setString(2, driverOperateInfo.driverId);
				stmt.setString(3, driverOperateInfo.driverName);
				stmt.setInt(4, driverOperateInfo.companyId);
				stmt.setString(5, driverOperateInfo.companyName);
				stmt.setInt(6, driverOperateInfo.driverGrade);
				stmt.setFloat(7, driverOperateInfo.durationTime);
				stmt.setInt(8, driverOperateInfo.workTimes);
				stmt.setFloat(9, driverOperateInfo.totalDistance);
				stmt.setFloat(10, driverOperateInfo.workDistance);
				stmt.setFloat(11, driverOperateInfo.freeDistance);
				stmt.setInt(12, driverOperateInfo.waitingHour);
				stmt.setInt(13, driverOperateInfo.waitingMinute);
				stmt.setInt(14, driverOperateInfo.waitingSecond);
				stmt.setFloat(15, driverOperateInfo.workIncome + fuelSurcharges * driverOperateInfo.workTimes);
				stmt.setFloat(16, driverOperateInfo.workIncome);
				stmt.setFloat(17, fuelSurcharges * driverOperateInfo.workTimes);
				stmt.setInt(18, driverOperateInfo.telcallTimes);
				stmt.setInt(19, driverOperateInfo.telcallFinishTimes);
				stmt.setInt(20, driverOperateInfo.serviceEvaluateTimes);
				stmt.setInt(21, driverOperateInfo.satisfisfyTimes);
				stmt.setInt(22, driverOperateInfo.unsatisfyTimes);
				stmt.setInt(23, driverOperateInfo.highlySatisfisfyTimes);
				stmt.setInt(24, driverOperateInfo.unJudgeTimes);
				stmt.setString(25, sdf2.format(sDate));
				stmt.setDate(26, new java.sql.Date(new Date().getTime()));
				stmt.setString(27, StrFilter.getNotNullIntByZero(driverOperateInfo.plateNo));
				stmt.setInt(28, driverOperateInfo.businessTime);
				recordNum ++;
				stmt.addBatch();
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
//		this.statDriverLogin(statInfo);
		System.out.println("Finish driver operate data Analysis:"+this.toString()+" recordNum="+recordNum);
	}
	
	private void statDriverLogin(InfoContainer statInfo){
		
		
		DbHandle conn=null;
		try{
			Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
			conn=DbServer.getSingleInstance().getConnWithUseTime(0);
			conn.setAutoCommit(false);
			StatementHandle stmt=conn.createStatement();
			String sql="delete from ana_driver_unlogout_stat where work_date >= to_date('"+GeneralConst.YYYY_MM_DD.format(sTime)+"','yyyy-MM-dd') and work_date <= to_date('"+GeneralConst.YYYY_MM_DD.format(sTime)+" 23:59:59','yyyy_mm-dd hh24:mi:ss')";
			stmt.execute(sql);
			System.out.println(sql);
			
			sql="insert into ana_driver_unlogout_stat "
					+ " (service_no,driver_name,dispatch_car_no,term_name,term_id,work_date,create_time,LAST_LOGIN_TIME) "
					+ " select service_no,driver_name,dispatch_car_no,term_name,term_id,"
					+ " to_date('"+GeneralConst.YYYY_MM_DD.format(sTime)+"','yyyy-MM-dd'),sysdate,to_date('"+GeneralConst.YYYY_MM_DD.format(sTime)+"','yyyy-MM-dd') "
							+ " from "
					+ "(select substr(aa.service_no,3,length(aa.service_no)) service_no,"
					+ " aa.dispatch_car_no,bb.driver_name,cc.term_name,cc.term_id "
					+ " from (select b.* from (select * from (select count(service_no) cout,"
					+ " dispatch_car_no from (select distinct service_no,dispatch_car_no "
					+ " from single_business_data_bs where date_up >= to_date('"+GeneralConst.YYYY_MM_DD.format(sTime)+"','yyyy-MM-dd') and date_up <= to_date('"+GeneralConst.YYYY_MM_DD.format(sTime)+" 23:59:59','yyyy_mm-dd hh24:mi:ss')) "
					+ " group by dispatch_car_no ) where cout=1) a "
					+ " inner join (select distinct service_no,dispatch_car_no "
					+ " from single_business_data_bs where date_up >= to_date('"+GeneralConst.YYYY_MM_DD.format(sTime)+"','yyyy-MM-dd') and date_up <= to_date('"+GeneralConst.YYYY_MM_DD.format(sTime)+" 23:59:59','yyyy_mm-dd hh24:mi:ss')) b "
					+ " on a.dispatch_car_no=b.dispatch_car_no) aa "
					+ " inner join driver_info bb "
					+ " on substr(aa.service_no,3,length(aa.service_no))=bb.service_no "
					+ " inner join term cc "
					+ " on bb.term_id=cc.term_id) where driver_name !='1'";
			
			System.out.println(sql);
			stmt.execute(sql);
			conn.commit();
		}catch(Exception ex){
			try {
				conn.rollback();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			ex.printStackTrace();
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
		updateDriverLoginTime(statInfo);		
	}
	
	private void updateDriverLoginTime(InfoContainer statInfo){
		
		HashMap<String,Date> tempMap=new HashMap<String,Date>();
		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
		try {			
			
			DriverLoginOutBean bean = new DriverLoginOutBean();			
			List resultList=RedisConnPoolServer.getSingleInstance().queryTableRecord(new DriverLoginOutBean[]{bean});
			if(resultList!=null&&resultList.size()>0){
				for(int i=0;i<resultList.size();i++){
					bean=(DriverLoginOutBean) resultList.get(i);
					if(bean.getLoginDate()!=null){
						tempMap.put(bean.getServiceNo(), bean.getLoginDate());
					}					
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		DbHandle conn=null;
		try{
			conn=DbServer.getSingleInstance().getConn();
			Date loginTime=sTime;
			String serviceNo="";
			String sql="update ana_driver_unlogout_stat set last_login_time=? where service_no=? and work_date= to_date('"+GeneralConst.YYYY_MM_DD.format(sTime)+"','yyyy-MM-dd')";
			StatementHandle psmt=conn.prepareCall(sql);
			Iterator itr=tempMap.keySet().iterator();
			int count=1;
			while(itr.hasNext()){
				serviceNo=(String) itr.next();
				loginTime=tempMap.get(serviceNo);
				psmt.setTimestamp(1, new Timestamp(loginTime.getTime()));
				psmt.setString(2, serviceNo);
				psmt.addBatch();
				count++;
				if(count%200==0){
					psmt.executeBatch();
				}
			}			
			psmt.executeBatch();
			
		}catch(Exception ex){
			ex.printStackTrace();
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
	
	}
	
	private class DriverOperateInfo
	{
		public String driverId;
		public String driverName;
		public int    companyId;
		public String companyName;
		public int    driverGrade;
		public float  durationTime;
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
		public String plateNo;
		public int businessTime;
	}

}
