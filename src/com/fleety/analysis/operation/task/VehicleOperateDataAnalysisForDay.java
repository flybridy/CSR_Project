package com.fleety.analysis.operation.task;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;

import server.db.DbServer;
import server.var.VarManageServer;

import com.fleety.analysis.AnalysisServer;
import com.fleety.analysis.operation.IOperationAnalysis;
import com.fleety.base.InfoContainer;
import com.fleety.base.event.Event;
import com.fleety.server.event.GlobalEventCenter;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;

public class VehicleOperateDataAnalysisForDay implements IOperationAnalysis{
	
	private HashMap          vehicleMapping  = null;
	private SimpleDateFormat sdf             = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private SimpleDateFormat sdf2            = new SimpleDateFormat("yyyy-MM-dd");
	private float            fuelSurcharges  = 0;
	private float            fuelSurcharges_h = 0;//红的燃油附加费
	private float            fuelSurcharges_l = 0;//绿的燃油附加费
	private float            fuelSurcharges_w = 0;//无障碍燃油附加费
	private float            fuelSurcharges_d = 0;//电动车燃油附加费
	private int              initiateRateKs  = 0;
	private int              maxKs           = 0;
	private String           dayWorkTime;
	private String           nightWorkTime;
	
	
	private boolean queryCondition() throws Exception{
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			StatementHandle stmt = conn.createStatement();
			stmt = conn.prepareStatement("select id,val,type from cost_manage where (valid_start_time is null or valid_start_time<=?) and (valid_end_time is null or valid_end_time>=?) order by id ");
			stmt.setDate(1, new java.sql.Date(new Date().getTime()));
			stmt.setDate(2, new java.sql.Date(new Date().getTime()));
			ResultSet sets = stmt.executeQuery();
			while (sets.next()) {
				String temp = sets.getString("type");
				if(temp!=null&&temp.equals("红的燃油附加费")){
					this.fuelSurcharges_h = sets.getFloat("val");
				}else if (temp!=null&&temp.equals("绿的燃油附加费")) {
					this.fuelSurcharges_l = sets.getFloat("val");
				}else if (temp!=null&&temp.equals("无障碍的士燃油附加费")) {
					this.fuelSurcharges_w = sets.getFloat("val");
				}
			}
			sets.close();
			stmt.close();
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
		return true;
	}
	
	private boolean queryCartype(String dest_no) throws Exception{
		this.fuelSurcharges = 0;
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			StatementHandle stmt = conn.createStatement();
			stmt = conn.prepareStatement("select car_type.signs from car_type left join car on car.type_id = car_type.id where car.car_id = '"+dest_no+"'");
			ResultSet sets = stmt.executeQuery();
			if (sets.next()) {
				String signs = sets.getString("signs");
				if(signs.equals("红的"))
					this.fuelSurcharges = this.fuelSurcharges_h;
				else if(signs.equals("绿的"))
					this.fuelSurcharges = this.fuelSurcharges_l;
				else if(signs.equals("电动")||signs.equals("电动绿的"))
					this.fuelSurcharges = this.fuelSurcharges_d;
				else if(signs.equals("无障碍"))
					this.fuelSurcharges = this.fuelSurcharges_w;
				
			}
			sets.close();
			stmt.close();
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
		return true;
	}
	
	@Override
	public boolean startAnalysisOperation(AnalysisServer parentServer,InfoContainer statInfo)
	{
		// 获取公司map和读取燃油附加费信息
		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
		Date eTime = statInfo.getDate(STAT_END_TIME_DATE);
		
		fuelSurcharges = Integer.parseInt(VarManageServer.getSingleInstance().getVarStringValue("fuel_surcharges"));
		initiateRateKs = Integer.parseInt(VarManageServer.getSingleInstance().getVarStringValue("initiate_rate_ks"));
		maxKs = Integer.parseInt(VarManageServer.getSingleInstance().getVarStringValue("max_ks"));
		dayWorkTime = VarManageServer.getSingleInstance().getVarStringValue("day_work_time");
		nightWorkTime = VarManageServer.getSingleInstance().getVarStringValue("night_work_time");
//		
		this.vehicleMapping = null;
		DbHandle conn = DbServer.getSingleInstance().getConnWithUseTime(0);
		try{
			StatementHandle stmt = conn.createStatement();
			StringBuilder sb = new StringBuilder();
			sb.append("select count(*) as sum from ANA_SINGLE_CAR_DAY_STAT ")
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
			System.out.println("Not Need Analysis:"+this.toString()+","+sdf2.format(sTime));
		}else{
			System.out.println("Start Analysis:"+this.toString()+","+sdf2.format(sTime));
			try {
				
//				只有深圳在用红绿的燃油附加费，而深圳已单独有此类，其他地方不再用
//				this.queryCondition();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
//		return true;
		return this.vehicleMapping != null;
	}

	@Override
	public void analysisDestOperation(AnalysisServer parentServer, InfoContainer statInfo)
	{
//		if(this.vehicleMapping == null){
//			return ;
//		}
		String[] dayWorkTimeArr = dayWorkTime.split(",");
		String[] nightWorkTimeArr = nightWorkTime.split(",");
		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
		Date eTime = statInfo.getDate(STAT_END_TIME_DATE);
		
		Calendar cal = Calendar.getInstance();
		cal.setTime(sTime);
//		cal.add(Calendar.DAY_OF_MONTH, -1);
		cal.set(Calendar.HOUR_OF_DAY, Integer.valueOf(dayWorkTimeArr[0]));
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		Date start = cal.getTime();
		
		cal.set(Calendar.HOUR_OF_DAY, Integer.valueOf(dayWorkTimeArr[1]));
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		Date end = cal.getTime();
		
		
		
		DbHandle conn = DbServer.getSingleInstance().getConnWithUseTime(0);
		
		
		
		try{
			StringBuilder sql = new StringBuilder();
			sql.append("select * from (")
			   .append(" select dispatch_car_no as plate_no,taxi_company as company_id,")
			   .append("  count(*) as work_times,")
			   .append("  sum(distance+free_distance) as total_distance,")
			   .append("  sum(decode(sign(distance),1,distance,-1,0,distance)) as work_distance,")
			   .append("  sum(free_distance) as free_distance,")
			   .append("  sum(waiting_hour) as waiting_hour,")
			   .append("  sum(waiting_minute) as waiting_minute,")
			   .append("  sum(waiting_second) as waiting_second,")
			   .append("  sum(sum) as work_income,")
			   .append("  sum(case when distance <= ").append(initiateRateKs).append(" then 1 else 0 end) as work_times_first,")
			   .append("  sum(case when distance > ").append(initiateRateKs).append(" and distance <= ").append(maxKs).append(" then 1 else 0 end) as work_times_second,")
			   .append("  sum(case when distance > ").append(maxKs).append(" then 1 else 0 end) as work_times_third,")
			   .append("  sum(case when date_up >= to_date('").append(sdf.format(start)).append("','yyyy-mm-dd hh24:mi:ss')")
			   .append("            and date_up < to_date('").append(sdf.format(end)).append("','yyyy-mm-dd hh24:mi:ss')")
			   .append("           then sum else 0 end) as work_income_day,")
			   .append("  sum(case when (date_up >= to_date('").append(sdf.format(end)).append("','yyyy-mm-dd hh24:mi:ss')")
			   .append("            and date_up < to_date('").append(sdf.format(eTime)).append("','yyyy-mm-dd hh24:mi:ss')) or ")
			   .append("            (date_up >= to_date('").append(sdf.format(sTime)).append("','yyyy-mm-dd hh24:mi:ss')")
			   .append("            and date_up < to_date('").append(sdf.format(start)).append("','yyyy-mm-dd hh24:mi:ss')) ")
			   .append("           then sum else 0 end) as work_income_night,")
			   .append("  sum(case when distance <= ").append(initiateRateKs).append(" then distance else 0 end) as work_distance_first,")
			   .append("  sum(case when distance > ").append(initiateRateKs).append(" and distance <= ").append(maxKs).append(" then distance else 0 end) as work_distance_second,")
			   .append("  sum(case when distance > ").append(maxKs).append(" then distance else 0 end) as work_distance_third,")
			   .append("  sum(case when distance <= ").append(initiateRateKs).append(" then sum else 0 end) as work_income_first,")
			   .append("  sum(case when distance > ").append(initiateRateKs).append(" and distance <= ").append(maxKs).append(" then sum else 0 end) as work_income_second,")
			   .append("  sum(case when distance > ").append(maxKs).append(" then sum else 0 end) as work_income_third")
			   .append(" from SINGLE_BUSINESS_DATA_BS ")
			   .append(" where dispatch_car_no is not null ")
			   .append("       and date_up >= to_date('").append(sdf.format(sTime)).append("','yyyy-mm-dd hh24:mi:ss')")
			   .append("       and date_up < to_date('").append(sdf.format(eTime)).append("','yyyy-mm-dd hh24:mi:ss')")//shijian
			   .append(" group by dispatch_car_no,taxi_company")
			   .append(") a ")
			   .append("left join ")
			   .append(" (")
			   .append("   select TERM_ID,TERM_NAME as company_name from term")
			   .append(" ) d on a.company_id = d.TERM_ID ")
			   .append("left join ")
			   .append(" (")
			   .append("   select car_no,count(*) as telcall_times,")
			   .append("          sum(case when status=3 then 1 else 0 end) as telcall_finish_times")
			   .append("   from taxi_order_list ")
			   .append("   where car_no is not null ")
			   .append("         and created_time >= to_date('").append(sdf.format(sTime)).append("','yyyy-mm-dd hh24:mi:ss')")
			   .append("         and created_time < to_date('").append(sdf.format(eTime)).append("','yyyy-mm-dd hh24:mi:ss')")
			   .append("   group by car_no")
			   .append(" ) b on a.plate_no = b.car_no ")
			   .append("left join ")
			   .append(" (")
			   .append("   select car_no,count(*) as service_evaluate_times,")
			   .append("          sum(case when grade_type = 0 then 1 else 0 end) as satisfisfy_times,")
			   .append("          sum(case when grade_type = 1 then 1 else 0 end) as unsatisfy_times,")
			   .append("          sum(case when grade_type = 2 then 1 else 0 end) as highlySatisfisfy_times,")
			   .append("          sum(case when grade_type = 3 then 1 else 0 end) as unJudge_times")
			   .append("   from grade where car_no is not null ")
			   .append("                    and create_time >= to_date('").append(sdf.format(sTime)).append("','yyyy-mm-dd hh24:mi:ss')")
			   .append("                    and create_time < to_date('").append(sdf.format(eTime)).append("','yyyy-mm-dd hh24:mi:ss')")
			   .append("   group by car_no")
			   .append(" ) c on a.plate_no = c.car_no");
			System.out.println(sql.toString());
			StatementHandle stmt = conn.createStatement();
			Long st1 = System.currentTimeMillis();
			ResultSet rs = stmt.executeQuery(sql.toString());
			Long st2 = System.currentTimeMillis();
			System.out.println("VehicleOperateDataAnalysisForDay查询耗时:"+(st2-st1));
			while(rs.next())
			{
				String plateNo = rs.getString("plate_no");
				VehicleOperateInfo vInfo = new VehicleOperateInfo();
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
				vInfo.telcallTimes = rs.getInt("telcall_times");
				vInfo.telcallFinishTimes = rs.getInt("telcall_finish_times");
				vInfo.serviceEvaluateTimes = rs.getInt("service_evaluate_times");
				vInfo.satisfisfyTimes = rs.getInt("satisfisfy_times");
				vInfo.unsatisfyTimes = rs.getInt("unsatisfy_times");
				vInfo.highlySatisfisfyTimes = rs.getInt("highlySatisfisfy_times");
				vInfo.unJudgeTimes = rs.getInt("unJudge_times");
				vInfo.workTimesFirst = rs.getInt("work_times_first");
				vInfo.workTimesSecond = rs.getInt("work_times_second");
				vInfo.workTimesThird = rs.getInt("work_times_third");
				vInfo.workDistanceFirst = rs.getFloat("work_distance_first");
				vInfo.workDistanceSecond = rs.getFloat("work_distance_second");
				vInfo.workDistanceThird = rs.getFloat("work_distance_third");
				
				vInfo.workIncomeFirst = rs.getFloat("work_income_first");
				vInfo.workIncomeSecond = rs.getFloat("work_income_second");
				vInfo.workIncomeThird = rs.getFloat("work_income_third");
				
				vInfo.workIncomeDay = rs.getFloat("work_income_day");
				vInfo.workIncomeNight = rs.getFloat("work_income_night");
				
				DecimalFormat df = new DecimalFormat(".##");
				if (vInfo.workTimes!=0) {
					String dis=df.format(vInfo.telcallTimes/vInfo.workTimes);//取两位小数
					vInfo.dispatch_percent = Double.parseDouble(dis);//电召率
				}
				if (vInfo.totalDistance!=0) {
					String l = df.format(vInfo.freeDistance/vInfo.totalDistance);
					vInfo.load_percent = Double.parseDouble(l);//载客率
				}
				vehicleMapping.put(plateNo, vInfo);
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
		Date sDate = statInfo.getDate(STAT_START_TIME_DATE);
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try{
			conn.setAutoCommit(false);
//			StatementHandle stmt = null;
//			 delete data first
//			StatementHandle stmt2 = conn.createStatement();
//			for(Iterator itr = this.vehicleMapping.keySet().iterator();itr.hasNext();){
//				driverNo = (String)itr.next();
//				vehicleOperateInfo = (VehicleOperateInfo)this.vehicleMapping.get(driverNo);
//				String workDate = vehicleOperateInfo.workDate;
//				String sql = "delete from ANA_DRIVER_STATUS_DAY_STAT where DRIVER_ID = '"+driverNo+"' and WORK_DATE = '"+workDate+"'";
//				stmt2.addBatch(sql);
//			}
//			stmt2.executeBatch();
			// inset into databases
			StatementHandle stmt = conn
					.prepareStatement("insert into ANA_SINGLE_CAR_DAY_STAT(id,plate_no,company_id,company_name,work_times,total_distance,work_distance,free_distance,waiting_hour,waiting_minute,waiting_second,total_income,work_income,fuel_income,telcall_times,telcall_finish_times,service_evaluate_times,satisfisfy_times,unsatisfy_times,highlySatisfisfy_times,unJudge_times,work_date,analysis_time,work_times_first,work_times_second,work_times_third,work_income_day,work_income_night,work_distance_first,work_distance_second,work_distance_third,work_income_first,work_income_second,work_income_third,DISPATCH_PERCENT,LOAD_PERCENT) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
			for(Iterator itr = this.vehicleMapping.keySet().iterator();itr.hasNext();){
				plateNo = (String)itr.next();
				vehicleOperateInfo = (VehicleOperateInfo)this.vehicleMapping.get(plateNo);
				stmt.setInt(1, (int)DbServer.getSingleInstance().getAvaliableId(conn, "ANA_SINGLE_CAR_DAY_STAT", "id"));
				stmt.setString(2, vehicleOperateInfo.plateNo);
				stmt.setInt(3, vehicleOperateInfo.companyId);
				stmt.setString(4, vehicleOperateInfo.companyName);
				stmt.setInt(5, vehicleOperateInfo.workTimes);
				stmt.setFloat(6, vehicleOperateInfo.totalDistance);
				stmt.setFloat(7, vehicleOperateInfo.workDistance);
				stmt.setFloat(8, vehicleOperateInfo.freeDistance);
				stmt.setInt(9, vehicleOperateInfo.waitingHour);
				stmt.setInt(10, vehicleOperateInfo.waitingMinute);
				stmt.setInt(11, vehicleOperateInfo.waitingSecond);
				
//				只有深圳在用红绿的燃油附加费，而深圳已单独有此类，其他地方不再用
//				this.queryCartype(vehicleOperateInfo.plateNo);
				
				stmt.setFloat(12, vehicleOperateInfo.workIncome + fuelSurcharges * vehicleOperateInfo.workTimes);
				stmt.setFloat(13, vehicleOperateInfo.workIncome);
				stmt.setFloat(14, fuelSurcharges * vehicleOperateInfo.workTimes);
				stmt.setInt(15, vehicleOperateInfo.telcallTimes);
				stmt.setInt(16, vehicleOperateInfo.telcallFinishTimes);
				stmt.setInt(17, vehicleOperateInfo.serviceEvaluateTimes);
				stmt.setInt(18, vehicleOperateInfo.satisfisfyTimes);
				stmt.setInt(19, vehicleOperateInfo.unsatisfyTimes);
				stmt.setInt(20, vehicleOperateInfo.highlySatisfisfyTimes);
				stmt.setInt(21, vehicleOperateInfo.unJudgeTimes);
				stmt.setString(22, sdf2.format(sDate));
				stmt.setDate(23, new java.sql.Date(new Date().getTime()));
				stmt.setInt(24, vehicleOperateInfo.workTimesFirst);
				stmt.setInt(25, vehicleOperateInfo.workTimesSecond);
				stmt.setInt(26, vehicleOperateInfo.workTimesThird);
				stmt.setFloat(27, vehicleOperateInfo.workIncomeDay);
				stmt.setFloat(28, vehicleOperateInfo.workIncomeNight);
				stmt.setFloat(29, vehicleOperateInfo.workDistanceFirst);
				stmt.setFloat(30, vehicleOperateInfo.workDistanceSecond);
				stmt.setFloat(31, vehicleOperateInfo.workDistanceThird);
				stmt.setFloat(32, vehicleOperateInfo.workIncomeFirst);
				stmt.setFloat(33, vehicleOperateInfo.workIncomeSecond);
				stmt.setFloat(34, vehicleOperateInfo.workIncomeThird);
				stmt.setDouble(35, vehicleOperateInfo.dispatch_percent);
				stmt.setDouble(36, vehicleOperateInfo.load_percent);
				stmt.addBatch();
				recordNum ++;
			}
			stmt.executeBatch();
			conn.commit();
			GlobalEventCenter.getSingleInstance().dispatchEvent(new Event(GlobalEventCenter.CAR_BUSINESS_STAT_FINISH, sDate, this));
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
		System.out.println("Finish vehicle operate data Analysis:"+this.toString()+" recordNum="+recordNum+","+sdf2.format(sDate));
	}
	
	private class VehicleOperateInfo
	{
		public String plateNo;
		public int    companyId;
		public String companyName;
		public int    workTimes;
		public int    workTimesFirst;
		public int    workTimesSecond;
		public int    workTimesThird;
		public float  totalDistance;
		public float  workDistance;
		public float  workDistanceFirst;
		public float  workDistanceSecond;
		public float  workDistanceThird;
		public float  workIncomeFirst;
		public float  workIncomeSecond;
		public float  workIncomeThird;
		public float  freeDistance;
		public int    waitingHour;
		public int    waitingMinute;
		public int    waitingSecond;
		public float  totalIncome;
		public float  workIncome;
		public float  workIncomeDay;
		public float  workIncomeNight;
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
		
		public double dispatch_percent;
		public double load_percent;
	}
			
}
