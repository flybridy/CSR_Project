package com.fleety.job.task;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import server.db.DbServer;
import server.var.VarManageServer;

import com.fleety.analysis.realtime.DriverBusinessRealTimeBean;
import com.fleety.base.GeneralConst;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.fleety.util.pool.db.redis.RedisConnPoolServer;
import com.fleety.util.pool.db.redis.RedisTableBean;
import com.fleety.util.pool.thread.BasicTask;

public class DriverBusinessRealTimeAnasisTask extends BasicTask {

	public boolean execute() throws Exception {
		this.deleteInfo();
		
		Calendar now = Calendar.getInstance();
		
		now.set(Calendar.HOUR_OF_DAY, 0);
		now.set(Calendar.MINUTE, 0);
		now.set(Calendar.SECOND, 0);
		now.set(Calendar.MILLISECOND, 0);
		Date startTime = now.getTime();
		
		now.set(Calendar.HOUR_OF_DAY, 23);
		now.set(Calendar.MINUTE, 59);
		now.set(Calendar.SECOND, 59);
		now.set(Calendar.MILLISECOND, 0);
		int fuelSurcharges = Integer.parseInt(VarManageServer.getSingleInstance().getVarStringValue("fuel_surcharges"));
		
		Date endTime = now.getTime();
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try{
			StringBuilder sql = new StringBuilder();
			sql.append("select * from (")
			   .append(" select SERVICE_NO as driver_id,taxi_company as company_id,")
			   .append(" count(*) as work_times,")
			   .append(" sum(distance+free_distance) as total_distance,")
			   .append(" sum(decode(sign(distance),1,distance,-1,0,distance)) as work_distance,")
			   .append(" sum(free_distance) as free_distance,")
			   .append(" sum(waiting_hour) as waiting_hour,")
			   .append(" sum(waiting_minute) as waiting_minute,")
			   .append(" sum(waiting_second) as waiting_second,")
			   .append(" sum(abs(date_down -date_up) * 24 * 60 * 60) work_time_seconds,")
			   .append(" sum(sum) as work_income")
			   .append(" from SINGLE_BUSINESS_DATA_BS ")
			   .append(" where SERVICE_NO is not null ")
			   .append(" and recode_time >= to_date('").append(GeneralConst.YYYY_MM_DD_HH_MM_SS.format(startTime)).append("','yyyy-mm-dd hh24:mi:ss')")
			   .append(" and recode_time <= to_date('").append(GeneralConst.YYYY_MM_DD_HH_MM_SS.format(endTime)).append("','yyyy-mm-dd hh24:mi:ss')")//shijian
			   .append(" group by SERVICE_NO,taxi_company) a ")
			   .append(" left join (select TERM_ID,TERM_NAME as company_name from term) d on a.company_id = d.TERM_ID")
			   .append(" left join ( select SERVICE_NO,DRIVER_NAME,GRADE as driver_grade from driver_info) d on a.driver_id = d.SERVICE_NO ")
			   .append(" left join (")
			   .append(" select driver_id,count(*) as telcall_times,sum(case when status=3 then 1 else 0 end) as telcall_finish_times")
			   .append(" from taxi_order_list where driver_id is not null ")
			   .append(" and created_time >= to_date('").append(GeneralConst.YYYY_MM_DD_HH_MM_SS.format(startTime)).append("','yyyy-mm-dd hh24:mi:ss')")
			   .append(" and created_time <= to_date('").append(GeneralConst.YYYY_MM_DD_HH_MM_SS.format(endTime)).append("','yyyy-mm-dd hh24:mi:ss')")
			   .append(" group by driver_id ) b on a.driver_id = b.driver_id ")
			   .append(" left join (select driver_id,count(*) as service_evaluate_times,")
			   .append(" sum(case when grade_type = 0 then 1 else 0 end) as satisfisfy_times,")
			   .append(" sum(case when grade_type = 1 then 1 else 0 end) as unsatisfy_times,")
			   .append(" sum(case when grade_type = 2 then 1 else 0 end) as highlySatisfisfy_times,")
			   .append(" sum(case when grade_type = 3 then 1 else 0 end) as unJudge_times")
			   .append(" from grade where driver_id is not null ")
			   .append(" and create_time >= to_date('").append(GeneralConst.YYYY_MM_DD_HH_MM_SS.format(startTime)).append("','yyyy-mm-dd hh24:mi:ss')")
			   .append(" and create_time <= to_date('").append(GeneralConst.YYYY_MM_DD_HH_MM_SS.format(endTime)).append("','yyyy-mm-dd hh24:mi:ss')")
			   .append(" group by driver_id ) c on a.driver_id = c.driver_id ")
			   .append("left join v_ana_driver_info f on a.driver_id = f.SERVICE_NO ");

			StatementHandle stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql.toString());
			List<DriverBusinessRealTimeBean> list = new ArrayList<DriverBusinessRealTimeBean>();
			DriverBusinessRealTimeBean dInfo = null;
			while(rs.next())
			{
				String driverId = rs.getString("driver_id");
				dInfo = new DriverBusinessRealTimeBean();
				dInfo.setUid(driverId);
				dInfo.setDriverId(driverId);
				dInfo.setDriverName(rs.getString("driver_name"));
				dInfo.setDriverGrade(rs.getInt("driver_grade"));
				dInfo.setWorkTimeSeconds(rs.getInt("work_time_seconds"));
				dInfo.setCompanyId(rs.getInt("company_id"));
				dInfo.setCompanyName(rs.getString("company_name"));
				dInfo.setWorkTimes(rs.getInt("work_times"));
				dInfo.setTotalDistance(rs.getFloat("total_distance"));
				dInfo.setWorkDistance(rs.getFloat("work_distance"));
			    dInfo.setFreeDistance(rs.getFloat("free_distance"));
				dInfo.setWaitingHour(rs.getInt("waiting_hour"));
				dInfo.setWaitingMinute(rs.getInt("waiting_minute"));
				dInfo.setWaitingSecond(rs.getInt("waiting_second"));
				dInfo.setWorkIncome(rs.getFloat("work_income"));
				dInfo.setFuelIncome(dInfo.getWorkTimes() * fuelSurcharges);
				dInfo.setTotalIncome(dInfo.getWorkIncome() + dInfo.getFuelIncome());
				dInfo.setTelcallTimes(rs.getInt("telcall_times"));
				dInfo.setTelcallFinishTimes(rs.getInt("telcall_finish_times"));
				dInfo.setServiceEvaluateTimes(rs.getInt("service_evaluate_times"));
				dInfo.setSatisfisfyTimes(rs.getInt("satisfisfy_times"));
				dInfo.setUnsatisfyTimes(rs.getInt("unsatisfy_times"));
				dInfo.setHighlySatisfisfyTimes(rs.getInt("highlySatisfisfy_times"));
				dInfo.setUnJudgeTimes(rs.getInt("unJudge_times"));
				dInfo.setPlateNo(rs.getString("CAR_ID"));
				list.add(dInfo);
			}
			
			RedisTableBean beans[] = new RedisTableBean[list.size()];
			list.toArray(beans);
			
			RedisConnPoolServer.getSingleInstance().saveTableRecord(beans);
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
		return false;
	}
	
	private void deleteInfo() throws Exception{
		Calendar now = Calendar.getInstance();
		String time = GeneralConst.HHMM.format(now.getTime());
		if(!time.equals("0000") && !time.equals("1200")){
			return;
		}
		
		DriverBusinessRealTimeBean bean = new DriverBusinessRealTimeBean();
		Set<String> keySet = RedisConnPoolServer.getSingleInstance().getAllIdsForTable(bean);
		Iterator<String> it = keySet.iterator();
		String uid = "";
		List<DriverBusinessRealTimeBean> list = new ArrayList<DriverBusinessRealTimeBean>();
		while(it.hasNext()){
			bean = new DriverBusinessRealTimeBean();
			uid = it.next();
			bean.setUid(uid);
			list.add(bean);
		}
		
		if(list.size()>0){
			RedisTableBean[] beanArr= new DriverBusinessRealTimeBean[list.size()];
			list.toArray(beanArr);
			RedisConnPoolServer.getSingleInstance().deleteTableRecord(beanArr);
		}
	}
	
	public String getDesc() {
		return "营运数据实时数据分析按驾驶员";
	}

	public Object getFlag() {
		return "DriverBusinessRealTimeAnasisTask";
	}

}
