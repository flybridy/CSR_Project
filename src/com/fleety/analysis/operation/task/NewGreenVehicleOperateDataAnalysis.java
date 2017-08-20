package com.fleety.analysis.operation.task;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import server.db.DbServer;

import com.fleety.analysis.AnalysisServer;
import com.fleety.analysis.operation.IOperationAnalysis;
import com.fleety.base.InfoContainer;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;

public class NewGreenVehicleOperateDataAnalysis implements IOperationAnalysis{
	
	private HashMap          resultMapping  = null;
	private HashMap          exceptionMapping  = null;
	private SimpleDateFormat sdf             = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private SimpleDateFormat sdf2            = new SimpleDateFormat("yyyy-MM-dd");
	private float            fuelSurcharges_h  = 0; //红车燃油附加费
	private float            fuelSurcharges_l  = 0; //绿车燃油附加费
	private float            fuelSurcharges_d  = 0;//电动车
	private float            fuelSurcharges_w  = 0;//无障碍燃油附加费
	
	List<Integer> list = null;
	
    private int total_car = 0;
    private int notfit_car = 0;
	
	@Override
	public boolean startAnalysisOperation(AnalysisServer parentServer,InfoContainer statInfo)
	{
		// 获取公司map和读取燃油附加费信息
		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
		Date eTime = statInfo.getDate(STAT_END_TIME_DATE);
		this.resultMapping = null;
		this.exceptionMapping = null;
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try{
			StatementHandle stmt = conn.createStatement();
			StringBuilder sb = new StringBuilder();
			sb.append("select count(*) as sum from green_price_date_sz ")
			  .append(" where record_date = to_date('").append(sdf2.format(sTime)).append("','yyyy-MM-dd')");
			System.out.println(sb.toString());
			ResultSet sets = stmt.executeQuery(sb.toString());
			if(sets.next()){
				int sum = sets.getInt("sum");
				if(sum == 0){
					this.resultMapping = new HashMap();
					this.exceptionMapping = new HashMap();				
	}		
	}
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
		
		if(this.resultMapping == null){
			System.out.println("Not Need Analysis:"+this.toString());
		}else{
			System.out.println("Start Analysis:新绿的："+this.toString());
		}
		
		return this.resultMapping != null;
	}

	

	@Override
	public void analysisDestOperation(AnalysisServer parentServer, InfoContainer statInfo)
	{   			
		this.resultMapping = new HashMap();
		this.exceptionMapping = new HashMap();	
		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
		Date eTime = statInfo.getDate(STAT_END_TIME_DATE);
		System.out.println("新绿的开始sTime:"+sdf.format(sTime)+" eTime:"+sdf.format(eTime));
		DbHandle conn = DbServer.getSingleInstance().getConn();
		int car_nums=1;
		try {
			StatementHandle stmt1 = conn.prepareStatement("select count(*) car_nums from car where type_id=9 and mdt_id >0");
			ResultSet rs1 = stmt1.executeQuery();
			while(rs1.next()){
				car_nums=rs1.getInt("car_nums");
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		try{
			StringBuilder sql = new StringBuilder();
			sql.append("select record_date, count(*) hege_nums, round(count(*)/").append(car_nums+",2) hegelv, round(avg(times),2) avg_times , round(avg(money_sum),2) money_sum, round(avg(money_sum_t),2) money_sum_t,  round(avg(distance_sum),2) avg_distance  from (  select  to_char(date_up, 'yyyy-mm-dd') as record_date, car_no,  count(*) as times ,  sum(sum) as money_sum,    sum(sum)+  count(*)*0.9 *1 as money_sum_t , sum(a.DISTANCE)  as distance_sum , sum (round((a.DATE_DOWN -a.date_up)*24*60, 2)) as  business_time from single_business_data_bs  a left join car b on a.car_no=b.car_id  left join car_type c on b.type_id= c.id  where     c.id=9  and   round((a.DATE_DOWN -a.date_up)*24*60, 2) >1  and  round((a.recode_time-a.DATE_DOWN)*24*60, 2) < 1440  and   a.DISTANCE > 0.3  and a.distance <100 ")
			.append("       and date_up >= to_date('").append(sdf.format(sTime)).append("','yyyy-mm-dd hh24:mi:ss')")
			   .append("       and date_up <= to_date('").append(sdf.format(eTime)).append("','yyyy-mm-dd hh24:mi:ss')")
			   .append("       and recode_time >= to_date('").append(sdf.format(sTime)).append("','yyyy-mm-dd hh24:mi:ss')")
			   .append("       and recode_time <= to_date('").append(sdf.format(eTime)).append("','yyyy-mm-dd hh24:mi:ss')")
			   .append("  group by  car_no,  to_char(date_up, 'yyyy-mm-dd')  having count(*) > 15  and   count(*) < 85 and sum(sum)<1500 and sum(sum)>200  and sum(a.distance)<350  and sum(a.distance)>160) group by record_date");
			
			System.out.println(sql.toString());
			StatementHandle stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql.toString());
			
			VehicleOperateInfo vInfo =  null;
			int count=0;
			String key="record";
			while(rs.next())					
			{	
				count++;
				vInfo=new VehicleOperateInfo();
				vInfo.record_date=rs.getDate("record_date");			
				vInfo.hege_nums=rs.getDouble("hege_nums");
				vInfo.hegelv=rs.getDouble("hegelv");
				vInfo.avg_times=rs.getDouble("avg_times");
				vInfo.money_sum=rs.getDouble("money_sum");
				vInfo.money_sum_t=rs.getDouble("money_sum_t");
				vInfo.avg_distance=rs.getDouble("avg_distance");
					resultMapping.put(key+count, vInfo);
				}
							
			
			System.out.println("车型 统计到:"+count+" 条数据。");
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		
		}
			endAnalysisOperationSaveData(parentServer,statInfo);
		
	}
	
	public void endAnalysisOperationSaveData(AnalysisServer parentServer,InfoContainer statInfo) {
		if(this.resultMapping == null){ 
			System.out.println("分析结果数据为:"+resultMapping.size()+" 条");		
			return ;
		}
		int recordNum = 0;
		String plateNo = "";
		VehicleOperateInfo vehicleOperateInfo;
		Date sDate = statInfo.getDate(STAT_START_TIME_DATE);
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try{
			conn.setAutoCommit(false);
			StatementHandle stmt = conn
					.prepareStatement("insert into green_price_date_sz(id,record_date,hege_nums,hegelv,avg_times,money_sum,money_sum_t,avg_distance) values(?,?,?,?,?,?,?,?)");
			for(Iterator itr = this.resultMapping.keySet().iterator();itr.hasNext();){
				plateNo = (String)itr.next();
				vehicleOperateInfo = (VehicleOperateInfo)this.resultMapping.get(plateNo);
				stmt.setInt(1, (int)DbServer.getSingleInstance().getAvaliableId(conn, "green_price_date_sz", "id"));
				stmt.setTimestamp(2, new Timestamp(vehicleOperateInfo.record_date.getTime()));
				stmt.setDouble(3, vehicleOperateInfo.hege_nums);
				stmt.setDouble(4, vehicleOperateInfo.hegelv);
				stmt.setDouble(5, vehicleOperateInfo.avg_times);
				stmt.setDouble(6, vehicleOperateInfo.money_sum);
				stmt.setDouble(7, vehicleOperateInfo.money_sum_t);
				stmt.setDouble(8, vehicleOperateInfo.avg_distance);
				stmt.addBatch();
				recordNum ++;
				if(recordNum%200==0){
					stmt.executeBatch();
				}
			}
			stmt.executeBatch();										
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
		System.out.println("Finish vehicle operate 新绿的 NewGreenVehicleOperateDataAnalysis:"+this.toString()+" recordNum="+recordNum);
	}
	
	public void endAnalysisOperation(AnalysisServer parentServer,InfoContainer statInfo) {
		return ;
	}
	
	private class VehicleOperateInfo
	{
		    public Date      record_date;//时间
			public double    hege_nums;//合格数
			public double    hegelv;//合格率
			public double    avg_times;//平均次数
			public double    money_sum;//营运金额
			public double    money_sum_t;//营运金额含燃油附加费
			public double    avg_distance;//营运距离
	}
	
	
}
