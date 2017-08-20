package com.fleety.analysis.operation.task;

import java.sql.ResultSet;
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

public class NewVehicleOperateDataAnalysisForDaySZ implements IOperationAnalysis{
	
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
			boolean bol = queryCondition();
			if(!bol){
				System.out.println("NewVehicleOperateDataAnalysisForDaySZ 燃油附加费条件不完整或过滤条件不存在");
				return false;
			}
			StatementHandle stmt = conn.createStatement();
			StringBuilder sb = new StringBuilder();
			sb.append("select count(*) as sum from ana_business_day_stat_sz_test ")
			  .append(" where stat_time = to_date('").append(sdf2.format(sTime)).append("','yyyy-MM-dd')");
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
			System.out.println("Start Analysis:"+this.toString());
		}
		
		return this.resultMapping != null;
	}

	private boolean queryCondition() throws Exception{
		this.list = new ArrayList<Integer>();
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			StatementHandle stmt = conn.createStatement();
			ResultSet sets = stmt.executeQuery("select id from car_type");
			while(sets.next()){		
				list.add(sets.getInt("id"));//获取所有车型			
			}
			sets.close();
			stmt.close();
			stmt = conn.prepareStatement("select id,val,type from cost_manage where (valid_start_time is null or valid_start_time<=?) and (valid_end_time is null or valid_end_time>=?) order by id ");
			stmt.setDate(1, new java.sql.Date(new Date().getTime()));
			stmt.setDate(2, new java.sql.Date(new Date().getTime()));
			sets = stmt.executeQuery();
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
//			if(fuelSurcharges_h==0 || fuelSurcharges_l==0 || fuelSurcharges_w==0){
//				return false;
//			}
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
		return true;
	}

	@Override
	public void analysisDestOperation(AnalysisServer parentServer, InfoContainer statInfo)
	{   
		for(int j=0;j<list.size();j++){
			int type_id=list.get(j);
		this.resultMapping = new HashMap();
		this.exceptionMapping = new HashMap();	
		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
		Date eTime = statInfo.getDate(STAT_END_TIME_DATE);
		System.out.println("新营运数据分析开始sTime:"+sdf.format(sTime)+" eTime:"+sdf.format(eTime));
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try{
			StringBuilder sql = new StringBuilder();
			sql.append("select plate_no dest_no,company_id,company_name,type_id, count(*) as workTimes,sum(total_distance) as total_distance ,sum(work_distance) as work_distance,sum(free_distance) as free_distance,sum(waiting_hour) as waiting_hour,sum(waiting_minute) as waiting_minute,sum(waiting_second) as waiting_second ,sum(work_income) as work_income from (select * from (select recode_time,dispatch_car_no as plate_no,taxi_company as company_id, distance,(distance+free_distance) as total_distance,  decode(sign(distance),1,distance,-1,0,distance) as work_distance,  free_distance,date_up,date_down,  waiting_hour,  waiting_minute,  waiting_second,  sum work_income from SINGLE_BUSINESS_DATA_BS  where dispatch_car_no is not null")
			.append("       and date_up >= to_date('").append(sdf.format(sTime)).append("','yyyy-mm-dd hh24:mi:ss')")
			   .append("       and date_up <= to_date('").append(sdf.format(eTime)).append("','yyyy-mm-dd hh24:mi:ss')")
			   .append(" ) a  left join  ( select TERM_ID,TERM_NAME as company_name from term ) d on a.company_id = d.TERM_ID  left join  (select car_id,type_id from car) car on car.car_id=a.plate_no where round((DATE_DOWN -date_up)*24*60, 2) >1  and  round((recode_time-a.DATE_DOWN)*24*60, 2) < 1440  and  DISTANCE > 0.3  and distance <200")
			.append(" and type_id="+type_id)
			.append(" )   group by plate_no,company_id,company_name,type_id  having count(*)>15 and count(*) < 85 and sum(work_income)<1500 and sum(work_income)>200");
			if(type_id==2||type_id==9){//绿的和调价绿的车型
				sql.append(" and sum(total_distance)>170");
			}else{
				sql.append(" and sum(total_distance)>190");
			}
			System.out.println(sql.toString());
			StatementHandle stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql.toString());
			
			VehicleOperateInfo vInfo =  null;
			int count=0;
			while(rs.next())
			{
				count++;
				String dest_no = rs.getString("dest_no");
				if(!resultMapping.containsKey(dest_no)){
					
					vInfo = new VehicleOperateInfo();
					vInfo.companyId = rs.getInt("company_id");
					vInfo.company_name = rs.getString("company_name");
					vInfo.type_id = type_id;
					int workTimes=rs.getInt("workTimes");
					vInfo.workTimes=workTimes;
					vInfo.total_distance=rs.getFloat("total_distance");
					vInfo.work_distance=rs.getFloat("work_distance");
					vInfo.free_distance=rs.getFloat("free_distance");
					vInfo.waiting_hour=rs.getInt("waiting_hour");
					vInfo.waiting_minute=rs.getInt("waiting_minute");
					vInfo.waiting_second=rs.getInt("waiting_second");
					float work_income=rs.getInt("work_income");
					float fuelIncome=0;
					
					if(type_id==1){
						fuelIncome= (float) (fuelSurcharges_h * workTimes*0.9);// 红的燃油附加收入
					}else if(type_id==2||type_id==9){
						fuelIncome= (float) (fuelSurcharges_l * workTimes*0.9);// 绿的燃油附加收入
					}else if(type_id==4){
						fuelIncome= (float) (fuelSurcharges_w * workTimes*0.9);// 燃油附加收入
					}
					
					vInfo.workIncome=work_income;
					vInfo.fuelIncome=fuelIncome;
					vInfo.totalIncome = work_income + fuelIncome ;// 总收入
					vInfo.startTime = sTime;										
					vInfo.isfit = 0 ;//车辆营运是否是否合格，默认值0合格，1不合格
					vInfo.filter_type = 5;//过滤方案号
					
					resultMapping.put(dest_no, vInfo);
				}
							
			}
			System.out.println("车型:"+type_id+" 统计到:"+count+" 辆车。");
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		
		}
			endAnalysisOperationSaveData(parentServer,statInfo);
		}
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
					.prepareStatement("insert into ana_business_day_stat_sz_test(id,dest_no,company_id,company_name," +
							"business_num,mile,business_mile,free_mile,waiting_hour,waiting_minute,waiting_second," +
							"business_money_t,business_money,stat_time,condition,type_id,is_fit,filter_type) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
			for(Iterator itr = this.resultMapping.keySet().iterator();itr.hasNext();){
				plateNo = (String)itr.next();
				vehicleOperateInfo = (VehicleOperateInfo)this.resultMapping.get(plateNo);
				stmt.setInt(1, (int)DbServer.getSingleInstance().getAvaliableId(conn, "ana_business_day_stat_sz_test", "id"));
				stmt.setString(2, plateNo);
				stmt.setInt(3, vehicleOperateInfo.companyId);
				stmt.setString(4, vehicleOperateInfo.company_name);
				stmt.setInt(5, vehicleOperateInfo.workTimes);
				stmt.setFloat(6, vehicleOperateInfo.total_distance);
				stmt.setFloat(7, vehicleOperateInfo.work_distance);
				stmt.setFloat(8, vehicleOperateInfo.free_distance);
				stmt.setInt(9, vehicleOperateInfo.waiting_hour);
				stmt.setInt(10, vehicleOperateInfo.waiting_minute);
				stmt.setInt(11, vehicleOperateInfo.waiting_second);
				stmt.setFloat(12, vehicleOperateInfo.totalIncome);
				stmt.setFloat(13, vehicleOperateInfo.workIncome);
				stmt.setDate(14, new java.sql.Date(vehicleOperateInfo.startTime.getTime()));
				stmt.setInt(15, vehicleOperateInfo.workTimes);
				stmt.setInt(16, vehicleOperateInfo.type_id);
				stmt.setInt(17, vehicleOperateInfo.isfit);
				stmt.setInt(18, vehicleOperateInfo.filter_type);
				stmt.addBatch();
				recordNum ++;
				if(recordNum%200==0){
					stmt.executeBatch();
				}
			}
			stmt.executeBatch();
			
			//不合格车辆的车牌号也保存
			
			stmt.executeBatch();
			conn.commit();
			int singlecount=0;
			int doublecount=0;
			//对电动车单双班状体位进行更新
			String singleElectric="select car_id, count(*) from v_ana_driver_info where car_id in (select dest_no from v_ana_dest_info where type_id=3 or type_id=7 ) group by car_id having count(*)=1";
			String doubleElectric="select car_id, count(*) from v_ana_driver_info where car_id in (select dest_no from v_ana_dest_info where type_id=3 or type_id=7 ) group by car_id having count(*)=2";
			StatementHandle stmtElectric=  conn.createStatement();
			Calendar calendar = Calendar.getInstance();//此时打印它获取的是系统当前时间
	        calendar.add(Calendar.DATE, -1);    //得到前一天
	        String  yestedayDate= new SimpleDateFormat("yyyy-MM-dd").format(calendar.getTime());
	        StatementHandle updateStmt=conn.prepareStatement("update ana_business_day_stat_sz_test set single_doubel_stat=?"
	        		+ " where dest_no=? and stat_time>=to_date(?,'yyyy-mm-dd')");
			ResultSet singleSet=stmtElectric.executeQuery(singleElectric);
			while (singleSet.next()) {
				String car_id=singleSet.getString("car_id");
				updateStmt.setInt(1, 1);//单班
				updateStmt.setString(2, car_id);
				updateStmt.setString(3, yestedayDate);
				updateStmt.addBatch();
				singlecount++;
			}
			updateStmt.executeBatch();
			System.out.println("count");
			ResultSet doubleSet=stmtElectric.executeQuery(doubleElectric);
			while (doubleSet.next()) {
				String car_id=doubleSet.getString("car_id");
				updateStmt.setInt(1, 2);//双班
				updateStmt.setString(2, car_id);
				updateStmt.setString(3, yestedayDate);
				updateStmt.addBatch();
				doublecount++;
			}
			updateStmt.executeBatch();
			conn.commit();
			System.out.println("singlecount:"+singlecount+"--doublecount:"+doublecount);
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
		System.out.println("Finish vehicle operate data NewVehicleOperateDataAnalysisForDaySZ:"+this.toString()+" recordNum="+recordNum);
	}
	
	public void endAnalysisOperation(AnalysisServer parentServer,InfoContainer statInfo) {
		return ;
	}
	
	private class VehicleOperateInfo
	{
		 public String dest_no;//车牌号
			public int    companyId;//公司id
			public String company_name;//公司名
			private int   type_id;//车型
			public int    workTimes = 0;//营运次数
			public float  total_distance = 0;//行驶距离
			public float  work_distance = 0;//营运距离
			public float  free_distance = 0;//空驶距离
			public int    waiting_hour = 0;//等候
			public int    waiting_minute = 0;
			public int    waiting_second = 0;
			public float  totalIncome = 0;//总金额
	        public float  workIncome = 0;//营运金额
			public float  fuelIncome = 0;//燃油金额
			public Date   startTime;//时间	
			public int isfit = 0 ;//车辆营运是否是否合格，默认值0合格，1不合格
			public int filter_type = 0;//过滤方案号
	}
	
	
}
