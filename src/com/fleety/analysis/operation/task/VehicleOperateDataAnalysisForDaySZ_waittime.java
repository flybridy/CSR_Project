package com.fleety.analysis.operation.task;

import java.sql.ResultSet;
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
import server.var.VarManageServer;

import com.fleety.analysis.AnalysisServer;
import com.fleety.analysis.operation.IOperationAnalysis;
import com.fleety.base.GeneralConst;
import com.fleety.base.InfoContainer;
import com.fleety.base.event.Event;
import com.fleety.server.event.GlobalEventCenter;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.sun.org.apache.bcel.internal.generic.Select;

public class VehicleOperateDataAnalysisForDaySZ_waittime implements IOperationAnalysis{
	
	private HashMap          vehicleMapping  = null;
	private HashMap          exceptionMapping  = null;
	private SimpleDateFormat sdf             = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private SimpleDateFormat sdf2            = new SimpleDateFormat("yyyy-MM-dd");
	private float            fuelSurcharges_h  = 0; //红车燃油附加费
	private float            fuelSurcharges_l  = 0; //绿车燃油附加费
	private float            fuelSurcharges_d  = 0;//电动车
	private float            fuelSurcharges_w  = 0;//无障碍燃油附加费
	
	List<filterInfo> list = new ArrayList<filterInfo>();
	
    private int total_car = 0;
    private int notfit_car = 0;
	
	@Override
	public boolean startAnalysisOperation(AnalysisServer parentServer,InfoContainer statInfo)
	{
		// 获取公司map和读取燃油附加费信息
		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
		Date eTime = statInfo.getDate(STAT_END_TIME_DATE);
		this.vehicleMapping = null;
		this.exceptionMapping = null;
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try{
			boolean bol = queryCondition();
			if(!bol){
				System.out.println("VehicleOperateDataAnalysisForDaySZ 燃油附加费条件不完整或过滤条件不存在");
				return false;
			}
			StatementHandle stmt = conn.createStatement();
			StringBuilder sb = new StringBuilder();
			sb.append("select count(*) as sum from ANA_BUSINESS_SZ_Waiting ")
			  .append(" where stat_time = to_date('").append(sdf2.format(sTime)).append("','yyyy-MM-dd')");
			System.out.println(sb.toString());
			ResultSet sets = stmt.executeQuery(sb.toString());
			if(sets.next()){
				int sum = sets.getInt("sum");
				if(sum == 0){
					this.vehicleMapping = new HashMap();
					this.exceptionMapping = new HashMap();				
	}		
			}
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
		
		if(this.vehicleMapping == null){
			System.out.println("VehicleOperateDataAnalysisForDaySZ_waittime Not Need Analysis:"+this.toString());
		}else{
			System.out.println("VehicleOperateDataAnalysisForDaySZ_waittime Start Analysis:"+this.toString());
		}
		
		return this.vehicleMapping != null;
	}

	private boolean queryCondition() throws Exception{
		this.list = new ArrayList<filterInfo>();
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			StatementHandle stmt = conn.createStatement();
			ResultSet sets = stmt.executeQuery("select * from business_screen_condition order by operator_time desc");
			while(sets.next()){
				filterInfo fi =new filterInfo();
				fi.con_one_dura = sets.getInt("con_one_dura");
				fi.con_one_mile = sets.getDouble("con_one_mile");
				fi.con_two_num_m = sets.getInt("con_two_num_m");
				fi.con_two_num_b = sets.getInt("con_two_num_b");
				fi.con_three_mony_b = sets.getDouble("con_three_mony_b");
				fi.con_three_mony_m = sets.getDouble("con_three_mony_m");
				fi.con_four_mile_m = sets.getDouble("con_four_mile_m");
				fi.con_four_mile_b = sets.getDouble("con_four_mile_b");
				fi.con_five_time1 = sets.getInt("con_five_time1");
				fi.con_five_time2 = sets.getInt("con_five_time2");
				fi.con_five_dura = sets.getInt("con_five_dura");
				fi.filter_type = sets.getInt("id");
				if(fi.filter_type!=0)//未获得数据时可能会把0加进去
				{
				list.add(fi);
				}
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
		this.vehicleMapping = new HashMap();
		this.exceptionMapping = new HashMap();	
		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
		Date eTime = statInfo.getDate(STAT_END_TIME_DATE);
		
		DbHandle conn = DbServer.getSingleInstance().getConnWithUseTime(0);
		
		try{
			StringBuilder sql = new StringBuilder();
			sql.append("select * from (select * from (")
			   .append(" select dispatch_car_no as plate_no,taxi_company as company_id,")
			   .append("  (distance+free_distance) as total_distance,")
			   .append("  decode(sign(distance),1,distance,-1,0,distance) as work_distance,")
			   .append("  free_distance,date_up,date_down,")
			   .append("  waiting_hour,")
			   .append("  waiting_minute,")
			   .append("  waiting_second,")
			   .append("  sum work_income")
			   .append(" from SINGLE_BUSINESS_DATA_BS ")
			   .append(" where dispatch_car_no is not null ")
			   .append("       and date_up >= to_date('").append(sdf.format(sTime)).append("','yyyy-mm-dd hh24:mi:ss')")
			   .append("       and date_up <= to_date('").append(sdf.format(eTime)).append("','yyyy-mm-dd hh24:mi:ss')")//shijian
			   .append(") a")
			   .append(" left join ")
			   .append(" (")
			   .append("   select TERM_ID,TERM_NAME as company_name from term")
			   .append(" ) d on a.company_id = d.TERM_ID ")
			   .append("left join ")
			   .append(" (select car_id,type_id from car) car on car.car_id=a.plate_no")
			   .append(" ) order by date_up");
			System.out.println("VehicleOperateDataAnalysisForDaySZ_waittime anasql:  "+sql.toString());
			StatementHandle stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql.toString());
			Calendar startTime = Calendar.getInstance();
			startTime.setTimeInMillis(sTime.getTime());
			//startTime.add(Calendar.DAY_OF_MONTH, -1);
			startTime.set(Calendar.HOUR_OF_DAY, list.get(j).con_five_time1);
			startTime.set(Calendar.MINUTE, 0);
			startTime.set(Calendar.SECOND, 0);
			startTime.set(Calendar.MILLISECOND, 0);
			Calendar endTime = Calendar.getInstance();
			endTime.setTimeInMillis(sTime.getTime());
			//endTime.add(Calendar.DAY_OF_MONTH, -1);
			endTime.set(Calendar.HOUR_OF_DAY, (list.get(j).con_five_time2-1));
			endTime.set(Calendar.MINUTE, 23);
			endTime.set(Calendar.SECOND, 59);
			endTime.set(Calendar.MILLISECOND, 59);
			VehicleOperateInfo vInfo =  null;
			while(rs.next())
			{
				String plateNo = rs.getString("plate_no");
				if(vehicleMapping.containsKey(plateNo)){
					vInfo = (VehicleOperateInfo)vehicleMapping.get(plateNo);
				}else {
					vInfo = new VehicleOperateInfo();
					vehicleMapping.put(plateNo, vInfo);
					
				}
				vInfo.startTime = sTime;
				vInfo.plateNo = plateNo;
				vInfo.companyId = rs.getInt("company_id");
				vInfo.companyName = rs.getString("company_name");
				vInfo.workTimes++;// 营运总次数
				//kaka 可以增加车辆类型？
				
				
				long dateUp = rs.getTimestamp("date_up").getTime();
				long dateDown = rs.getTimestamp("date_down").getTime();
				float workDistance = rs.getFloat("work_distance");
				boolean isAdd = false;
				//条件一
				if(dateDown-dateUp>=(list.get(j).con_one_dura*60*1000)&&workDistance>=list.get(j).con_one_mile){
					vInfo.conditionOne++;
					isAdd = true;
				}else {
					isAdd = false;
				}
				//条件五
				if(vInfo.dateUp>=startTime.getTimeInMillis()&&vInfo.dateUp<=endTime.getTimeInMillis()
						&&dateUp>=startTime.getTimeInMillis()&&dateUp<=endTime.getTimeInMillis()
						&&dateUp-vInfo.dateDown<=list.get(j).con_five_dura*60*60*1000){
					vInfo.conditionFive++;
					isAdd = true;
				}else {
					isAdd = false;
				}
				if(rs.getFloat("total_distance")<=list.get(j).con_four_mile_b){
					vInfo.conditionFour++;
					isAdd = true;
				}else {
					isAdd = false;
				}
				
				vInfo.dateUp = dateUp;
				vInfo.dateDown = dateDown;
				if(isAdd){
//					System.out.println("VehicleOperateDataAnalysisForDaySZ_waittime isadd "+plateNo);
					vInfo.totalDistance += rs.getFloat("total_distance");// 行驶里程
					vInfo.workDistance += workDistance;// 营运里程
				    vInfo.freeDistance += rs.getFloat("free_distance");// 空驶里程
					int curtime = rs.getInt("waiting_hour")*3600+ rs.getInt("waiting_minute")*60+rs.getInt("waiting_second");// 低速等候时间(时)
					vInfo.workIncome += rs.getFloat("work_income");//计价收入
					vInfo.typeId = rs.getInt("type_id");//计价收入
					vInfo.filter_type = list.get(j).filter_type;
					vInfo.condition ++ ;
					vInfo.waitingSecond+=curtime;
					int res=this.DateJudge(dateUp);
					if(res==1){
						vInfo.waiting_monring+=curtime;
					}else if(res==2){
						vInfo.waiting_night+=curtime;
					}
					vInfo.worktime+=((dateDown-dateUp)>0?(dateDown-dateUp):0);
					
					
				}
				
			}
			
			System.out.println("VehicleOperateDataAnalysisForDaySZ_waittime vehicleMapping00:"+vehicleMapping.size());
			
			List tempList = new ArrayList();
			Map<String,VehicleOperateInfo> tempMap = new HashMap<String, VehicleOperateInfo>();
			for (Iterator iterator = vehicleMapping.keySet().iterator(); iterator.hasNext();) {
				String plateNo = (String) iterator.next();
				vInfo = (VehicleOperateInfo)vehicleMapping.get(plateNo);
				if(vInfo.typeId==1){
					vInfo.fuelIncome += (fuelSurcharges_h * vInfo.condition*0.9);// 燃油附加收入
				}else if(vInfo.typeId==2){
					vInfo.fuelIncome += (fuelSurcharges_l * vInfo.condition*0.9);// 燃油附加收入
				}else if(vInfo.typeId==4){
					vInfo.fuelIncome += (fuelSurcharges_w * vInfo.condition*0.9);// 燃油附加收入
				}
				vInfo.totalIncome = vInfo.totalIncome + vInfo.workIncome + vInfo.fuelIncome;// 总收入
//				vehicleMapping.put(plateNo, vInfo);
				//条件一
				boolean is_exception = false;
				if(vInfo.conditionOne<=0){
//					tempList.add(plateNo);
					is_exception = true;
				}
				//条件五
				else if(vInfo.conditionFive<=0){
//					tempList.add(plateNo);
					is_exception = true;
				}
				//条件二
				else if(!(vInfo.workTimes>=list.get(j).con_two_num_m&&vInfo.workTimes<=list.get(j).con_two_num_b)){
//					tempList.add(plateNo);
					is_exception = true;
				}
				//条件三
				else if(!(vInfo.workIncome>=list.get(j).con_three_mony_m&&vInfo.workIncome<=list.get(j).con_three_mony_b)){
//					tempList.add(plateNo);
					is_exception = true;
				}
				//条件四
				else if(vInfo.totalDistance<list.get(j).con_four_mile_m||vInfo.conditionFour<=0){
//					tempList.add(plateNo);
					is_exception = true;
				}
				if(is_exception){
					vInfo.isfit=1;
					vInfo.filter_type = list.get(j).filter_type;
					exceptionMapping.put(plateNo, vInfo);
				}
			}
			String temp = null;
			total_car= vehicleMapping.keySet().size();
			
			Iterator<String> it = exceptionMapping.keySet().iterator();
			notfit_car = exceptionMapping.keySet().size();
			
			System.out.println("VehicleOperateDataAnalysisForDaySZ_waittime vehicleMapping:"+vehicleMapping.size()+",exceptionMapping:"+exceptionMapping.size()+",");
			
			while(it.hasNext()){
				temp = it.next();
//				System.out.println("VehicleOperateDataAnalysisForDaySZ_waittime exceptionMapping remove:"+temp);
				vehicleMapping.remove(temp);
			}
			
			endAnalysisOperationSaveData(parentServer,statInfo);
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
		}
	}
	
	public void endAnalysisOperationSaveData(AnalysisServer parentServer,InfoContainer statInfo) {
		
		System.out.println("VehicleOperateDataAnalysisForDaySZ_waittime 准备插入数据量"+vehicleMapping.size());
		if(this.vehicleMapping == null){ 
			return ;
		}
		int recordNum = 0;
		String plateNo = "";
		VehicleOperateInfo vehicleOperateInfo;
		Date sDate = statInfo.getDate(STAT_START_TIME_DATE);
		DbHandle conn = DbServer.getSingleInstance().getConnWithUseTime(0);
		try{
			conn.setAutoCommit(false);
			StatementHandle stmt = conn
					.prepareStatement("insert into ANA_BUSINESS_SZ_Waiting(id,dest_no,company_id,company_name,STAT_TIME," +
							"waiting_second," +
							"type_id,is_fit,filter_type,waiting_second_monring,waiting_second_night,worktime,freetime,business_money,business_money_t,waitting_money,waitting_money_morning,waitting_money_night) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
			for(Iterator itr = this.vehicleMapping.keySet().iterator();itr.hasNext();){
				plateNo = (String)itr.next();
				vehicleOperateInfo = (VehicleOperateInfo)this.vehicleMapping.get(plateNo);
				stmt.setInt(1, (int)DbServer.getSingleInstance().getAvaliableId(conn, "ANA_BUSINESS_SZ_Waiting", "id"));
				stmt.setString(2, vehicleOperateInfo.plateNo);
				stmt.setInt(3, vehicleOperateInfo.companyId);
				stmt.setString(4, vehicleOperateInfo.companyName);
				stmt.setDate(5, new java.sql.Date(vehicleOperateInfo.startTime.getTime()));
				stmt.setInt(6, vehicleOperateInfo.waitingSecond);
				stmt.setInt(7, vehicleOperateInfo.typeId);
				stmt.setInt(8, vehicleOperateInfo.isfit);
				stmt.setInt(9, vehicleOperateInfo.filter_type);
				stmt.setInt(10, vehicleOperateInfo.waiting_monring);
				stmt.setInt(11, vehicleOperateInfo.waiting_night);
				stmt.setInt(12, (int) vehicleOperateInfo.worktime);
				stmt.setInt(13, (int) (24*60*60-vehicleOperateInfo.worktime));
				
				stmt.setFloat(14, vehicleOperateInfo.workIncome);
				stmt.setFloat(15, vehicleOperateInfo.totalIncome);
				
				stmt.setFloat(16, (float) (vehicleOperateInfo.waitingSecond/45*0.6));
				stmt.setFloat(17, (float) (vehicleOperateInfo.waiting_monring/45*0.6));
				stmt.setFloat(18, (float) (vehicleOperateInfo.waiting_night/45*0.6));

				stmt.addBatch();
				recordNum ++;
				if(recordNum%200==0){
					stmt.executeBatch();
				}
			}
			stmt.executeBatch();
			conn.commit();
		}catch(Exception e){
			System.out.println("VehicleOperateDataAnalysisForDaySZ_waittime 异常");
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
	
	public void endAnalysisOperation(AnalysisServer parentServer,InfoContainer statInfo) {
		return ;
	}
	
	private class VehicleOperateInfo
	{
		public String plateNo;
		public int    companyId;
		public String companyName;
		private int   typeId;
		public int    workTimes = 0;
		public float  totalDistance = 0;
		public float  workDistance = 0;
		public float  freeDistance = 0;
		public int    waitingHour = 0;
		public int    waitingMinute = 0;
		public int    waitingSecond = 0;
		public float  totalIncome = 0;
		public float  workIncome = 0;
		public float  fuelIncome = 0;
		public Date   startTime;
		public long dateUp = 0;
		public long dateDown = 0;
		public int conditionOne = 0;
		public int conditionFive = 0;
		public int conditionFour=0;
		public int condition = 0;
		public int isfit = 0 ;//车辆营运是否是否合格，默认值0合格，1不合格
		public int filter_type = 0;//过滤方案号
		public int waiting_monring = 0;//过滤方案号
		public int waiting_night = 0;//过滤方案号
		
		public long worktime = 0;//营运时长，单位秒
		public long freetime = 0;//空时时长，单位秒
		
		
		
	}
	
	private class filterInfo{
		public int con_one_dura = 0;//单笔营运时间
		public double con_one_mile = 0;//单笔营运里程
		public int con_two_num_m = 0;//营运笔数大于数
		public int con_two_num_b = 0;//-营运笔数小于数
		public double con_three_mony_b = 0;//营运金额小于数
		public double con_three_mony_m = 0;//营运金额大于数
		public double con_four_mile_m = 0;//总公里小于数
		public double con_four_mile_b = 0;//总公里小于数
		public int con_five_time1 = 0;//筛选开始时间
		public int con_five_time2 = 0;//筛选结束时间
		public int con_five_dura = 0;//筛选2笔连续时长
		public int filter_type = 0;//过滤方案号
	}
	private int DateJudge(long da){
		 int res=0;
		 Calendar ca= Calendar.getInstance();
		 ca.setTimeInMillis(da);
		 int hour  = ca.get(Calendar.HOUR_OF_DAY);
		 int minute = ca.get(Calendar.MINUTE);
		 if(hour>=7 && hour<=9){
			 return 1;//早高峰
		 }else if(hour==17&&minute>=30){
				 return 2;//晚高峰
		 }else if (hour==18){
			 return 2;//晚高峰
		 }else if(hour==19&&minute<=30){
			 return 2;//晚高峰
		 }
		return res;
	}
}
