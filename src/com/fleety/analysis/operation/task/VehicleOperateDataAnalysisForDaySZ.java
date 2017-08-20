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

public class VehicleOperateDataAnalysisForDaySZ implements IOperationAnalysis{
	
	private HashMap          vehicleMapping  = null;
	private HashMap          exceptionMapping  = null;
	private SimpleDateFormat sdf             = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private SimpleDateFormat sdf2            = new SimpleDateFormat("yyyy-MM-dd");
	private float            fuelSurcharges_h  = 0; //红车燃油附加费
	private float            fuelSurcharges_l  = 0; //绿车燃油附加费
	private float            fuelSurcharges_d  = 0;//电动车
	private float            fuelSurcharges_w  = 0;//无障碍燃油附加费
	
	List<filterInfo> list = null;
	
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
			sb.append("select count(*) as sum from ana_business_day_stat_sz ")
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
			System.out.println("Not Need Analysis:"+this.toString());
		}else{
			System.out.println("Start Analysis:"+this.toString());
		}
		
		return this.vehicleMapping != null;
	}

	private boolean queryCondition() throws Exception{
		this.list = new ArrayList<filterInfo>();
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			StatementHandle stmt = conn.createStatement();
			ResultSet sets = stmt.executeQuery("select * from business_screen_condition  order by operator_time desc");
			while(sets.next()){
				filterInfo fi =new filterInfo();
				fi.con_one_dura = sets.getInt("con_one_dura");//单笔营运时间大于数 1
				fi.con_one_mile = sets.getDouble("con_one_mile");//单笔营运里程小于数 0.3
				fi.con_two_num_m = sets.getInt("con_two_num_m");// 营运笔数小于数 15
				fi.con_two_num_b = sets.getInt("con_two_num_b");// 营运笔数大于数 85
				fi.con_three_mony_b = sets.getDouble("con_three_mony_b");// 单车一天营运金额大于数 1500
				fi.con_three_mony_m = sets.getDouble("con_three_mony_m");//单车一天营运金额小于数 500
				fi.con_four_mile_m = sets.getDouble("con_four_mile_m");// 单车一天总公里小于数 绿的170，红的190
				fi.con_four_mile_b = sets.getDouble("con_four_mile_b");// 单笔数据营运里程大于数 200
				fi.con_five_time1 = sets.getInt("con_five_time1");//筛选开始时间
				fi.con_five_time2 = sets.getInt("con_five_time2");//筛选结束时间
				fi.con_five_dura = sets.getInt("con_five_dura");//筛选2笔连续时长超过4小时
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
		System.out.println("sTime:"+sdf.format(sTime)+" eTime:"+sdf.format(sTime));
		DbHandle conn = DbServer.getSingleInstance().getConn();
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
			System.out.println(sql.toString());
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
			int count=0;
			int record=0;
			while(rs.next())
			{
				count++;
				String plateNo = rs.getString("plate_no");
				if(vehicleMapping.containsKey(plateNo)){
					vInfo = (VehicleOperateInfo)vehicleMapping.get(plateNo);
				}else {
					vInfo = new VehicleOperateInfo();
				}
				vInfo.startTime = sTime;
				vInfo.plateNo = plateNo;
				vInfo.companyId = rs.getInt("company_id");
				vInfo.companyName = rs.getString("company_name");
				vInfo.workTimes++;// 营运次数
				//kaka 可以增加车辆类型？
				
				
				long dateUp = rs.getTimestamp("date_up").getTime();
				long dateDown = rs.getTimestamp("date_down").getTime();
				float workDistance = rs.getFloat("work_distance");
				boolean isAdd = false;//false不合格，true合格
				//条件一：//      单笔营运时间大于数 1  &&      单笔营运距离大于 0.3
				if(dateDown-dateUp>=(list.get(j).con_one_dura*60*1000)&&workDistance>=list.get(j).con_one_mile){
					vInfo.conditionOne++;
					isAdd = true;
				}else {
					isAdd = false;
				}
				//条件五  上车时间在统计时间的24小时之内;两笔营运数据之间相隔con_five_dura 时间
				if(vInfo.dateUp==0L)
				{
					vInfo.conditionFive++;
					isAdd = true;
				}
				else if(vInfo.dateUp>=startTime.getTimeInMillis()&&vInfo.dateUp<=endTime.getTimeInMillis()
						&&dateUp>=startTime.getTimeInMillis()&&dateUp<=endTime.getTimeInMillis()
						&&dateUp-vInfo.dateDown<=list.get(j).con_five_dura*60*60*1000){
					vInfo.conditionFive++;
					isAdd = true;
				}else {
					isAdd = false;
				}
				//单笔公里大于数con_four_mile_b，单笔小于200公里合格
				if(rs.getFloat("total_distance")<=list.get(j).con_four_mile_b){
					vInfo.conditionFour++;
					isAdd = true;
				}else {
					isAdd = false;
				}
				vInfo.dateUp = dateUp;
				vInfo.dateDown = dateDown;
				if(isAdd){
					record++;
					vInfo.totalDistance += rs.getFloat("total_distance");// 行驶里程
					vInfo.workDistance += workDistance;// 营运里程
				    vInfo.freeDistance += rs.getFloat("free_distance");// 空驶里程
					vInfo.waitingHour += rs.getInt("waiting_hour");// 低速等候时间(时)
					vInfo.waitingMinute += rs.getInt("waiting_minute");// 低速等候时间(分)
					vInfo.waitingSecond += rs.getInt("waiting_second");// 低速等候时间(秒)
					vInfo.workIncome += rs.getFloat("work_income");//计价收入
					vInfo.typeId = rs.getInt("type_id");//
					vInfo.filter_type = list.get(j).filter_type;
					vInfo.condition ++ ;//合格营运次数
				}
				vehicleMapping.put(plateNo, vInfo);
			}
			System.out.println("变更原始数据中查询需要分析:"+count+" 条数据.isAdd "+record+" 条数据。");
			List tempList = new ArrayList();
			Map<String,VehicleOperateInfo> tempMap = new HashMap<String, VehicleOperateInfo>();
			for (Iterator iterator = vehicleMapping.keySet().iterator(); iterator.hasNext();) {
				String plateNo = (String) iterator.next();
				vInfo = (VehicleOperateInfo)vehicleMapping.get(plateNo);
				if(vInfo.typeId==1){//燃油附加费*合格营运次数*百分比
					vInfo.fuelIncome += (fuelSurcharges_h * vInfo.condition*0.9);// 燃油附加收入
				}else if(vInfo.typeId==2||vInfo.typeId==9){//新增调价绿的。
					vInfo.fuelIncome += (fuelSurcharges_l * vInfo.condition*0.9);// 燃油附加收入
				}else if(vInfo.typeId==4){
					vInfo.fuelIncome += (fuelSurcharges_w * vInfo.condition*0.9);// 燃油附加收入
				}
				vInfo.totalIncome = vInfo.totalIncome + vInfo.workIncome + vInfo.fuelIncome;// 总收入
				vehicleMapping.put(plateNo, vInfo);
				//条件一
				boolean is_exception = false;
				if(vInfo.conditionOne<=0){

					is_exception = true;
				}
				//条件五
				else if(vInfo.conditionFive<=0){
					is_exception = true;
				}
				//条件二 单天营运次数大于 con_two_num_m 85不合格 ，营运次数小于con_two_num_b 15不合格
				else if(!(vInfo.workTimes>=list.get(j).con_two_num_m&&vInfo.workTimes<=list.get(j).con_two_num_b)){

					is_exception = true;
				}
				//条件三 单天营运收入大于 con_three_mony_b 
				else if(!(vInfo.workIncome>=list.get(j).con_three_mony_m&&vInfo.workIncome<=list.get(j).con_three_mony_b)){

					is_exception = true;
				}
				//条件四 单天营运距离 大于 绿的170，红的190。
				else if(vInfo.conditionFour<=0){				
					is_exception = true;
				}
				else if(vInfo.totalDistance<170&&(vInfo.typeId==2||vInfo.typeId==9)){
					is_exception = true;
				}
				else if(vInfo.totalDistance<190&&(vInfo.typeId!=2||vInfo.typeId!=9)){
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
			while(it.hasNext()){
				temp = it.next();
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
			StatementHandle stmt = conn
					.prepareStatement("insert into ana_business_day_stat_sz(id,dest_no,company_id,company_name," +
							"business_num,mile,business_mile,free_mile,waiting_hour,waiting_minute,waiting_second," +
							"business_money_t,business_money,stat_time,condition,type_id,is_fit,filter_type) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
			for(Iterator itr = this.vehicleMapping.keySet().iterator();itr.hasNext();){
				plateNo = (String)itr.next();
				vehicleOperateInfo = (VehicleOperateInfo)this.vehicleMapping.get(plateNo);
				stmt.setInt(1, (int)DbServer.getSingleInstance().getAvaliableId(conn, "ana_business_day_stat_sz", "id"));
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
				stmt.setFloat(12, vehicleOperateInfo.totalIncome);
				stmt.setFloat(13, vehicleOperateInfo.workIncome);
				stmt.setDate(14, new java.sql.Date(vehicleOperateInfo.startTime.getTime()));
				stmt.setInt(15, vehicleOperateInfo.condition);
				stmt.setInt(16, vehicleOperateInfo.typeId);
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
			for(Iterator itr = this.exceptionMapping.keySet().iterator();itr.hasNext();){
				plateNo = (String)itr.next();
				vehicleOperateInfo = (VehicleOperateInfo)this.exceptionMapping.get(plateNo);
				stmt.setInt(1, (int)DbServer.getSingleInstance().getAvaliableId(conn, "ana_business_day_stat_sz", "id"));
				stmt.setString(2, vehicleOperateInfo.plateNo);
				stmt.setInt(3, vehicleOperateInfo.companyId);
				stmt.setString(4, vehicleOperateInfo.companyName);
				stmt.setInt(5, 0);
				stmt.setFloat(6, 0);
				stmt.setFloat(7, 0);
				stmt.setFloat(8, 0);
				stmt.setInt(9, 0);
				stmt.setInt(10,0);
				stmt.setInt(11, 0);
				stmt.setFloat(12, 0);
				stmt.setFloat(13, 0);
				stmt.setDate(14, new java.sql.Date(vehicleOperateInfo.startTime.getTime()));
				stmt.setInt(15, 0);
				stmt.setInt(16, vehicleOperateInfo.typeId);
				stmt.setInt(17, vehicleOperateInfo.isfit);
				stmt.setInt(18, vehicleOperateInfo.filter_type);
				stmt.addBatch();
				recordNum ++;
				if(recordNum%200==0){
					stmt.executeBatch();
				}
			}
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
	        StatementHandle updateStmt=conn.prepareStatement("update ana_business_day_stat_sz set single_doubel_stat=?"
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
		System.out.println("Finish vehicle operate data Analysis:"+this.toString()+" recordNum="+recordNum);
	}
	
	public void endAnalysisOperation(AnalysisServer parentServer,InfoContainer statInfo) {
		return ;
	}
	
	private class VehicleOperateInfo
	{
		public String plateNo;//车牌号
		public int    companyId;//公司id
		public String companyName;//公司名
		private int   typeId;//车型
		public int    workTimes = 0;//营运次数
		public float  totalDistance = 0;//行驶距离
		public float  workDistance = 0;//营运距离
		public float  freeDistance = 0;//空驶距离
		public int    waitingHour = 0;//等候
		public int    waitingMinute = 0;
		public int    waitingSecond = 0;
		public float  totalIncome = 0;//总金额
		public float  workIncome = 0;//营运金额
		public float  fuelIncome = 0;//燃油金额
		public Date   startTime;//时间
		public long dateUp = 0;
		public long dateDown = 0;
		public int conditionOne = 0;
		public int conditionFive = 0;
		public int conditionFour=0;
		public int condition = 0;
		public int isfit = 0 ;//车辆营运是否是否合格，默认值0合格，1不合格
		public int filter_type = 0;//过滤方案号
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
}
