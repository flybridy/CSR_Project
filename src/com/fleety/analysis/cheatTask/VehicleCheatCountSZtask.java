package com.fleety.analysis.cheatTask;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;

import server.db.DbServer;

import com.fleety.analysis.AnalysisServer;
import com.fleety.analysis.operation.IOperationAnalysis;
import com.fleety.base.InfoContainer;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;

public class VehicleCheatCountSZtask implements IOperationAnalysis{
	
	private HashMap<String,StandardInfo>          StandardInfoMapping  = null;
	private HashMap<String,CheatAnaResInfo>          ResIfoMapping  = null;
	private SimpleDateFormat sdf             = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private SimpleDateFormat sdf2            = new SimpleDateFormat("yyyy-MM-dd");
	
	
	
   
	@Override
	public boolean startAnalysisOperation(AnalysisServer parentServer,InfoContainer statInfo)
	{
		// 获取公司map和读取燃油附加费信息
		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
		Date eTime = statInfo.getDate(STAT_END_TIME_DATE);
		this.StandardInfoMapping = null;
		this.ResIfoMapping = null;
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try{
			
			StatementHandle stmt = conn.createStatement();
			StringBuilder sb = new StringBuilder();
			sb.append("select count(*) as sum from cheat_Standard_date")
			  .append(" where record_date = to_date('").append(sdf2.format(sTime)).append("','yyyy-MM-dd')");
			System.out.println(sb.toString());
			ResultSet sets = stmt.executeQuery(sb.toString());
			if(sets.next()){
				int sum = sets.getInt("sum");
				if(sum == 0){
					this.ResIfoMapping = new HashMap();				
					this.StandardInfoMapping = new HashMap();				
	}		
			}
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
		
		if(this.StandardInfoMapping == null){
			System.out.println("VehicleCheatCountSZtask Not Need Analysis:"+this.toString());
		}else{
			System.out.println("VehicleCheatCountSZtask Start Analysis:"+this.toString());
		}
		
		return this.StandardInfoMapping != null;
	}

	
	
	
	@Override
	public void analysisDestOperation (AnalysisServer parentServer, InfoContainer statInfo)
	{   
		
		this.StandardInfoMapping = new HashMap();
		this.ResIfoMapping = new HashMap();	
		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
		Date eTime = statInfo.getDate(STAT_END_TIME_DATE);
		
		DbHandle conn = DbServer.getSingleInstance().getConnWithUseTime(0);
		try{
		//获取标准sql 对误差率小于百分之五的数据统计。得到白天或者晚上，各车型，在各营运时长下的，营运次数，最大金额，最大里程。
		StringBuilder sql1 = new StringBuilder();
		 sql1.append("select business_time,count(*) as times,  max(DISTANCE) as max_distince,max(sum) as max_sum,type_id as car_type,hour_type from (select car_no, sum,DISTANCE,case when to_char(DATE_UP,'hh24')>6 and to_char(DATE_UP,'hh24')<=23 then 1 else 0 end hour_type, round((DATE_DOWN - DATE_UP)*60*24,0) as business_time,b.type_id from GPS_BUSINESS_ANALYSIS a left join car b on a.car_no=b.car_id where  DISTANCE >0 ")
	     .append(" and date_up >= to_date('").append(sdf.format(sTime)).append("','yyyy-mm-dd hh24:mi:ss')")
	     .append(" and date_up <= to_date('").append(sdf.format(eTime)).append("','yyyy-mm-dd hh24:mi:ss')")	 
		 .append("and   abs( round( (DISTANCE-gps_distance)/ DISTANCE, 2)) < 0.05 ) group by business_time,type_id,hour_type");
		 System.out.println("统计作弊标准数据sql"+sql1.toString());
		 
		
		 StatementHandle stmt1=conn.prepareStatement(sql1.toString());
		 ResultSet res1=stmt1.executeQuery();
		 StandardInfo standInfo;
		 while(res1.next()){
			 double operator_time=res1.getDouble("business_time");
			 int hour_type=res1.getInt("hour_type");
			 int car_type=res1.getInt("car_type");
			 
			 String key=car_type+"_"+hour_type+"_"+operator_time;
			 if(!StandardInfoMapping.containsKey(key)){
				 standInfo=new StandardInfo();
				 standInfo.setOperator_time(operator_time);
				 standInfo.setHour_type(hour_type);
				 standInfo.setCar_type(car_type);
				 standInfo.setMax_DISTANCE(res1.getDouble("max_distince"));
				 standInfo.setMax_sum(res1.getDouble("max_sum"));
				 standInfo.setOperator_nums(res1.getDouble("times"));
				 StandardInfoMapping.put(key, standInfo);
			 }
		 }
		 res1.close();
		 System.out.println("作弊标准数据条数"+StandardInfoMapping.size());
		 
		 
		 //获取营运数据 将营运数据
		 /*以天为单位，遍历所有的营运数据。逐条获取营运时长，车型，白班晚班信息。然后去标准数据中寻找对应数据。如：当前营运数据营运20分钟，里程30公里，金额
		 45元。标准数据中营运时长20分钟，最大里程为28公里，最大金额50块。则判定该条营运数据不合理。（必须同时在两个最大标准之内才合格）。反之则判定合理；如
		 果标准数据中没有营运时长20分钟的，则无标准，跳如下一条数据判断。（sum = 0 distance = 0  sum >200 distance >500的记录就不分析了）*/
		 StringBuilder sql2 = new StringBuilder();
		 sql2.append("select s.id,s.car_no,round((s.DATE_DOWN - s.DATE_UP)*60*24,0) operator_time,s.distance,s.sum,case when to_char(DATE_UP,'hh24')>6 and to_char(DATE_UP,'hh24')<=23 then 1 else 0 end hour_type,to_char(s.date_up,'yyyy-mm-dd hh24:mi:ss') date_up,to_char(s.date_down,'yyyy-mm-dd hh24:mi:ss') date_down,c.type_id car_type from SINGLE_BUSINESS_DATA_BS s left join car c on s.car_no=c.car_id where 1=1 " )
		 .append(" and s.date_up >= to_date('").append(sdf.format(sTime)).append("','yyyy-mm-dd hh24:mi:ss')")
			   .append(" and s.date_up <= to_date('").append(sdf.format(eTime)).append("','yyyy-mm-dd hh24:mi:ss')");
		 System.out.println("sql2:"+sql2.toString());
		 StatementHandle stmt2=conn.prepareStatement(sql2.toString());
		 ResultSet res2=stmt2.executeQuery();
		 CheatAnaResInfo cheatRes;
		int count=0;
		int ninght=0;
		int day=0;
			while(res2.next()){
				 double operator_time=res2.getDouble("operator_time");
				 int hour_type=res2.getInt("hour_type");
				 int car_type=res2.getInt("car_type");
				 String key=car_type+"_"+hour_type+"_"+operator_time;
				 if(StandardInfoMapping.containsKey(key)){
					 count++;
					 cheatRes=new CheatAnaResInfo();
					 cheatRes.setOp_id(res2.getLong(1));
					 cheatRes.setCar_no(res2.getString(2));
					 cheatRes.setOperator_time(res2.getDouble(3));
					 cheatRes.setDISTANCE(res2.getDouble(4));
					 cheatRes.setSum(res2.getDouble(5));
					 cheatRes.setHour_type(res2.getInt(6));
					 if(res2.getInt(6)==0){
						 ninght++;
					 }else{
						 day++;
					 }
					 cheatRes.setDate_up(res2.getString(7));
					 cheatRes.setDate_down(res2.getString(8));
					 cheatRes.setCar_type(res2.getInt(9));
					 standInfo=StandardInfoMapping.get(key);
					 cheatRes.setMax__distance(standInfo.getMax_DISTANCE());
					 cheatRes.setMax__sum(standInfo.getMax_sum());
					 cheatRes.setOperator_nums(standInfo.getOperator_nums());
					if(res2.getDouble(4)>standInfo.getMax_DISTANCE()||res2.getDouble(5)>standInfo.getMax_sum()){
						 cheatRes.setIs_fit(0);
					}else{
						 cheatRes.setIs_fit(1);
					}
					 ResIfoMapping.put(res2.getString(1), cheatRes);
				 }
				 
			}
			res2.close();
			System.out.println("VehicleCheatCountSZtask anasql: 判断分析了 "+count+" 条营运数据 day:+"+day+" night:"+ninght+" "+sql2.toString());
			
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
		
	}

	@Override
	public void endAnalysisOperation(AnalysisServer parentServer,
			InfoContainer statInfo) {
		System.out.println("VehicleCheatCountSZtask 准备插入数据量"+ResIfoMapping.size());
		if(this.ResIfoMapping == null||StandardInfoMapping==null){ 
			return ;
		}
		int recordNum = 0;
		CheatAnaResInfo cheatRes;
		Date sDate = statInfo.getDate(STAT_START_TIME_DATE);
		DbHandle conn = DbServer.getSingleInstance().getConnWithUseTime(0);
		DbHandle conn2 = DbServer.getSingleInstance().getConnWithUseTime(0);
		try{
			
			conn2.setAutoCommit(false);
			StandardInfo standInfo;
			//保存标准数据
			StatementHandle stmt2=conn2.prepareStatement("insert into cheat_Standard_date(id,car_type,max_DISTANCE,operator_time,hour_type,operator_nums,record_date,max_sum)values(?,?,?,?,?,?,?,?)");
			for(Iterator itr = this.StandardInfoMapping.keySet().iterator();itr.hasNext();){
				standInfo =StandardInfoMapping.get(itr.next());
				stmt2.setInt(1, (int)DbServer.getSingleInstance().getAvaliableId(conn, "cheat_Standard_date", "id"));
				stmt2.setInt(2, standInfo.getCar_type());
				stmt2.setDouble(3, standInfo.getMax_DISTANCE());
				stmt2.setDouble(4, standInfo.getOperator_time());
				stmt2.setInt(5, standInfo.getHour_type());
				stmt2.setDouble(6, standInfo.getOperator_nums());
				stmt2.setTimestamp(7, new Timestamp(new Date().getTime()));
				stmt2.setDouble(8, standInfo.getMax_sum());
				stmt2.addBatch();
				if(recordNum%200==0){
					stmt2.executeBatch();
				}
			}
			stmt2.executeBatch();
			conn2.commit();
			
			//保存分析结果
			conn.setAutoCommit(false);
			StatementHandle stmt = conn
					.prepareStatement("insert into CheatAna_Record (id,op_id,car_no,car_type,date_up,date_down,distance,sum,operator_time,hour_type,max_distance,max_sum,operator_nums,is_fit,record_date) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
			for(Iterator itr = this.ResIfoMapping.keySet().iterator();itr.hasNext();){
				cheatRes =ResIfoMapping.get(itr.next());
				
				stmt.setInt(1, (int)DbServer.getSingleInstance().getAvaliableId(conn, "CheatAna_Record", "id"));
				stmt.setLong(2, cheatRes.getOp_id());
				stmt.setString(3, cheatRes.getCar_no());
				stmt.setInt(4, cheatRes.getCar_type());
				stmt.setTimestamp(5,  new Timestamp(sdf.parse(cheatRes.getDate_up()).getTime()));
				stmt.setTimestamp(6, new Timestamp(sdf.parse(cheatRes.getDate_down()).getTime()));
				stmt.setDouble(7, cheatRes.getDISTANCE());
				stmt.setDouble(8, cheatRes.getSum());
				stmt.setDouble(9, cheatRes.getOperator_time());
				stmt.setInt(10, cheatRes.getHour_type());
				stmt.setDouble(11, cheatRes.getMax__distance());
				stmt.setDouble(12, cheatRes.getMax__sum());
				stmt.setDouble(13, cheatRes.getOperator_nums());
				stmt.setDouble(14, cheatRes.getIs_fit());
				stmt.setTimestamp(15, new Timestamp(new Date().getTime()));
				stmt.addBatch();
				recordNum ++;
				if(recordNum%200==0){
					stmt.executeBatch();
				}
			}
			stmt.executeBatch();
			conn.commit();
		}catch(Exception e){
			System.out.println("VehicleCheatCountSZtask数据保存 异常");
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
			DbServer.getSingleInstance().releaseConn(conn2);
			DbServer.getSingleInstance().releaseConn(conn);
		}
		System.out.println("Finish VehicleCheatCountSZtask Analysis:"+this.toString()+" recordNum="+recordNum);
	}
	

	
}
