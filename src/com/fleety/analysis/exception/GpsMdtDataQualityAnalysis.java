package com.fleety.analysis.exception;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;

import server.db.DbServer;

import com.fleety.analysis.AnalysisServer;
import com.fleety.analysis.feedback.DestInfo;
import com.fleety.base.GeneralConst;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.fleety.util.pool.thread.BasicTask;
import com.fleety.util.pool.timer.FleetyTimerTask;

public class GpsMdtDataQualityAnalysis  extends AnalysisServer {
	private TimerTask task = null;

	public boolean startServer() {
		super.startServer();
		if(!this.isRunning()){
			return this.isRunning();
		}		
		
		int hour = this.getIntegerPara("hour").intValue();
		int minute = this.getIntegerPara("minute").intValue();
		
		Calendar cal = this.getNextExecCalendar(hour, minute);
		if(cal.get(Calendar.DAY_OF_MONTH) != Calendar.getInstance().get(Calendar.DAY_OF_MONTH)){
			this.scheduleTask(new TimerTask(), 500);
		}
		long delay = cal.getTimeInMillis() - System.currentTimeMillis();
		this.isRunning = this.scheduleTask(this.task = new TimerTask(), delay, GeneralConst.ONE_DAY_TIME);
		
		return this.isRunning();
	}
	
	public void stopServer(){
		if(this.task != null){
			this.task.cancel();
		}
		super.stopServer();
	}

	private void executeTask(Calendar anaDate) throws Exception{
		System.out.println("Start GpsMdtDataQualityAnalysis:"+GeneralConst.YYYY_MM_DD_HH_MM_SS.format(new Date().getTime()));
		if(queryGpsMdtData(anaDate)){
			return;
		}
		this.getDestMapping();
		if(this.destMapping==null){
			return ;
		}
		HashMap carNumMap = new HashMap(); 
		DestInfo dest = null;
		TypeNum typeNum = null;
		TypeNum typeNum1 = null;
		//初始化车辆异常数据和不合格数量
		for (Iterator iterator = destMapping.values().iterator(); iterator.hasNext();) {
			dest = (DestInfo) iterator.next();
			typeNum = new TypeNum();
			typeNum.carNo = dest.getDestNo();
			typeNum.com_id = dest.getCompanyId()+"";
			typeNum.run_com_id = dest.getRunComId()+"";
			carNumMap.put(dest.getDestNo(),typeNum);
		}
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			queryNoQualified(carNumMap, anaDate);
			queryOperateException(carNumMap, anaDate);
			queryGpsException(carNumMap, anaDate);
			HashMap yestodaycarNumMap = queryContinueDay(anaDate);
			conn.setAutoCommit(false);
			int count = 1;
			StatementHandle stmt = conn.prepareStatement("insert into gps_mdt_data_quality (id,dest_no,com_id,server_id," +
					"speed,points,point_valid,point_realtime,business_status,business_num,business_time,business_wait_time," +
					"gps_lo_la,gps_speed,gps_time,gps_status," +
					"single_business_money,single_business_time,single_business_price,single_business_mile,single_business_empty_mile,single_business_wait_time," +
					"assess_date,qualified,qualified_g,qualified_s,qualified_n,fly_num,noqualified_day,gps_day,business_day,total_day) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
			for (Iterator iterator = carNumMap.values().iterator(); iterator.hasNext();) {
				typeNum = (TypeNum) iterator.next();
				stmt.setInt(1, (int)DbServer.getSingleInstance().getAvaliableId(conn, "gps_mdt_data_quality", "id"));
				stmt.setString(2, typeNum.carNo);
				if(typeNum.com_id==null||typeNum.com_id.equals("")||typeNum.com_id.equals("null")){
					continue;
				}
				if(typeNum.run_com_id==null||typeNum.run_com_id.equals("")||typeNum.run_com_id.equals("null")){
					continue;
				}
				stmt.setInt(3, Integer.valueOf(typeNum.com_id));
				stmt.setInt(4, Integer.valueOf(typeNum.run_com_id));
				stmt.setInt(5, typeNum.type0Num);
				stmt.setInt(6, typeNum.type2Num);
				stmt.setInt(7, typeNum.type3Num);
				stmt.setInt(8, typeNum.type4Num);
				stmt.setInt(9, typeNum.type1Num);
				stmt.setInt(10, typeNum.type5Num);
				stmt.setInt(11, typeNum.type6Num);
				stmt.setInt(12, typeNum.type7Num);
				stmt.setInt(13, typeNum.type8Num);
				stmt.setInt(14, typeNum.type9Num);
				stmt.setInt(15, typeNum.type10Num);
				stmt.setInt(16, typeNum.type11Num);
				stmt.setInt(17, typeNum.type12Num);
				stmt.setInt(18, typeNum.type13Num);
				stmt.setInt(19, typeNum.type14Num);
				stmt.setInt(20, typeNum.type15Num);
				stmt.setInt(21, typeNum.type16Num);
				stmt.setInt(22, typeNum.type17Num);
				stmt.setDate(23, new java.sql.Date(anaDate.getTimeInMillis()));
				if(typeNum.type0Num <= 0&&typeNum.type1Num<=0&&typeNum.type2Num<=0&&typeNum.type3Num<=0&&
						typeNum.type4Num<=0&&typeNum.type5Num<=0&&typeNum.type6Num<=0&&typeNum.type7Num<=0&&
						typeNum.type8Num<=0&&typeNum.type9Num<=0&&typeNum.type10Num<=0&&typeNum.type11Num<=0&&
						typeNum.type12Num<=0&&typeNum.type13Num<=0&&typeNum.type14Num<=0&&typeNum.type15Num<=0&&
						typeNum.type16Num<=0&&typeNum.type17Num<=0&&typeNum.type18Num<=0){
					stmt.setInt(24, 0);
				}else{
					stmt.setInt(24, 1);
					if(yestodaycarNumMap.containsKey(typeNum.carNo)){
						typeNum1 = (TypeNum)yestodaycarNumMap.get(typeNum.carNo);
						typeNum.totalDay = typeNum1.totalDay+1;
					}else {
						typeNum.totalDay = 1;
					}
				}
				if(typeNum.type8Num<=0&&typeNum.type9Num<=0&&typeNum.type10Num<=0&&typeNum.type11Num<=0){
					stmt.setInt(25, 0);
				}else{
					stmt.setInt(25, 1);
					if(yestodaycarNumMap.containsKey(typeNum.carNo)){
						typeNum1 = (TypeNum)yestodaycarNumMap.get(typeNum.carNo);
						typeNum.gpsDay = typeNum1.gpsDay+1;
					}else {
						typeNum.gpsDay = 1;
					}
				}
				if(typeNum.type12Num<=0&&typeNum.type13Num<=0&&typeNum.type14Num<=0&&typeNum.type15Num<=0&&
						typeNum.type16Num<=0&&typeNum.type17Num<=0){
					stmt.setInt(26, 0);
				}else{
					stmt.setInt(26, 1);
					if(yestodaycarNumMap.containsKey(typeNum.carNo)){
						typeNum1 = (TypeNum)yestodaycarNumMap.get(typeNum.carNo);
						typeNum.businessDay = typeNum1.businessDay+1;
					}else {
						typeNum.businessDay = 1;
					}
				}
				if(typeNum.type0Num <= 0&&typeNum.type1Num<=0&&typeNum.type2Num<=0&&typeNum.type3Num<=0&&
						typeNum.type4Num<=0&&typeNum.type5Num<=0&&typeNum.type6Num<=0&&typeNum.type7Num<=0&&typeNum.type18Num<=0){
					stmt.setInt(27, 0);
				}else{
					stmt.setInt(27, 1);
					if(yestodaycarNumMap.containsKey(typeNum.carNo)){
						typeNum1 = (TypeNum)yestodaycarNumMap.get(typeNum.carNo);
						typeNum.noQualifiedDay = typeNum1.noQualifiedDay+1;
					}else {
						typeNum.noQualifiedDay = 1;
					}
				}
				stmt.setInt(28, typeNum.type18Num);
				stmt.setInt(29, typeNum.noQualifiedDay);
				stmt.setInt(30, typeNum.gpsDay);
				stmt.setInt(31, typeNum.businessDay);
				stmt.setInt(32, typeNum.totalDay);
				stmt.addBatch();
				if(count%200==0){
					stmt.executeBatch();
				}
				count++;
			}
			stmt.executeBatch();
			conn.commit();
		} catch (Exception e) {
			e.printStackTrace();
			conn.rollback();
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
		System.out.println("End GpsMdtDataQualityAnalysis:"+GeneralConst.YYYY_MM_DD_HH_MM_SS.format(new Date().getTime()));
	}
	private HashMap queryContinueDay(Calendar anaDate) {
		Calendar date = Calendar.getInstance();
		date.setTimeInMillis(anaDate.getTimeInMillis());
		date.add(Calendar.DAY_OF_MONTH, -1);
		HashMap tempMap = new HashMap(); 
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			StatementHandle stmt = conn.createStatement();
			ResultSet sets = stmt.executeQuery("select dest_no,noqualified_day,gps_day,business_day,total_day from gps_mdt_data_quality where assess_date>=to_date('"+GeneralConst.YYYY_MM_DD.format(date.getTime())+" 00:00:00','yyyy-MM-dd hh24:mi:ss') " +
				" and assess_date<=to_date('"+GeneralConst.YYYY_MM_DD.format(date.getTime())+" 23:59:59','yyyy-MM-dd hh24:mi:ss') order by dest_no");
			TypeNum typeNum = null;
			String temp = "";
			while(sets.next()){
				typeNum = new TypeNum();
				typeNum.carNo = sets.getString("dest_no");
				temp = sets.getString("noqualified_day");
				if(temp!=null&&!temp.equals("")){
					typeNum.noQualifiedDay = Integer.valueOf(temp);
				}else {
					typeNum.noQualifiedDay = 0;
				}
				temp = sets.getString("gps_day");
				if(temp!=null&&!temp.equals("")){
					typeNum.gpsDay = Integer.valueOf(temp);
				}else {
					typeNum.gpsDay = 0;
				}
				temp = sets.getString("business_day");
				if(temp!=null&&!temp.equals("")){
					typeNum.businessDay = Integer.valueOf(temp);
				}else {
					typeNum.businessDay = 0;
				}
				temp = sets.getString("total_day");
				if(temp!=null&&!temp.equals("")){
					typeNum.totalDay = Integer.valueOf(temp);
				}else {
					typeNum.totalDay = 0;
				}
				tempMap.put(typeNum.carNo, typeNum);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
		
		return tempMap;
	}

	//判断该日是否已存在数据
	private boolean queryGpsMdtData(Calendar anaDate) {
		anaDate.set(Calendar.HOUR_OF_DAY, 0);
		anaDate.set(Calendar.MINUTE, 0);
		anaDate.set(Calendar.SECOND, 0);
		anaDate.set(Calendar.MILLISECOND, 0);
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(anaDate.getTimeInMillis());
		cal.set(Calendar.DAY_OF_MONTH, cal.get(Calendar.DAY_OF_MONTH)+1);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		DbHandle conn = DbServer.getSingleInstance().getConnWithUseTime(-1);
		try {
			StatementHandle stmt = conn.prepareStatement("select * from gps_mdt_data_quality where assess_date>=? and assess_date<?");
			stmt.setTimestamp(1, new Timestamp(anaDate.getTimeInMillis()));
			stmt.setTimestamp(2, new Timestamp(cal.getTimeInMillis()));
			ResultSet sets = stmt.executeQuery();
			if(sets.next()){
				return true;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
		return false;
	}

	//统计单车GPS异常数据数量
	private void queryGpsException(HashMap carNumMap,Calendar anaDate) throws Exception{
		String sql = "select car_no,exception_type type,count(*) sum from (" +
				" select * from gps_exception_data where query_time>=to_date('"+GeneralConst.YYYY_MM_DD.format(anaDate.getTime())+" 00:00:00','yyyy-MM-dd hh24:mi:ss') " +
				" and query_time<=to_date('"+GeneralConst.YYYY_MM_DD.format(anaDate.getTime())+" 23:59:59','yyyy-MM-dd hh24:mi:ss') order by car_no)" +
				" group by car_no,exception_type order by car_no";
		
		DbHandle conn = DbServer.getSingleInstance().getConnWithUseTime(-1);
		try{
			StatementHandle stmt = conn.createStatement();
			ResultSet sets = stmt.executeQuery(sql);
			TypeNum typeNum = null;
			while(sets.next()){
				String carNo = sets.getString("car_no");
				if(carNumMap.containsKey(carNo)){
					typeNum = (TypeNum)carNumMap.get(carNo);
				}else {
					continue;
				}
				int type = sets.getInt("type");
				if(type==0){
					typeNum.type8Num = sets.getInt("sum");
				}else if (type==1) {
					typeNum.type9Num = sets.getInt("sum");
				}else if (type==2) {
					typeNum.type10Num = sets.getInt("sum");
				}else if (type==3) {
					typeNum.type11Num = sets.getInt("sum");
				}
				carNumMap.put(carNo, typeNum);
			}
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
	}
	//统计单车营运数据异常数量
	private void queryOperateException(HashMap carNumMap,Calendar anaDate) throws Exception{
		String sql = "select car_no,exception_type type,count(*) sum from (" +
				" select * from operation_exception_data where query_time>=to_date('"+GeneralConst.YYYY_MM_DD.format(anaDate.getTime())+" 00:00:00','yyyy-MM-dd hh24:mi:ss') " +
				" and query_time<=to_date('"+GeneralConst.YYYY_MM_DD.format(anaDate.getTime())+" 23:59:59','yyyy-MM-dd hh24:mi:ss') order by car_no)" +
				" group by car_no,exception_type order by car_no";
		DbHandle conn = DbServer.getSingleInstance().getConnWithUseTime(-1);
		try{
			StatementHandle stmt = conn.createStatement();
			ResultSet sets = stmt.executeQuery(sql);
			TypeNum typeNum = null;
			while(sets.next()){
				String carNo = sets.getString("car_no");
				if(carNumMap.containsKey(carNo)){
					typeNum = (TypeNum)carNumMap.get(carNo);
				}else {
					continue;
				}
				int type = sets.getInt("type");
				if(type==0){
					typeNum.type12Num = sets.getInt("sum");
				}else if (type==1) {
					typeNum.type13Num = sets.getInt("sum");
				}else if (type==2) {
					typeNum.type14Num = sets.getInt("sum");
				}else if (type==3) {
					typeNum.type15Num = sets.getInt("sum");
				}else if (type==4) {
					typeNum.type16Num = sets.getInt("sum");
				}else if (type==5) {
					typeNum.type17Num = sets.getInt("sum");
				}
				carNumMap.put(carNo, typeNum);
			}
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
	}
	//统计单车不合格数据数量
	private void queryNoQualified(HashMap carNumMap,Calendar anaDate) throws Exception{
		String sql = "select car_no,type,count(*) sum from (" +
				" select * from no_qualified_data where query_time>=to_date('"+GeneralConst.YYYY_MM_DD.format(anaDate.getTime())+" 00:00:00','yyyy-MM-dd hh24:mi:ss') " +
				" and query_time<=to_date('"+GeneralConst.YYYY_MM_DD.format(anaDate.getTime())+" 23:59:59','yyyy-MM-dd hh24:mi:ss') order by car_no)" +
				" group by car_no,type order by car_no";
		DbHandle conn = DbServer.getSingleInstance().getConnWithUseTime(-1);
		TypeNum typeNum = null;
		try{
			StatementHandle stmt = conn.createStatement();
			ResultSet sets = stmt.executeQuery(sql);
			while(sets.next()){
				String carNo = sets.getString("car_no");
				if(carNumMap.containsKey(carNo)){
					typeNum = (TypeNum)carNumMap.get(carNo);
				}else {
					continue;
				}
				int type = sets.getInt("type");
				if(type==0){
					typeNum.type0Num = sets.getInt("sum");
				}else if (type==1) {
					typeNum.type1Num = sets.getInt("sum");
				}else if (type==2) {
					typeNum.type2Num = sets.getInt("sum");
				}else if (type==3) {
					typeNum.type3Num = sets.getInt("sum");
				}else if (type==4) {
					typeNum.type4Num = sets.getInt("sum");
				}else if (type==5) {
					typeNum.type5Num = sets.getInt("sum");
				}else if (type==6) {
					typeNum.type6Num = sets.getInt("sum");
				}else if (type==7) {
					typeNum.type7Num = sets.getInt("sum");
				}else if (type==8) {
					typeNum.type18Num = sets.getInt("sum");
				}
				carNumMap.put(carNo, typeNum);
			}
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
	}
	
	private class TimerTask extends FleetyTimerTask{
		public void run(){
			Calendar cal = Calendar.getInstance();
			cal.add(Calendar.DAY_OF_MONTH, -1);
			cal.set(Calendar.HOUR_OF_DAY, 0);
			cal.set(Calendar.MINUTE, 0);
			cal.set(Calendar.SECOND, 0);
			cal.set(Calendar.MILLISECOND, 0);
			
			System.out.println("Fire ExecTask GpsMdtDataQualityAnalysis:"+GeneralConst.YYYY_MM_DD_HH.format(cal.getTime()));
			GpsMdtDataQualityAnalysis.this.addExecTask(new ExecTask(cal));
		}
	}
	

	public void runTask(){
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DAY_OF_MONTH, -1);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		
		System.out.println("Fire ExecTask GpsMdtDataQualityAnalysis:"+GeneralConst.YYYY_MM_DD_HH.format(cal.getTime()));
		GpsMdtDataQualityAnalysis.this.addExecTask(new ExecTask(cal));
	}
	
	private class ExecTask extends BasicTask{
		private Calendar anaDate = null;
		public ExecTask(Calendar anaDate){
			this.anaDate = anaDate;
		}
		
		public boolean execute() throws Exception{
			GpsMdtDataQualityAnalysis.this.executeTask(this.anaDate);
			return true;
		}
		
		public String getDesc(){
			return "车辆GPS终端数据质量跟踪分析服务";
		}
		public Object getFlag(){
			return "GpsMdtDataQualityAnalysis";
		}
	}
	private HashMap destMapping = null;
	private void getDestMapping(){
			HashMap tempMapping = new HashMap();
			DestInfo destInfo = null;
			DbHandle conn = DbServer.getSingleInstance().getConnWithUseTime(-1);
			try {
				String sql = "select * from v_ana_dest_info";
				StatementHandle stmt = conn.createStatement();
				ResultSet sets = stmt.executeQuery(sql);
				int mdtId;
				while (sets.next()) {
					mdtId = sets.getInt("mdt_id");
					if(mdtId < 10){
						continue;
					}
					mdtId = Integer.parseInt((mdtId+"").substring(1));
					
					destInfo = new DestInfo();
					destInfo.setMdtId(mdtId);
					destInfo.setDestNo(sets.getString("dest_no"));
					destInfo.setCompanyId(sets.getInt("company_id"));
					destInfo.setCompanyName(sets.getString("company_name"));
					destInfo.setRunComId(sets.getInt("gps_run_com_id"));
					destInfo.setRunComName(sets.getString("gps_run_com_name"));
					tempMapping.put(destInfo.getDestNo(), destInfo);
				}
				destMapping = tempMapping;
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("从数据库[v_ana_dest_info]加载车辆信息失败！");
			}finally{
				DbServer.getSingleInstance().releaseConn(conn);
			}
	};
	public class TypeNum{
		public String carNo;
		public String com_id;
		public String run_com_id;
		public int type0Num = 0;//连续速度不合格
		public int type1Num = 0;//营运状态不合格
		public int type2Num = 0;//上传频度不合格
		public int type3Num = 0;//有效点数不合格
		public int type4Num = 0;//实时数据不合格
		public int type5Num = 0;//营运笔数不合格
		public int type6Num = 0;//营运时间不合格
		public int type7Num = 0;//等候时长不合格
		public int type18Num = 0;//飞点次数不合格
		public int noQualifiedDay = 0;//不合格连续天数
		
		public int type8Num = 0;//经纬度异常
		public int type9Num = 0;//速度异常
		public int type10Num = 0;//时间异常
		public int type11Num = 0;//状态异常
		public int gpsDay = 0;//gps异常连续天数
		
		public int type12Num = 0;//营运金额异常
		public int type13Num = 0;//营运时长异常
		public int type14Num = 0;//营运单价异常
		public int type15Num = 0;//营运里程异常
		public int type16Num = 0;//空驶里程异常
		public int type17Num = 0;//等候时长异常
		public int businessDay = 0;//营运数据异常连续天数
		
		public int totalDay = 0;//故障车辆连续天数
	}
}
