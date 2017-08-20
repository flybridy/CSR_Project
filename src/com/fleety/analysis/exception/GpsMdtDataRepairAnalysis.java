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

public class GpsMdtDataRepairAnalysis  extends AnalysisServer {
	private TimerTask task = null;
	private int repair_num = 3;//给定修复天数
	private HashMap repairMap = null;
	private int day=3;//为保障数据，连续分析天数

	public boolean startServer() {
		super.startServer();
		if(!this.isRunning()){
			return this.isRunning();
		}		
		
		int hour = this.getIntegerPara("hour").intValue();
		int minute = this.getIntegerPara("minute").intValue();
		repair_num = this.getIntegerPara("repair_num").intValue();
		day = this.getIntegerPara("day").intValue();
		
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
		System.out.println("Start GpsMdtDataRepairAnalysis:"+GeneralConst.YYYY_MM_DD_HH_MM_SS.format(anaDate.getTime())+GeneralConst.YYYY_MM_DD_HH_MM_SS.format(new Date().getTime()));
		if(queryGpsMdtDataRepair(anaDate)){
			return;
		}
		repairMap = new HashMap();
		HashMap gpsMdtQualityMap = new HashMap(); 
		queryGpsMdtQualityMap(gpsMdtQualityMap, anaDate);
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(anaDate.getTimeInMillis());
		cal.set(Calendar.DAY_OF_MONTH, cal.get(Calendar.DAY_OF_MONTH)-repair_num-1);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		HashMap gpsMdtQualityMap1 = new HashMap(); 
		queryGpsMdtQualityMap(gpsMdtQualityMap1, cal);
		
		TypeNum typeNum = null;
		GpsMdtQuality gpsMdtQuality = null;
		GpsMdtQuality gpsMdtQuality1 = null;
		for (Iterator iterator = gpsMdtQualityMap.values().iterator(); iterator.hasNext();) {
			gpsMdtQuality = (GpsMdtQuality) iterator.next();
			gpsMdtQuality1 = (GpsMdtQuality)gpsMdtQualityMap1.get(gpsMdtQuality.carNo);
			if(gpsMdtQuality1!=null){
				typeNum = new TypeNum();
				typeNum.carNo = gpsMdtQuality.carNo;
				typeNum.com_id = gpsMdtQuality.com_id;
				typeNum.run_com_id = gpsMdtQuality.server_id;
				if(gpsMdtQuality.qualified==1&&gpsMdtQuality1.qualified==0){
					typeNum.newadd = 1;
				}else if(gpsMdtQuality.qualified==0&&gpsMdtQuality1.qualified==1){
					typeNum.repair = 1;
					typeNum.isrepair = 1;
				}else if (gpsMdtQuality.qualified==1&&gpsMdtQuality1.qualified==1) {
					typeNum.isrepair = 1;
				}
				if(gpsMdtQuality.qualified_g==1&&gpsMdtQuality1.qualified_g==0){
					typeNum.newadd_g = 1;
				}else if(gpsMdtQuality.qualified_g==0&&gpsMdtQuality1.qualified_g==1){
					typeNum.repair_g = 1;
					typeNum.isrepair_g = 1;
				}else if (gpsMdtQuality.qualified_g==1&&gpsMdtQuality1.qualified_g==1) {
					typeNum.isrepair_g = 1;
				}
				if(gpsMdtQuality.qualified_s==1&&gpsMdtQuality1.qualified_s==0){
					typeNum.newadd_s = 1;
				}else if(gpsMdtQuality.qualified_s==0&&gpsMdtQuality1.qualified_s==1){
					typeNum.repair_s = 1;
					typeNum.isrepair_s = 1;
				}else if (gpsMdtQuality.qualified_s==1&&gpsMdtQuality1.qualified_s==1) {
					typeNum.isrepair_s = 1;
				}
				if(gpsMdtQuality.qualified_n==1&&gpsMdtQuality1.qualified_n==0){
					typeNum.newadd_n = 1;
				}else if(gpsMdtQuality.qualified_n==0&&gpsMdtQuality1.qualified_n==1){
					typeNum.repair_n = 1;
					typeNum.isrepair_n = 1;
				}else if (gpsMdtQuality.qualified_n==1&&gpsMdtQuality1.qualified_n==1) {
					typeNum.isrepair_n = 1;
				}
				typeNum.newstatus = gpsMdtQuality.qualified;
				typeNum.newstatus_g = gpsMdtQuality.qualified_g;
				typeNum.newstatus_s = gpsMdtQuality.qualified_s;
				typeNum.newstatus_n = gpsMdtQuality.qualified_n;
				repairMap.put(typeNum.carNo, typeNum);
			}
		}
		
		
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			conn.setAutoCommit(false);
			int count = 1;
			StatementHandle stmt = conn.prepareStatement("insert into gps_mdt_data_repair (id,dest_no,com_id,server_id,repair,newadd,isrepair,newstatus,repair_g,newadd_g,isrepair_g,newstatus_g,repair_s,newadd_s,isrepair_s,newstatus_s,repair_n,newadd_n,isrepair_n,newstatus_n,assess_date) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
			for (Iterator iterator = repairMap.values().iterator(); iterator.hasNext();) {
				typeNum = (TypeNum) iterator.next();
				stmt.setInt(1, (int)DbServer.getSingleInstance().getAvaliableId(conn, "gps_mdt_data_repair", "id"));
				stmt.setString(2, typeNum.carNo);
				if(typeNum.com_id==null||typeNum.com_id.equals("")||typeNum.com_id.equals("null")){
					continue;
				}
				if(typeNum.run_com_id==null||typeNum.run_com_id.equals("")||typeNum.run_com_id.equals("null")){
					continue;
				}
				stmt.setInt(3, Integer.valueOf(typeNum.com_id));
				stmt.setInt(4, Integer.valueOf(typeNum.run_com_id));
				stmt.setInt(5, typeNum.repair);
				stmt.setInt(6, typeNum.newadd);
				stmt.setInt(7, typeNum.isrepair);
				stmt.setInt(8, typeNum.newstatus);
				stmt.setInt(9, typeNum.repair_g);
				stmt.setInt(10, typeNum.newadd_g);
				stmt.setInt(11, typeNum.isrepair_g);
				stmt.setInt(12, typeNum.newstatus_g);
				stmt.setInt(13, typeNum.repair_s);
				stmt.setInt(14, typeNum.newadd_s);
				stmt.setInt(15, typeNum.isrepair_s);
				stmt.setInt(16, typeNum.newstatus_s);
				stmt.setInt(17, typeNum.repair_n);
				stmt.setInt(18, typeNum.newadd_n);
				stmt.setInt(19, typeNum.isrepair_n);
				stmt.setInt(20, typeNum.newstatus_n);
				stmt.setDate(21, new java.sql.Date(anaDate.getTimeInMillis()));
				stmt.addBatch();
				if(count%200==0){
					stmt.executeBatch();
				}
				count++;
			}
			stmt.executeBatch();
			conn.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
		System.out.println("End GpsMdtDataRepairAnalysis:"+GeneralConst.YYYY_MM_DD_HH_MM_SS.format(new Date().getTime()));
	}
	//判断改日是否已存在数据
	private boolean queryGpsMdtDataRepair(Calendar anaDate) {
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
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			StatementHandle stmt = conn.prepareStatement("select * from gps_mdt_data_repair where assess_date>=? and assess_date<?");
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
	//查询某日车辆跟踪数据
	private void queryGpsMdtQualityMap(HashMap gpsMdtQualityMap,Calendar anaDate) throws Exception{
		String sql = "select dest_no,com_id,server_id,to_char(assess_date,'yyyy-MM-dd') assess_date,qualified,qualified_g,qualified_s,qualified_n " +
				" from gps_mdt_data_quality where assess_date>=to_date('"+GeneralConst.YYYY_MM_DD.format(anaDate.getTime())+" 00:00:00','yyyy-MM-dd hh24:mi:ss') " +
				" and assess_date<=to_date('"+GeneralConst.YYYY_MM_DD.format(anaDate.getTime())+" 23:59:59','yyyy-MM-dd hh24:mi:ss') order by dest_no";
		DbHandle conn = DbServer.getSingleInstance().getConn();
		GpsMdtQuality gpsMdtQuality = null;
		try{
			StatementHandle stmt = conn.createStatement();
			ResultSet sets = stmt.executeQuery(sql);
			while(sets.next()){
				String carNo = sets.getString("dest_no");
				if(gpsMdtQualityMap.containsKey(carNo)){
					gpsMdtQuality = (GpsMdtQuality)gpsMdtQualityMap.get(carNo);
				}else {
					gpsMdtQuality = new GpsMdtQuality();
					gpsMdtQuality.carNo = carNo;
				}
				gpsMdtQuality.assess_date = sets.getString("assess_date");
				gpsMdtQuality.com_id = sets.getString("com_id");
				gpsMdtQuality.server_id = sets.getString("server_id");
				gpsMdtQuality.qualified = sets.getInt("qualified");
				gpsMdtQuality.qualified_g = sets.getInt("qualified_g");
				gpsMdtQuality.qualified_s = sets.getInt("qualified_s");
				gpsMdtQuality.qualified_n = sets.getInt("qualified_n");
				gpsMdtQualityMap.put(carNo, gpsMdtQuality);
			}
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
	}
	
	private class TimerTask extends FleetyTimerTask{
		public void run(){
			Calendar cal = Calendar.getInstance();
			cal.set(Calendar.HOUR_OF_DAY, 0);
			cal.set(Calendar.MINUTE, 0);
			cal.set(Calendar.SECOND, 0);
			cal.set(Calendar.MILLISECOND, 0);
			Calendar cal1 = null;
			for (int i = 1; i <= day; i++) {
				cal.add(Calendar.DAY_OF_MONTH, -1);
				cal1 = Calendar.getInstance();
				cal1.setTimeInMillis(cal.getTimeInMillis());
				System.out.println("Fire ExecTask GpsMdtDataRepairAnalysis:"+GeneralConst.YYYY_MM_DD_HH_MM_SS.format(cal1.getTime()));
				GpsMdtDataRepairAnalysis.this.addExecTask(new ExecTask(cal1));
			}
		}
	}
	
	private class ExecTask extends BasicTask{
		private Calendar anaDate = null;
		public ExecTask(Calendar anaDate){
			this.anaDate = anaDate;
		}
		
		public boolean execute() throws Exception{
			try {
				GpsMdtDataRepairAnalysis.this.executeTask(this.anaDate);
			} catch (Exception e) {
				e.printStackTrace();
			}
			return true;
		}
		
		public String getDesc(){
			return "车辆GPS终端数据质量跟踪分析服务";
		}
		public Object getFlag(){
			return "GpsMdtDataRepairAnalysis";
		}
	}
	public class GpsMdtQuality{
		public String carNo;
		public String com_id;
		public String server_id;
		public String assess_date;
		public int qualified;//0,合格;1,不合格;
		public int qualified_g;
		public int qualified_s;
		public int qualified_n;
	}
	public class TypeNum{
		public String carNo;
		public String com_id;
		public String run_com_id;
		public int repair = 0;//总体修复的 0,已修复;1,未修复
	    public int newadd = 0;//总体新增的 0,不是新增;1,新增
		public int  isrepair = 0;//总体是否需要修复 0,否;1,是、
		public int  newstatus = 0;//当前状态 0,正常;1,异常;
		    
		public int  repair_g = 0;//GPS异常修复的 0,已修复;1,未修复
		public int  newadd_g = 0;//GPS异常新增的 0,不是新增;1,新增
		public int  isrepair_g = 0;//GPS异常是否需要修复 0,否;1,是
		public int  newstatus_g = 0;//当前GPS异常状态 0,正常;1,异常;
		
		public int  repair_s = 0;//营运异常修复的 0,已修复;1,未修复
		public int  newadd_s = 0;//营运异常新增的 0,不是新增;1,新增
		public int  isrepair_s = 0;//营运异常是否需要修复 0,否;1,是
		public int  newstatus_s = 0;//当前营运异常状态 0,正常;1,异常;
		
		public int  repair_n = 0;//不合格数据修复的 0,已修复;1,未修复
		public int  newadd_n = 0;//不合格数据新增的 0,不是新增;1,新增
		public int  isrepair_n = 0;//--不合格数据是否需要修复 0,否;1,是
		public int  newstatus_n = 0;//当前不合格状态 0,正常;1,异常;
	}
}
