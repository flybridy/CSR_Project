package com.fleety.analysis.track.task;

import java.awt.Shape;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;

import server.db.DbServer;
import server.var.VarManageServer;

import com.fleety.analysis.AnalysisServer;
import com.fleety.analysis.operation.IOperationAnalysis;
import com.fleety.analysis.track.ITrackAnalysis;
import com.fleety.analysis.track.TrackInfo;
import com.fleety.base.GeneralConst;
import com.fleety.base.InfoContainer;
import com.fleety.server.area.AreaDataLoadServer;
import com.fleety.server.area.AreaInfo;
import com.fleety.track.TrackIO;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;

public class CallCarTroubleAreaDataAnalysis implements ITrackAnalysis{
	private HashMap callCarTroubleAreaPassMap = null;
	private HashMap<Integer,CallCarTroubleAreaPlan> callCarTroubleAreaPlanMap = null;
	private HashMap<Integer, AreaInfo> areaData = null;
	private long duras = 30*60*1000;//向上车时间延长的时间段
	private long durax = 60*60*1000;//向下车时间延长的时间段
	private long tingStandard = 60*1000;
	
	public boolean startAnalysisTrack(AnalysisServer parentServer,InfoContainer statInfo) {
		this.callCarTroubleAreaPassMap = null;
		this.callCarTroubleAreaPlanMap = null;
		this.areaData = null;
		String temp = VarManageServer.getSingleInstance().getVarStringValue("tingStandard");
		if(temp!=null&&!temp.equals("")){
			tingStandard = (long)(Double.valueOf(temp)*1000);
		}
		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
		AreaDataLoadServer areaDataLoadServer = new AreaDataLoadServer();
		//加载打车难区域信息
		this.areaData = areaDataLoadServer.getAreaData(101, false);
		//加载打车难区域计划
		this.callCarTroubleAreaPlanMap = this.queryCallCarTroubleAreaPlan(sTime);
		//加载在打车难区域内的营运数据
		if(!querySingleBusinessDataBs(statInfo)){
			return false;
		}
		if(this.callCarTroubleAreaPlanMap==null||areaData == null){
			return false;
		}
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try{
			StatementHandle stmt = conn.createStatement();
			StringBuilder sb = new StringBuilder();
			sb.append("select * from call_car_trouble_area_pass ")
			  .append(" where assess_date = to_date('").append(GeneralConst.YYYY_MM_DD.format(sTime)).append("','yyyy-MM-dd hh24:mi:ss')");
			ResultSet sets = stmt.executeQuery(sb.toString());
			if(!sets.next()){
				this.callCarTroubleAreaPassMap = new HashMap();
			}
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
			
		if(this.callCarTroubleAreaPassMap == null){
			System.out.println("Not Need Analysis:"+this.toString());
		}else{
				System.out.println("Start Analysis:"+this.toString());
		}
		return this.callCarTroubleAreaPassMap != null;
	}
	
	public void analysisDestTrack(AnalysisServer parentServer,TrackInfo trackInfo) {
		if(this.callCarTroubleAreaPlanMap==null||this.callCarTroubleAreaPlanMap.size()<=0||this.areaData.size()<=0){
			return ;
		}
		String destNo = trackInfo.dInfo.destNo;
		Date sTime = trackInfo.sDate;
		Date eTime = trackInfo.eDate;
		double slo = -1;
		double sla = -1;
		long sTime1 = 0;
		int sStatus = -1;
		long dest_time ;
		double templo = 0;
		double templa = 0;
		double tempSpeed = 0;
		int tempStatus = -1;
		long kongj = -1;//空车进入时间
		long zhong = -1;//重车出现时间
		long chu = -1;//出区域时间
		long tingN = -1;//空车进入到出现重车停留时间
		double preKilo = -1;
		int first = 0;
		boolean isAdd = false;
		CallCarTroubleAreaPassInfo callCarTroubleAreaPassInfo = null;
		for (Iterator iterator = this.callCarTroubleAreaPlanMap.values().iterator(); iterator.hasNext();) {
			CallCarTroubleAreaPlan callCarTroubleAreaPlan = (CallCarTroubleAreaPlan) iterator.next();
			if(!callCarTroubleAreaPlan.carMap.containsKey(destNo))
			{
				System.out.println("not continue: destNo:"+destNo+" size:"+callCarTroubleAreaPlan.carMap.size()+" id:"+callCarTroubleAreaPlan.plan_id);
				continue;
			}
			System.out.println("destNo:"+destNo);
			HashMap<String,SingleBusinessDataBs> singleBusinessDataBsMap = callCarTroubleAreaPlan.singleBusinessDataBsMap;
			AreaInfo areaInfo = areaData.get(callCarTroubleAreaPlan.area_id);
			for (Iterator iterator2 = singleBusinessDataBsMap.values().iterator(); iterator2
					.hasNext();) {
				SingleBusinessDataBs singleBusinessDataBs = (SingleBusinessDataBs) iterator2.next();
				if(!singleBusinessDataBs.dest_no.equals(destNo)){
					continue;
				}
				boolean preInArea=false;
				boolean tempInArea=false;
				for (int i = 0; i < trackInfo.trackArr.length; i++) {
					dest_time = trackInfo.trackArr[i].getDate(TrackIO.DEST_TIME_FLAG).getTime();
					if(dest_time<singleBusinessDataBs.date_up-this.duras||dest_time>singleBusinessDataBs.date_down+this.durax){
						continue;
					}
					templo = trackInfo.trackArr[i].getDouble(TrackIO.DEST_LO_FLAG);
					templa = trackInfo.trackArr[i].getDouble(TrackIO.DEST_LA_FLAG);
					tempSpeed = trackInfo.trackArr[i].getDouble(TrackIO.DEST_SPEED_FLAG);
					tempStatus = trackInfo.trackArr[i].getInteger(TrackIO.DEST_STATUS_FLAG)&0x0F;
					tempInArea=areaInfo.contains(templo, templa);
					if(!isAdd&&dest_time>=singleBusinessDataBs.date_up-3*60*1000L){
						if(tempInArea){
							isAdd = true;
						}
					}
					if(first==0){
						slo = templo;
						sla = templa;
						preInArea=tempInArea;
						sStatus = tempStatus;
						sTime1 = dest_time;
						first = 1;
						continue;
					}
					if(!preInArea&&tempInArea){
						if(tempStatus==0){
							kongj = dest_time;
						}
					}
					if(kongj>0){
						if(sStatus==0&&tempStatus==1&&zhong<0){
							zhong = dest_time;
							if(zhong<=kongj||zhong<singleBusinessDataBs.date_up-30000||zhong>singleBusinessDataBs.date_down){
								zhong = -1;
							}
						}
					}
					if(preInArea&&!tempInArea){
						if(zhong>0){
							chu = dest_time;
						}else {
							kongj=-1;
						}
					}
					if(zhong>0&&dest_time<=singleBusinessDataBs.date_down){
						if(preKilo >= 0){
							double dis = countDistance(slo,sla,templo,templa);
							if(dis < 10){
								preKilo += dis;
							}
						}else{
							preKilo = 0;
						}
					}
					else if(zhong>0)
					{
						break;
					}
					slo = templo;
					sla = templa;
					sStatus = tempStatus;
					sTime1 = dest_time;
					preInArea=tempInArea;
				}
				if(kongj>0&&zhong>0&&isAdd&&preKilo>0){
					tingN = zhong - kongj;
					if(tingN >= tingStandard){
						callCarTroubleAreaPassInfo = new CallCarTroubleAreaPassInfo();
						callCarTroubleAreaPassInfo.plan_id = callCarTroubleAreaPlan.plan_id;
						callCarTroubleAreaPassInfo.area_id = callCarTroubleAreaPlan.area_id;
						callCarTroubleAreaPassInfo.assess_date = new Timestamp(sTime.getTime());
						callCarTroubleAreaPassInfo.service_no = singleBusinessDataBs.service_no;
						callCarTroubleAreaPassInfo.dest_no = destNo;
						callCarTroubleAreaPassInfo.com_id = trackInfo.dInfo.companyId;
						callCarTroubleAreaPassInfo.server_id = trackInfo.dInfo.gpsRunComId;
						callCarTroubleAreaPassInfo.nul_in_time = kongj;//空车进入时间
						callCarTroubleAreaPassInfo.zhong_time_n = zhong;//区域内出现重车时间
						callCarTroubleAreaPassInfo.leave_time = chu;//出区域时间
						callCarTroubleAreaPassInfo.stop_dura = tingN;//区域停留时长
						callCarTroubleAreaPassInfo.down_time = singleBusinessDataBs.date_down;//乘客下车时间
						callCarTroubleAreaPassInfo.zhong_run_time = singleBusinessDataBs.date_down-singleBusinessDataBs.date_up;//重车营运时长
						callCarTroubleAreaPassInfo.zhong_run_mile = preKilo;
						this.callCarTroubleAreaPassMap.put(destNo+"_"+singleBusinessDataBs.date_up, callCarTroubleAreaPassInfo);
						//System.out.println("callCarTroubleAreaPassMap.size::"+callCarTroubleAreaPassMap.size()+" zhong:"+zhong+" kongj:"+kongj+" preKilo:"+preKilo);
					}
				}
				System.out.println("kongj::"+kongj+" zhong::"+zhong+" chu::"+chu+" tingN::"+tingN+" isAdd::"+isAdd+" destNo::"+destNo+" preKilo::"+preKilo);
				kongj = -1;
				zhong = -1;
				tingN = -1;
				chu = -1;
				preKilo = -1;
				first = 0;
				isAdd = false;
			}
		}
	}

	public void endAnalysisTrack(AnalysisServer parentServer,InfoContainer statInfo) {
		if(this.callCarTroubleAreaPassMap==null){
			return;
		}
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			conn.setAutoCommit(false);
			int count = 1;
			StatementHandle stmt = conn.prepareStatement("insert into call_car_trouble_area_pass" +
					"(id,plan_id,area_id,assess_date,service_no,dest_no,com_id,server_id,nul_in_time,zhong_time_n," +
					"leave_time,stop_dura,down_time,zhong_run_time,zhong_run_mile,record_time) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
			System.out.println("this.callCarTroubleAreaPassMap.size::"+this.callCarTroubleAreaPassMap.size());
			for (Iterator iterator = this.callCarTroubleAreaPassMap.values().iterator(); iterator.hasNext();) {
				CallCarTroubleAreaPassInfo passInfo = (CallCarTroubleAreaPassInfo) iterator.next();
				stmt.setInt(1, (int)DbServer.getSingleInstance().getAvaliableId(conn, "call_car_trouble_area_pass", "id"));
				stmt.setInt(2, passInfo.plan_id);
				stmt.setInt(3, passInfo.area_id);
				stmt.setTimestamp(4, passInfo.assess_date);
				stmt.setString(5, passInfo.service_no);
				stmt.setString(6, passInfo.dest_no);
				stmt.setInt(7, passInfo.com_id);
				stmt.setInt(8, passInfo.server_id);
				stmt.setTimestamp(9, new Timestamp(passInfo.nul_in_time));
				stmt.setTimestamp(10, new Timestamp(passInfo.zhong_time_n));
				stmt.setTimestamp(11, new Timestamp(passInfo.leave_time));
				stmt.setInt(12, (int)passInfo.stop_dura);
				stmt.setTimestamp(13, new Timestamp(passInfo.down_time));
				stmt.setInt(14, (int)passInfo.zhong_run_time);
				stmt.setDouble(15, passInfo.zhong_run_mile);
				stmt.setTimestamp(16, new Timestamp(new Date().getTime()));
				stmt.addBatch();
				if (count % 200 == 0) {
					stmt.executeBatch();
				}
				count++;
			}
			stmt.executeBatch();
			conn.commit();
		}catch (Exception e) {
			e.printStackTrace();
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
			this.callCarTroubleAreaPlanMap = null;
		}
	}
	private HashMap<Integer,CallCarTroubleAreaPlan> queryCallCarTroubleAreaPlan(Date sTime){
		HashMap<Integer,CallCarTroubleAreaPlan> callCarTroubleAreaPlanMap = new HashMap<Integer,CallCarTroubleAreaPlan>();
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			StatementHandle stmt = conn.prepareStatement("select id,plan_name,start_time,end_time,start_date,end_date,area_id,status from call_car_trouble_area_plan where start_date<=? and end_date>=? and status<2");
			stmt.setTimestamp(1, new Timestamp(sTime.getTime()));
			stmt.setTimestamp(2, new Timestamp(sTime.getTime()));
			StatementHandle carStat = conn.prepareStatement("select car_id from call_car_trouble_com_bind_car where id =?");
			ResultSet sets = stmt.executeQuery();
			CallCarTroubleAreaPlan callCarTroubleAreaPlan = null;
			while (sets.next()) {
				callCarTroubleAreaPlan = new CallCarTroubleAreaPlan();
				callCarTroubleAreaPlan.plan_id = sets.getInt("id");
				callCarTroubleAreaPlan.plan_name = sets.getString("plan_name");
				callCarTroubleAreaPlan.start_date = GeneralConst.YYYY_MM_DD_HH_MM_SS.parse(sets.getString("start_date"));
				callCarTroubleAreaPlan.end_date = GeneralConst.YYYY_MM_DD_HH_MM_SS.parse(sets.getString("end_date"));
				callCarTroubleAreaPlan.start_time = sets.getString("start_time");
				callCarTroubleAreaPlan.end_time = sets.getString("end_time");
				callCarTroubleAreaPlan.area_id = sets.getInt("area_id");
				callCarTroubleAreaPlan.status = sets.getInt("status");
				carStat.setInt(1, callCarTroubleAreaPlan.plan_id);
				ResultSet rs=carStat.executeQuery();
				while(rs.next())
				{
					callCarTroubleAreaPlan.carMap.put(rs.getString("car_id"), rs.getString("car_id"));
				}
				rs.close();
				System.out.println("callCarTroubleAreaPlan.plan_id:"+callCarTroubleAreaPlan.plan_id +" carNum:"+callCarTroubleAreaPlan.carMap.size());
				callCarTroubleAreaPlanMap.put(sets.getInt("id"),callCarTroubleAreaPlan);
			}
			
			
		}catch (Exception e) {
			e.printStackTrace();
			return null;
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
		return callCarTroubleAreaPlanMap;
	}
	public boolean querySingleBusinessDataBs(InfoContainer statInfo){
		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
		Date eTime = statInfo.getDate(STAT_END_TIME_DATE);
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			StatementHandle stmt = conn.prepareStatement("select * from single_business_data_bs where date_up>=? and date_up<=?");
			stmt.setTimestamp(1, new Timestamp(sTime.getTime()+18*60*1000L));
			stmt.setTimestamp(2, new Timestamp(eTime.getTime()));
			ResultSet sets = stmt.executeQuery();
			double lo = -1;
			double la = -1;
			long dateup = 0;
			long startTime = 0;
			long endTime = 0;
			AreaInfo areaInfo = null;
			SingleBusinessDataBs singleBusinessDataBs = null;
			while (sets.next()) {
				dateup = GeneralConst.YYYY_MM_DD_HH_MM_SS.parse(sets.getString("DATE_UP")).getTime();
				for (Iterator iterator = this.callCarTroubleAreaPlanMap.keySet().iterator(); iterator.hasNext();) {
					 int id = (Integer)iterator.next();
					 CallCarTroubleAreaPlan callCarTroubleAreaPlan = (CallCarTroubleAreaPlan)this.callCarTroubleAreaPlanMap.get(id);
					 startTime = GeneralConst.YYYY_MM_DD_HH_MM_SS.parse(GeneralConst.YYYY_MM_DD.format(sTime)+" "+callCarTroubleAreaPlan.start_time.trim()+":00").getTime();
					 endTime = GeneralConst.YYYY_MM_DD_HH_MM_SS.parse(GeneralConst.YYYY_MM_DD.format(sTime)+" "+callCarTroubleAreaPlan.end_time.trim()+":00").getTime();
					 if(dateup<startTime||dateup>endTime){
						 continue;
					 }
					 singleBusinessDataBs = new SingleBusinessDataBs();
					 singleBusinessDataBs.id = sets.getString("id");
					 singleBusinessDataBs.dest_no = sets.getString("DISPATCH_CAR_NO");
					 singleBusinessDataBs.service_no = sets.getString("SERVICE_NO");
					 singleBusinessDataBs.date_down = GeneralConst.YYYY_MM_DD_HH_MM_SS.parse(sets.getString("DATE_DOWN")).getTime();
					 singleBusinessDataBs.date_up = dateup;
					 callCarTroubleAreaPlan.singleBusinessDataBsMap.put(singleBusinessDataBs.id, singleBusinessDataBs);
					 this.callCarTroubleAreaPlanMap.put(id, callCarTroubleAreaPlan);
				}
			}
		}catch (Exception e) {
			e.printStackTrace();
			return false;
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
		return true;
	}
	public static double countDistance(double lo1, double la1, double lo2,double la2)
	{
		double radLat1 = rad(la1);
		double radLat2 = rad(la2);
		double a = radLat1 - radLat2;
		double b = rad(lo1) - rad(lo2);
		double s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2), 2)
				+ Math.cos(radLat1) * Math.cos(radLat2)
				* Math.pow(Math.sin(b / 2), 2)));
		s = s * EARTH_RADIUS;
		s = Math.round(s * 100000) / 100000.0;
		return s;
	}
	private static double EARTH_RADIUS = 6378.137;
	private static double rad(double d)
	{
		return d * Math.PI / 180.0;
	}
	public class CallCarTroubleAreaPlan{
		public int plan_id;
		public String plan_name;
		public String start_time;//考核开始时间
		public String end_time;//考核结束时间
	    public Date start_date;//考核开始日期
	    public Date end_date;//考核结束日期
	    public int area_id;//打车难区域区域编号
	    public int status;
	    public HashMap<String, SingleBusinessDataBs> singleBusinessDataBsMap = new HashMap<String, SingleBusinessDataBs>();
	    public Hashtable carMap=new Hashtable();
	}
	public class SingleBusinessDataBs{
		public String id;
		public String service_no;
		public String dest_no;
		public long date_up;
		public long date_down;
	}
	public class CallCarTroubleAreaPassInfo{
		public String id;
		public int plan_id;
		public int area_id;
		public Timestamp assess_date;
		public String service_no;
		public String dest_no;
		public int com_id;
		public int server_id;
		public long nul_in_time;//空车进入时间
		public long zhong_time_n;//区域内出现重车时间
		public long leave_time;//出区域时间
		public long stop_dura;//区域停留时长
		public long down_time;//乘客下车时间
		public long zhong_run_time;//重车营运时长
		public double zhong_run_mile;//重车经历里程
	}
}
