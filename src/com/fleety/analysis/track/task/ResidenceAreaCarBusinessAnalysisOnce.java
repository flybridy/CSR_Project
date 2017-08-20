package com.fleety.analysis.track.task;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import server.db.DbServer;

import com.fleety.analysis.AnalysisServer;
import com.fleety.analysis.track.ITrackAnalysis;
import com.fleety.analysis.track.TrackInfo;
import com.fleety.base.GeneralConst;
import com.fleety.base.InfoContainer;
import com.fleety.server.area.AreaDataLoadServer;
import com.fleety.server.area.AreaInfo;
import com.fleety.server.area.BindCarInfo;
import com.fleety.server.area.IsResidenceArea;
import com.fleety.server.area.PlanInfo;
import com.fleety.track.TrackIO;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;

public class ResidenceAreaCarBusinessAnalysisOnce implements ITrackAnalysis {
	private HashMap residenceAreaBusinessMap = null;
	private HashMap<Integer,AreaInfo> areaData = null;
	private HashMap<String,List<SingleBusinessDataBs>> singleMap = null;
	private SimpleDateFormat sdf             = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private SimpleDateFormat sdf2            = new SimpleDateFormat("yyyy-MM-dd");
	public boolean startAnalysisTrack(AnalysisServer parentServer,
			InfoContainer statInfo) {
		this.residenceAreaBusinessMap = null;
		this.areaData = null;
		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
		DbHandle conn = DbServer.getSingleInstance().getConn();
		
		try {
			StringBuilder sb = new StringBuilder();
			sb.append("select * from residence_area_business_day ").append(
					" where statistics_date>=? and statistics_date<?");
			System.out.println("查询是否要分析："+sb.toString());
			StatementHandle stmt = conn.prepareStatement(sb.toString());
			stmt.setTimestamp(1, new Timestamp(sTime.getTime()));
			stmt.setTimestamp(2, new Timestamp(sTime.getTime()+GeneralConst.ONE_DAY_TIME));
			ResultSet sets = stmt.executeQuery();
			if (!sets.next()) {
				this.residenceAreaBusinessMap = new HashMap();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}
		
		if (this.residenceAreaBusinessMap == null) {
			System.out.println("Not Need Analysis:" + this.toString());
			return false;
		} else {
			System.out.println("Start Analysis:begin:::"+sdf.format(sTime) +"日数据 "+ this.toString());
		}
		try{
			autoStartPlan();
			
			autoStopPlan();
			
			AreaDataLoadServer aa = new AreaDataLoadServer();
			
			areaData = aa.getAreaData(100, true);
			
			if(areaData==null){
				
				throw new NullPointerException("areaData is null");
			}
			this.querySingleBusinessDataBs(sTime);
			if(singleMap==null) {
				throw new NullPointerException("singleMap is null");
			}
		}catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return this.residenceAreaBusinessMap != null;
	}
	
	public void analysisDestTrack(AnalysisServer parentServer,
			TrackInfo trackInfo) {		
		if (this.residenceAreaBusinessMap == null) {
			return;
		}
		if (trackInfo.trackArr == null) {
			return;
		}
		String yesterday = GeneralConst.YYYY_MM_DD.format(trackInfo.sDate);
		String destNo = trackInfo.dInfo.destNo;
		long planstart = 0;//计划开始时间
		long planend = 0;//计划结束时间
		for (Iterator iterator = areaData.values().iterator(); iterator.hasNext();) {
			AreaInfo areaInfo = (AreaInfo) iterator.next();
			if(areaInfo==null){
				System.out.println("areaInfo is null");
				continue;
			}
			HashMap<Integer,PlanInfo> pInfos = areaInfo.getpInfos();
			if(pInfos==null){
				continue;
			}
			for (Iterator iterator2 = pInfos.values().iterator(); iterator2
					.hasNext();) {
				PlanInfo planInfo = (PlanInfo) iterator2.next();
				if(planInfo==null){
					continue;
				}
				try {
					planstart = GeneralConst.YYYY_MM_DD_HH_MM_SS.parse(yesterday + " " + planInfo.getStart_time1()+":00").getTime();
					planend = GeneralConst.YYYY_MM_DD_HH_MM_SS.parse(yesterday + " " + planInfo.getEnd_time1()+":59").getTime();
				} catch (ParseException e) {
					e.printStackTrace();
				}
				HashMap<String,BindCarInfo> bInfos = planInfo.getbInfos();
				if(bInfos==null){
					continue;
				}
				if(!bInfos.containsKey(destNo)){
					continue;
				}
				double preKilo = -1;
				IsDateN isDateN = new IsDateN();
				IsDateN isDateN1 = null;
				IsResidenceArea firstPoint = null;
				IsResidenceArea secondPoint = null;
				IsResidenceArea firstPointStatus = null;
				IsResidenceArea secondPointStatus = null;
				ResidenceAreaBusinessDay residenceAreaBusinessDay = new ResidenceAreaBusinessDay();
				residenceAreaBusinessDay.area_id = areaInfo.getAreaId();
				residenceAreaBusinessDay.dest_no = destNo;
				residenceAreaBusinessDay.plan_id = planInfo.getPlan_id();
				residenceAreaBusinessDay.com_id = trackInfo.dInfo.companyId;
				residenceAreaBusinessDay.com_name = trackInfo.dInfo.companyName;
				residenceAreaBusinessDay.server_id = trackInfo.dInfo.gpsRunComId;
				residenceAreaBusinessDay.server_name = trackInfo.dInfo.gpsRunComName;
				int j = 0;
				for (int i = 0; i < trackInfo.trackArr.length; i++) {
					secondPoint = new IsResidenceArea(trackInfo.trackArr[i].getDouble(TrackIO.DEST_LO_FLAG),trackInfo.trackArr[i].getDouble(TrackIO.DEST_LA_FLAG),areaInfo.getAreaShape());
					secondPoint.setStatus((trackInfo.trackArr[i].getInteger(TrackIO.DEST_STATUS_FLAG)&0x0F));
					secondPoint.setTime(trackInfo.trackArr[i].getDate(TrackIO.DEST_TIME_FLAG).getTime());
					if(secondPoint.getTime() < planstart){
						j= i + 1;
						continue;
					}
					if(secondPoint.getTime() > planend){
						continue;
					}
					if(preKilo >= 0){
						double dis = countDistance((double)firstPoint.getLo()/10000000,(double)firstPoint.getLa()/10000000,(double)secondPoint.getLo()/10000000,(double)secondPoint.getLa()/10000000);
						if(dis < 10){
							preKilo += dis;
						}
					}else{
						preKilo = 0;
					}
					secondPoint.setMile(preKilo);
					if(i==j){
						firstPoint = new IsResidenceArea(secondPoint.getLo(), secondPoint.getLa(), secondPoint.getShape(),secondPoint.isNw());
						firstPoint.setStatus(secondPoint.getStatus());
						firstPoint.setMile(secondPoint.getMile());
						firstPoint.setTime(secondPoint.getTime());
						continue;
					}
					//进入区域
					if(firstPoint.isNw()!=secondPoint.isNw()&&!firstPoint.isNw()){
						residenceAreaBusinessDay.total_num_j = residenceAreaBusinessDay.total_num_j + 1;
						if(firstPoint.getStatus()==0||firstPoint.getStatus()==2){
							residenceAreaBusinessDay.kong_num_j = residenceAreaBusinessDay.kong_num_j + 1;
						}else if (firstPoint.getStatus()==1) {
							residenceAreaBusinessDay.zhong_num_j = residenceAreaBusinessDay.zhong_num_j + 1;
						}
					//出区域
					}else if(firstPoint.isNw()!=secondPoint.isNw()&&firstPoint.isNw()) {
						residenceAreaBusinessDay.total_num_l += 1;
						if(firstPoint.getStatus()==0||firstPoint.getStatus()==2){
							residenceAreaBusinessDay.kong_num_l += 1;
						}else if (firstPoint.getStatus()==1) {
							residenceAreaBusinessDay.zhong_num_l += 1;
						}
					}
					if((firstPoint.getStatus()==0||firstPoint.getStatus()==2)&&secondPoint.getStatus()==1){
						firstPointStatus =  new IsResidenceArea(secondPoint.getLo(), secondPoint.getLa(), secondPoint.getShape(),secondPoint.isNw());
						firstPointStatus.setStatus(secondPoint.getStatus());
						firstPointStatus.setMile(secondPoint.getMile());
						firstPointStatus.setTime(secondPoint.getTime());
						secondPointStatus = null;
					}else if (firstPoint.getStatus()==1&&(secondPoint.getStatus()==0||secondPoint.getStatus()==2)) {
						secondPointStatus =  new IsResidenceArea(secondPoint.getLo(), secondPoint.getLa(), secondPoint.getShape(),secondPoint.isNw());
						secondPointStatus.setStatus(secondPoint.getStatus());
						secondPointStatus.setMile(secondPoint.getMile());
						secondPointStatus.setTime(secondPoint.getTime());
					}
					if(firstPointStatus!=null&&secondPointStatus!=null){
						isDateN1 = new IsDateN();
						isDateN1.sTime = firstPointStatus.getTime();
						isDateN1.eTime = secondPointStatus.getTime();
						if(firstPointStatus.isNw()){
							residenceAreaBusinessDay.run_dura_n += (secondPointStatus.getTime() - firstPointStatus.getTime());
							residenceAreaBusinessDay.run_num_n += 1;
							isDateN.list.add(isDateN1);
						}else {
							residenceAreaBusinessDay.run_dura_w += (secondPointStatus.getTime() - firstPointStatus.getTime());
							residenceAreaBusinessDay.run_num_w += 1;
						}
						firstPointStatus = null;
						secondPointStatus = null;
					}
					if(firstPoint.isNw()){
						residenceAreaBusinessDay.total_mile_n += secondPoint.getMile()-firstPoint.getMile();
						residenceAreaBusinessDay.total_dura_n += (secondPoint.getTime() - firstPoint.getTime());
						if(firstPoint.getStatus()==0||firstPoint.getStatus()==2){
							residenceAreaBusinessDay.nul_mile_n += secondPoint.getMile()-firstPoint.getMile();
						}
					}else {
						residenceAreaBusinessDay.total_mile_w += secondPoint.getMile()-firstPoint.getMile();
						residenceAreaBusinessDay.total_dura_w += (secondPoint.getTime() - firstPoint.getTime());
						if(firstPoint.getStatus()==0||firstPoint.getStatus()==2){
							residenceAreaBusinessDay.nul_mile_w += secondPoint.getMile()-firstPoint.getMile();
						}
					}
					
					firstPoint = new IsResidenceArea(secondPoint.getLo(), secondPoint.getLa(), secondPoint.getShape(),secondPoint.isNw());
					firstPoint.setStatus(secondPoint.getStatus());
					firstPoint.setMile(secondPoint.getMile());
					firstPoint.setTime(secondPoint.getTime());
				}
				List<SingleBusinessDataBs> singlelist = singleMap.get(destNo);
				SingleBusinessDataBs singleBusinessDataBs = null;
				if(singlelist!=null){
					for (int i = 0; i < singlelist.size(); i++) {
						singleBusinessDataBs = singlelist.get(i);
						if(isDateN.isIn(singleBusinessDataBs.date_up)){
							residenceAreaBusinessDay.run_money_n += singleBusinessDataBs.sum;
						}else {
							residenceAreaBusinessDay.run_money_w += singleBusinessDataBs.sum;
						}
					}
				}
				Date sDate = trackInfo.sDate;
				residenceAreaBusinessDay.statistics_date = new java.sql.Date(sDate.getTime());
				residenceAreaBusinessMap.put(destNo, residenceAreaBusinessDay);
			}
		}
		
	}
	private double querySingleBusinessDataBs(Date sTime) {
		DbHandle conn = DbServer.getSingleInstance().getConn();
		singleMap = null;
		singleMap = new HashMap<String, List<SingleBusinessDataBs>>();
		HashMap<Integer, Integer> is_nddemap=new HashMap<Integer, Integer>();//判定考核计划是否在判定范围内
		List<SingleBusinessDataBs> singleList = null;
		SingleBusinessDataBs singleBusinessDataBs = null;
		String sqlcheck="select id,start_time,end_time from car_area_plan_info";
		
		String sql = "select s.dispatch_car_no,s.date_up,s.sum,cb.plan_id from single_business_data_bs s left join car_area_plan_bind_info cb on cb.car_no=s.dispatch_car_no where  s.date_up >= to_date('"+GeneralConst.YYYY_MM_DD.format(sTime)+"'||' 00:00:00','yyyy-MM-dd hh24:mi:ss') and s.date_up <= to_date('"+GeneralConst.YYYY_MM_DD.format(sTime)+"'||' 23:59:59','yyyy-MM-dd hh24:mi:ss')";
		System.out.println("查询计划:"+sql);
		try {
			StatementHandle stmt1 = conn.prepareStatement(sqlcheck);
			ResultSet sets1 = stmt1.executeQuery();
			
			while(sets1.next()){
				int plan_id=sets1.getInt("id");
				Date start=sets1.getDate("start_time");
				Date end=sets1.getDate("end_time");
				if(sTime.getTime()>=start.getTime()&&sTime.getTime()<=end.getTime()){
					is_nddemap.put(plan_id, 1);
				}
			}
			sets1.close();
			stmt1.close();
			StatementHandle stmt = conn.prepareStatement(sql);
			ResultSet sets = stmt.executeQuery();
			while (sets.next()) {
				int p_id=sets.getInt(4);
				if(is_nddemap.containsKey(p_id)){
					String carNo = sets.getString(1);
					if(singleMap.containsKey(carNo)){
						singleList = singleMap.get(carNo);
					}else {
						singleList = new ArrayList<SingleBusinessDataBs>();
					}
					singleBusinessDataBs = new SingleBusinessDataBs();
					singleBusinessDataBs.date_up = GeneralConst.YYYY_MM_DD_HH_MM_SS.parse(sets.getString(2)).getTime();
					singleBusinessDataBs.sum = sets.getDouble(3);
					singleList.add(singleBusinessDataBs);
					singleMap.put(carNo, singleList);
				}
				
			}
			sets.close();
			stmt.close();
		} catch (Exception e) {
			System.out.println("查询result异常");
			e.printStackTrace();
			return 0;
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
		return 0;
	}
	
	public void endAnalysisTrack(AnalysisServer parentServer,
			InfoContainer statInfo) {
		if (this.residenceAreaBusinessMap == null) {
			return;
		}
		int count = 1;
		String temp = "";
		ResidenceAreaBusinessDay residenceAreaBusinessDay;
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			conn.setAutoCommit(false);
			StatementHandle stmt = conn.prepareStatement("insert into residence_area_business_day(id,statistics_date,dest_no," +
				"area_id,com_id,com_name,server_id,server_name,run_dura_n,run_dura_w,total_mile_n,total_mile_w,nul_mile_n," +
				"nul_mile_w,run_num_n,run_num_w,run_money_n,run_money_w,total_num_j,total_num_l,kong_num_j,kong_num_l," +
				"zhong_num_j,zhong_num_l,record_time,total_dura_n,total_dura_w,plan_id) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
			for (Iterator itr = this.residenceAreaBusinessMap.keySet().iterator(); itr
					.hasNext();) {
				temp = (String) itr.next();
				residenceAreaBusinessDay = (ResidenceAreaBusinessDay) this.residenceAreaBusinessMap.get(temp);
				stmt.setInt(1, (int) DbServer.getSingleInstance().getAvaliableId(conn, "residence_area_business_day", "id"));
				stmt.setDate(2, residenceAreaBusinessDay.statistics_date);
				stmt.setString(3, residenceAreaBusinessDay.dest_no);
				stmt.setInt(4, residenceAreaBusinessDay.area_id);
				stmt.setInt(5, residenceAreaBusinessDay.com_id);
				stmt.setString(6, residenceAreaBusinessDay.com_name);
				stmt.setInt(7, residenceAreaBusinessDay.server_id);
				stmt.setString(8, residenceAreaBusinessDay.server_name);
				stmt.setInt(9, residenceAreaBusinessDay.run_dura_n/60000);
				stmt.setInt(10, residenceAreaBusinessDay.run_dura_w/60000);
				stmt.setDouble(11, residenceAreaBusinessDay.total_mile_n);
				stmt.setDouble(12, residenceAreaBusinessDay.total_mile_w);
				stmt.setDouble(13, residenceAreaBusinessDay.nul_mile_n);
				stmt.setDouble(14, residenceAreaBusinessDay.nul_mile_w);
				stmt.setInt(15, residenceAreaBusinessDay.run_num_n);
				stmt.setInt(16, residenceAreaBusinessDay.run_num_w);
				stmt.setDouble(17, residenceAreaBusinessDay.run_money_n);
				stmt.setDouble(18, residenceAreaBusinessDay.run_money_w);
				stmt.setInt(19, residenceAreaBusinessDay.total_num_j);
				stmt.setInt(20, residenceAreaBusinessDay.total_num_l);
				stmt.setInt(21, residenceAreaBusinessDay.kong_num_j);
				stmt.setInt(22, residenceAreaBusinessDay.kong_num_l);
				stmt.setInt(23, residenceAreaBusinessDay.zhong_num_j);
				stmt.setInt(24, residenceAreaBusinessDay.zhong_num_l);
				stmt.setTimestamp(25, residenceAreaBusinessDay.record_time);
				stmt.setInt(26, residenceAreaBusinessDay.total_dura_n/60000);
				stmt.setInt(27, residenceAreaBusinessDay.total_dura_w/60000);
				stmt.setInt(28, residenceAreaBusinessDay.plan_id);
				stmt.addBatch();
				if (count % 200 == 0) {
					stmt.executeBatch();
				}
				count++;
			}
			stmt.executeBatch();
			conn.commit();
		} catch (Exception e) {
			e.printStackTrace();
			if (conn != null) {
				try {
					conn.rollback();
				} catch (Exception ee) {
					ee.printStackTrace();
				}
			}
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}
		System.out.println("End Analysis:end:::"+sdf.format(new Date()) + this.toString());
	}
	private class IsDateN{
		public long sTime;
		public long eTime;
		List<IsDateN> list = new ArrayList<IsDateN>();
		public boolean isExist = false;
		
		public boolean isIn(long time){
			if(this.isExist){
				this.isExist = false;
			}
			IsDateN isDateN = null;
			for (int j = 0; j < list.size();j++) {
				isDateN = list.get(j);
				//System.out.println(GeneralConst.YYYY_MM_DD_HH_MM_SS.format(isDateN.sTime)+" "+GeneralConst.YYYY_MM_DD_HH_MM_SS.format(time)+" "+GeneralConst.YYYY_MM_DD_HH_MM_SS.format(isDateN.eTime));
				this.isExist = time >= isDateN.sTime && time <= isDateN.eTime;
				if(this.isExist){
					return this.isExist;
				}
			}
			return this.isExist;
		}
	}
	private class SingleBusinessDataBs{
		public long date_up;
		public double sum;
	}
	private class ResidenceAreaBusinessDay {
		public int id;
		public java.sql.Date statistics_date;//统计日期
		public String dest_no;//车牌号
		public int area_id;//驻点区域编号
		public int com_id;//企业编号
		public int plan_id;
		public String com_name;//企业名
		public int server_id;//运营商编号
		public String server_name;//运营商名
		public int run_dura_n = 0;//驻点区域内营运时长
		public int run_dura_w = 0;//驻点区域外营运时长    
		public double total_mile_n = 0;//驻点区域内总里程
		public double total_mile_w = 0;//驻点区域外总里程
		public double nul_mile_n = 0;//驻点区域内空驶里程
		public double nul_mile_w = 0;//驻点区域外空驶里程
		public int run_num_n = 0;//驻点区域内营运次数
		public int run_num_w = 0;//驻点区域外营运次数
		public double run_money_n = 0;//驻点区域内营运金额
		public double run_money_w = 0;//驻点区域外营运金额
		public int total_num_j = 0;//车辆进入区域总次数
		public int total_num_l = 0;//车辆离开区域总次数
		public int kong_num_j = 0;//空车进入区域总次数
		public int kong_num_l = 0;//空车离开区域总次数
		public int zhong_num_j = 0;//重车进入区域总次数
		public int zhong_num_l = 0;//重车离开区域总次数
		private int total_dura_n = 0;//区域内停留时长
		private int total_dura_w = 0;//区域外停留时长
		public Timestamp record_time;//记录时间
	}
	private void autoStartPlan() throws Exception{
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try 
		{
			String sql = "update car_area_plan_info set status=1 where  status=0 and start_time<=?";
			Calendar calStart = Calendar.getInstance();
			calStart.set(Calendar.HOUR_OF_DAY, 0);
			calStart.set(Calendar.MINUTE, 0);
			calStart.set(Calendar.SECOND, 0);
			StatementHandle stmt = conn.prepareStatement(sql);
			stmt.setTimestamp(1, new java.sql.Timestamp(calStart.getTimeInMillis()));
			stmt.executeUpdate();
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
	}
	private void autoStopPlan() throws Exception{
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try 
		{
			String sql = "update car_area_plan_info set status=2 where  status=1 and end_time<?";
			Calendar calStart = Calendar.getInstance();
			calStart.set(Calendar.HOUR_OF_DAY, 0);
			calStart.set(Calendar.MINUTE, 0);
			calStart.set(Calendar.SECOND, 0);
			StatementHandle stmt = conn.prepareStatement(sql);
			stmt.setTimestamp(1, new java.sql.Timestamp(calStart.getTimeInMillis()));
			stmt.executeUpdate();
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
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
}
