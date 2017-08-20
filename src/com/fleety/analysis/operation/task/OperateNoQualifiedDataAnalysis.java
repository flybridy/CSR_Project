package com.fleety.analysis.operation.task;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;

import server.db.DbServer;
import server.var.VarManageServer;

import com.fleety.analysis.AnalysisServer;
import com.fleety.analysis.operation.IOperationAnalysis;
import com.fleety.analysis.track.DestInfo;
import com.fleety.base.GeneralConst;
import com.fleety.base.InfoContainer;
import com.fleety.base.StrFilter;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;

public class OperateNoQualifiedDataAnalysis implements IOperationAnalysis{
	private HashMap  noQualifiedMapping = null;
	private int operateNum = 1;
	private long waitTime = 12*60*60;
	private int type_5 = 5;//营运笔数
	private int type_6 = 6;//营运时间交叉
	private int type_7 = 7;//营运等候时长
	
	@Override
	public boolean startAnalysisOperation(AnalysisServer parentServer,
			InfoContainer statInfo) {
		this.noQualifiedMapping = null;
		String temp = VarManageServer.getSingleInstance().getVarStringValue(
		"continuationOperateNum");
		if (StrFilter.hasValue(temp)) {
			try {
				this.operateNum = Integer.parseInt(temp);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		temp = VarManageServer.getSingleInstance().getVarStringValue(
		"continuationWaitTime");
		if (StrFilter.hasValue(temp)) {
			try {
				this.waitTime = Long.parseLong(temp)*60*60;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
		
		DbHandle conn = DbServer.getSingleInstance().getConnWithUseTime(0);
		try{
			StringBuilder sb = new StringBuilder();
			sb.append("select * from no_qualified_data ")
			  .append(" where query_time>=? and query_time<?")
			  .append(" and type >= 5 and type <= 7 and rownum<2");

			StatementHandle stmt = conn.prepareStatement(sb.toString());
			stmt.setTimestamp(1, new Timestamp(sTime.getTime()));
			stmt.setTimestamp(2, new Timestamp(sTime.getTime()+GeneralConst.ONE_DAY_TIME));
			ResultSet sets = stmt.executeQuery();
			if(!sets.next()){
				this.noQualifiedMapping = new HashMap();
			}
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
		
		if(this.noQualifiedMapping == null){
			System.out.println("Not Need Analysis:"+this.toString());
		}else{
			System.out.println("Start Analysis:"+this.toString());
		}
		return this.noQualifiedMapping != null;
	}

	@Override
	public void analysisDestOperation(AnalysisServer parentServer,
			InfoContainer statInfo) {
		DbHandle conn = DbServer.getSingleInstance().getConn();
		ArrayList destList = this.queryDestInfo();
		try{
			StatementHandle stmt = conn.createStatement();
			HashMap vehicleOperateMappings = new HashMap();
			List<VehicleOperateInfo> vehicleOperateMapping;
			Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
			Date eTime = statInfo.getDate(STAT_END_TIME_DATE);
			System.out.println(GeneralConst.YYYY_MM_DD_HH_MM_SS.format(sTime));
			Calendar time = Calendar.getInstance();
			time.setTime(sTime);
			StringBuilder sql = new StringBuilder();
			sql.append(" select s.id,s.dispatch_car_no plate_no,s.taxi_company company_id,t.term_name company_name,s.waiting_hour,s.waiting_minute,s.waiting_second,s.date_up,s.date_down,s.recode_time from ( ")
			.append(" select * from single_business_data_bs")
			.append(" where date_up>=to_date('"+GeneralConst.YYYY_MM_DD_HH_MM_SS.format(sTime)+"','yyyy-MM-dd hh24:mi:ss')")
			.append(" and date_up<=to_date('"+GeneralConst.YYYY_MM_DD_HH_MM_SS.format(eTime)+"','yyyy-MM-dd hh24:mi:ss')) s")
			.append(" left join term t on s.taxi_company=t.term_id ")
			.append(" order by dispatch_car_no,date_up,date_down");
			System.out.println(sql.toString());
			ResultSet rs = stmt.executeQuery(sql.toString());
			while(rs.next())
			{
				String plateNo = rs.getString("plate_no");
				if(vehicleOperateMappings.containsKey(plateNo)){
					vehicleOperateMapping = (List<VehicleOperateInfo>)vehicleOperateMappings.get(plateNo);
				}else {
					vehicleOperateMapping = new ArrayList<VehicleOperateInfo>();
					vehicleOperateMappings.put(plateNo, vehicleOperateMapping);
				}
				VehicleOperateInfo vInfo = new VehicleOperateInfo();
				vInfo.id = rs.getInt("id");
				vInfo.plateNo = plateNo;
				vInfo.companyId = rs.getInt("company_id");
				vInfo.companyName = rs.getString("company_name");
				vInfo.waitingHour = rs.getInt("waiting_hour");
				vInfo.waitingMinute = rs.getInt("waiting_minute");
				vInfo.waitingSecond = rs.getInt("waiting_second");
				vInfo.date_up = rs.getTimestamp("date_up");
				vInfo.date_down = rs.getTimestamp("date_down");
				vInfo.recode_time = rs.getTimestamp("recode_time");
				vehicleOperateMapping.add(vInfo);
			}
			
			for (int i = 0; i < destList.size(); i++) {
				DestInfo destInfo = (DestInfo)destList.get(i);
				this.continuationOperateNum(statInfo,(List<VehicleOperateInfo>)vehicleOperateMappings.get(destInfo.destNo),destInfo);
				if(!vehicleOperateMappings.containsKey(destInfo.destNo)){
					continue;
				}
				this.continuationOperateTime(statInfo, (List<VehicleOperateInfo>)vehicleOperateMappings.get(destInfo.destNo), destInfo);
				this.continuationOperateWaitTime(statInfo, (List<VehicleOperateInfo>)vehicleOperateMappings.get(destInfo.destNo), destInfo);
			}
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
		
	}
	//营运时间交叉
	private void continuationOperateTime(InfoContainer statInfo,List<VehicleOperateInfo> vehicleOperateMapping,DestInfo destInfo){
		if(vehicleOperateMapping==null){
			return;
		}
		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
		long total = 0;
		long startTime = 0;
		long endTime = 0;
		long startTime1 = 0;
		long endTime1 = 0;
		int biao = 1;
		for (int i = 0; i < vehicleOperateMapping.size(); i++) {
			VehicleOperateInfo vehicleOperateInfo = vehicleOperateMapping.get(i);
			if(biao==1){
				startTime = vehicleOperateInfo.date_up.getTime();
				endTime = vehicleOperateInfo.date_down.getTime();
				biao++;
				continue;
			}
			startTime1 = vehicleOperateInfo.date_up.getTime();
			endTime1 = vehicleOperateInfo.date_down.getTime();
			if(startTime1 < endTime){
				biao=-1;
				break;
			}
			startTime = startTime1;
			endTime = endTime1;
			biao++;
		}
		if(biao>0){
			return;
		}
		NoQualifiedData noQualifiedData = new NoQualifiedData();
		noQualifiedData.carNo = destInfo.destNo;
		noQualifiedData.companyId = destInfo.companyId;
		noQualifiedData.companyName = destInfo.companyName;
		noQualifiedData.runComId = destInfo.gpsRunComId;
		noQualifiedData.runComName = destInfo.gpsRunComName;
		noQualifiedData.queryTime = GeneralConst.YYYY_MM_DD.format(sTime.getTime());
		//交叉时间段：start_end
		String start_end = GeneralConst.YYYYMMDDHHMMSS.format(startTime)+"-"+
		GeneralConst.YYYYMMDDHHMMSS.format(endTime)+"~"+
		GeneralConst.YYYYMMDDHHMMSS.format(startTime1)+"-"+
		GeneralConst.YYYYMMDDHHMMSS.format(endTime1);
		JSONObject jsonObject = new JSONObject();
		try {
			jsonObject.put("start_end", start_end);
		} catch (JSONException e) {
			e.printStackTrace();
		}
		noQualifiedData.remark = jsonObject.toString();
		noQualifiedData.type = this.type_6;
		noQualifiedMapping.put(destInfo.destNo+"_"+noQualifiedData.type, noQualifiedData);
		
	}
	//营运笔数
	private void continuationOperateNum(InfoContainer statInfo,List<VehicleOperateInfo> vehicleOperateMapping,DestInfo destInfo){
		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
		int vsize = 0;
		if(vehicleOperateMapping!=null){
			vsize = vehicleOperateMapping.size();
		}
		if(vsize>=this.operateNum){
			return;
		}
		NoQualifiedData noQualifiedData = new NoQualifiedData();
		noQualifiedData.carNo = destInfo.destNo;
		noQualifiedData.companyId = destInfo.companyId;
		noQualifiedData.companyName = destInfo.companyName;
		noQualifiedData.runComId = destInfo.gpsRunComId;
		noQualifiedData.runComName = destInfo.gpsRunComName;
		noQualifiedData.queryTime = GeneralConst.YYYY_MM_DD.format(sTime.getTime());
		//营运笔数：vsize 营运笔数标准：operateNum
		JSONObject jsonObject = new JSONObject();
		try {
			jsonObject.put("vsize", vsize);
			jsonObject.put("operateNum", this.operateNum);
		} catch (JSONException e) {
			e.printStackTrace();
		}
		noQualifiedData.remark = jsonObject.toString();
		noQualifiedData.type = this.type_5;
		noQualifiedMapping.put(destInfo.destNo+"_"+noQualifiedData.type, noQualifiedData);
		
	}
	//营运等候时长
	private void continuationOperateWaitTime(InfoContainer statInfo,List<VehicleOperateInfo> vehicleOperateMapping,DestInfo destInfo){
		if(vehicleOperateMapping==null){
			return;
		}
		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
		long total = 0;
		for (int i = 0; i < vehicleOperateMapping.size(); i++) {
			VehicleOperateInfo vehicleOperateInfo = vehicleOperateMapping.get(i);
			total = total + vehicleOperateInfo.waitingHour*60*60+vehicleOperateInfo.waitingMinute*60+vehicleOperateInfo.waitingSecond;
		}
		if(total<this.waitTime){
			return;
		}
		NoQualifiedData noQualifiedData = new NoQualifiedData();
		noQualifiedData.carNo = destInfo.destNo;
		noQualifiedData.companyId = destInfo.companyId;
		noQualifiedData.companyName = destInfo.companyName;
		noQualifiedData.runComId = destInfo.gpsRunComId;
		noQualifiedData.runComName = destInfo.gpsRunComName;
		noQualifiedData.queryTime = GeneralConst.YYYY_MM_DD.format(sTime.getTime());
		//等候时长：total 等候时长标准：waitTime
		JSONObject jsonObject = new JSONObject();
		try {
			jsonObject.put("total", total);
			jsonObject.put("waitTime", this.waitTime);
		} catch (JSONException e) {
			e.printStackTrace();
		}
		noQualifiedData.remark = jsonObject.toString();
		noQualifiedData.type = this.type_7;
		noQualifiedMapping.put(destInfo.destNo+"_"+noQualifiedData.type, noQualifiedData);
	}

	@Override
	public void endAnalysisOperation(AnalysisServer parentServer,
			InfoContainer statInfo) {
		if(this.noQualifiedMapping == null){ 
			return ;
		}
		int count = 0;
		String carNo = "";
		NoQualifiedData noQualifiedData;
		Date sDate = statInfo.getDate(STAT_START_TIME_DATE);
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try{
			conn.setAutoCommit(false);
			StatementHandle stmt = conn.prepareStatement("insert into no_qualified_data(id,car_no,company_id,company_name,run_com_id,run_com_name,query_time,remark,record_time,type) values(?,?,?,?,?,?,?,?,?,?)");
			for(Iterator itr = this.noQualifiedMapping.keySet().iterator();itr.hasNext();){
				carNo = (String)itr.next();
				noQualifiedData = (NoQualifiedData)this.noQualifiedMapping.get(carNo);
				if(noQualifiedData.runComName==null||noQualifiedData.runComName.equals("")){
					continue;
				}
				stmt.setInt(1, (int)DbServer.getSingleInstance().getAvaliableId(conn, "no_qualified_data", "id"));
				stmt.setString(2, noQualifiedData.carNo);
				stmt.setInt(3, noQualifiedData.companyId);
				stmt.setString(4, noQualifiedData.companyName);
				stmt.setInt(5, noQualifiedData.runComId);
				stmt.setString(6, noQualifiedData.runComName);
				stmt.setDate(7, new java.sql.Date(GeneralConst.YYYY_MM_DD_HH_MM_SS.parse(noQualifiedData.queryTime + " 00:00:00").getTime()));
				stmt.setString(8, noQualifiedData.remark);
				stmt.setTimestamp(9, new Timestamp(new Date().getTime()));
				stmt.setInt(10, noQualifiedData.type);
				stmt.addBatch();
				if(count%200==0){
					stmt.executeBatch();
				}
				count++;
			}
			stmt.executeBatch();
			conn.commit();
		}catch(Exception e){
			e.printStackTrace();
			if(conn != null){
				try{
					conn.rollback();
				}catch(Exception ee){
					ee.printStackTrace();
				}
			}
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
			this.noQualifiedMapping = null;
		}
		System.out.println("End Analysis Date:"+GeneralConst.YYYY_MM_DD_HH_MM_SS.format(new Date().getTime()));
	}
	
	private ArrayList queryDestInfo() {
		DbHandle conn = DbServer.getSingleInstance().getConn();
		ArrayList destList = new ArrayList();
		DestInfo dInfo;
		try{
			StatementHandle stmt = conn.prepareStatement("select mdt_id,dest_no,company_id,company_name,type_id,gps_run_com_id,gps_run_com_name from v_ana_dest_info");
			ResultSet sets = stmt.executeQuery();
			while(sets.next()){
				dInfo = new DestInfo();
				dInfo.mdtId = sets.getInt("mdt_id");
				dInfo.destNo = sets.getString("dest_no");
				dInfo.companyId = sets.getInt("company_id");
				dInfo.companyName = sets.getString("company_name");
				dInfo.gpsRunComId = sets.getInt("gps_run_com_id");
				dInfo.gpsRunComName = sets.getString("gps_run_com_name");
				dInfo.carType=sets.getInt("type_id");
				destList.add(dInfo);
			}
		}catch (Exception e) {
			e.printStackTrace();
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
		return destList;
	}

	private class NoQualifiedData{
		public int id;
		public String carNo;
		public int companyId;
		public String companyName;
		public int runComId;
		public String runComName;
		public String queryTime;
		public int type;
		public String remark;
		public String recordTime;
	}
	private class VehicleOperateInfo
	{
		public int id;
		public String plateNo;
		public int    companyId;
		public String companyName;
		public int    waitingHour;
		public int    waitingMinute;
		public int    waitingSecond;
		public Date   date_up;
		public Date   date_down;
		public Date   recode_time;
	}
}
