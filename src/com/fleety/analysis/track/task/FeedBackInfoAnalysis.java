package com.fleety.analysis.track.task;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;

import server.db.DbServer;
import server.var.VarManageServer;

import com.fleety.analysis.AnalysisServer;
import com.fleety.analysis.track.ITrackAnalysis;
import com.fleety.analysis.track.TrackInfo;
import com.fleety.base.GeneralConst;
import com.fleety.base.InfoContainer;
import com.fleety.base.StrFilter;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;

public class FeedBackInfoAnalysis  implements ITrackAnalysis{
	private HashMap feedBackMapping = null;
	private HashMap termMap = null;
	private HashMap destMapping = null;
	private double standard_1 = 0.05;
	private double standard_2 = 0.05;
	private double standard_3 = 0.05;
	private double standard_4 = 0.05;
	public void init(){
		this.feedBackMapping = null;
		String temp = VarManageServer.getSingleInstance().getVarStringValue(
				"standard_1");
		if (StrFilter.hasValue(temp)) {
			try {
				this.standard_1 = Double.parseDouble(temp)/100d;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		temp = VarManageServer.getSingleInstance().getVarStringValue(
				"standard_2");
		if (StrFilter.hasValue(temp)) {
			try {
				this.standard_2 = Double.parseDouble(temp)/100d;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		temp = VarManageServer.getSingleInstance().getVarStringValue(
				"standard_3");
		if (StrFilter.hasValue(temp)) {
			try {
				this.standard_3 = Double.parseDouble(temp)/100d;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		temp = VarManageServer.getSingleInstance().getVarStringValue(
				"standard_4");
		if (StrFilter.hasValue(temp)) {
			try {
				this.standard_4 = Double.parseDouble(temp)/100d;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		this.getDestMapping();
	}
	@Override
	public boolean startAnalysisTrack(AnalysisServer parentServer,
			InfoContainer statInfo) {
		this.init();
		Date sDate = statInfo.getDate(ITrackAnalysis.STAT_START_TIME_DATE);
		Date eDate = statInfo.getDate(ITrackAnalysis.STAT_END_TIME_DATE);
		System.out.println(GeneralConst.YYYY_MM_DD_HH_MM_SS.format(sDate));
		System.out.println(GeneralConst.YYYY_MM_DD_HH_MM_SS.format(eDate));
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			StatementHandle stmt = conn.createStatement();
			StringBuilder sb = new StringBuilder();
			sb.append("select * from no_qualified_feedback_info where  ")
				.append("     query_time >= to_date('"+GeneralConst.YYYY_MM_DD.format(sDate)+" 00:00:00','yyyy-MM-dd hh24:mi:ss')")
				.append(" and query_time <= to_date('"+GeneralConst.YYYY_MM_DD.format(eDate)+" 23:59:59','yyyy-MM-dd hh24:mi:ss')");
			ResultSet sets = stmt.executeQuery(sb.toString());
			if (!sets.next()) {
				this.feedBackMapping = new HashMap();
			}
			termMap = this.queryCompany();
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}
		if (this.feedBackMapping == null) {
			System.out.println("Not Need Analysis:" + this.toString());
		} else {
			System.out.println("Start Analysis:" + this.toString());
		}
		return this.feedBackMapping != null;
	}

	@Override
	public void analysisDestTrack(AnalysisServer parentServer,
			TrackInfo trackInfo) {
		Date sDate = trackInfo.sDate;
		Date eDate = trackInfo.eDate;
		try {
			this.gpsFeedBackAnalysis(trackInfo,sDate, eDate);
			this.operateFeedBackAnalysis(sDate, eDate);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	@Override
	public void endAnalysisTrack(AnalysisServer parentServer,
			InfoContainer statInfo) {
		if(this.feedBackMapping==null){
			return;
		}
		this.saveFeedBackInfo();
	}
	public void gpsFeedBackAnalysis(TrackInfo trackInfo,Date sDate,Date eDate) throws Exception{
		HashMap<Integer, Integer> termNumMap = new HashMap<Integer, Integer>();
		for (Iterator iterator = termMap.keySet().iterator(); iterator.hasNext();) {
			int comId = (Integer)iterator.next();
			termNumMap.put(comId, 0);
		}
		String car_no = trackInfo.dInfo.destNo;
		DestInfo destInfo = (DestInfo)destMapping.get(car_no);
		InfoContainer[] trackArr = trackInfo.trackArr;
		if(termNumMap.get(destInfo.getCompanyId())!=null){
			int temp = (Integer)termNumMap.get(destInfo.getCompanyId());
			temp+=trackArr.length;
			termNumMap.put(destInfo.getCompanyId(), temp);
		}
		FeedBackData feedBackData = null;
		Term term = null;
		for (Iterator iterator = termNumMap.keySet().iterator(); iterator.hasNext();) {
			int comId = (Integer)iterator.next();
			int normalNum = termNumMap.get(comId);
			int exceNum = this.queryGpsExceCount(comId, sDate);
			int noQualifiedNum = this.queryGpsNoQualifiedCount(comId, sDate);
			int total = normalNum + exceNum;
			if(total <= 0){
				continue;
			}
			if(exceNum/(total)>this.standard_1){
				feedBackData = new FeedBackData();
				feedBackData.company_id = comId;
				term = (Term)termMap.get(feedBackData.company_id);
				feedBackData.company_name = term.term_name;
				feedBackData.no_qualified_num = exceNum;
				feedBackData.total_num = total;
				feedBackData.type = 0;
				feedBackData.query_time = new Timestamp(sDate.getTime());
				feedBackData.standard = this.standard_1;
				feedBackMapping.put(comId+"_"+feedBackData.type, feedBackData);
			}
			if(normalNum <= 0){
				continue;
			}
			if(noQualifiedNum/normalNum>this.standard_3){
				feedBackData = new FeedBackData();
				feedBackData.company_id = comId;
				term = (Term)termMap.get(feedBackData.company_id);
				feedBackData.company_name = term.term_name;
				feedBackData.no_qualified_num = noQualifiedNum;
				feedBackData.total_num = normalNum;
				feedBackData.type = 2;
				feedBackData.query_time = new Timestamp(sDate.getTime());
				feedBackData.standard = this.standard_3;
				feedBackMapping.put(comId+"_"+feedBackData.type, feedBackData);
			}
		}
	}
	private void operateFeedBackAnalysis(Date sDate, Date eDate) throws Exception{
		FeedBackData feedBackData = null;
		Term term = null;
		int i=0;
		for (Iterator iterator = termMap.keySet().iterator(); iterator.hasNext();) {
			int comId = (Integer)iterator.next();
			int exceNum = this.queryOperateExceCount(comId, sDate);
			int noQualifiedNum = this.queryOperateNoQualifiedCount(comId, sDate);
			int operateNum = this.queryOperateDataCount(comId, sDate);
			int totalNum = exceNum + operateNum;
			if(totalNum <= 0){
				continue;
			}
			if(exceNum/(totalNum)>this.standard_2){
				feedBackData = new FeedBackData();
				feedBackData.company_id = comId;
				term = (Term)termMap.get(feedBackData.company_id);
				feedBackData.company_name = term.term_name;
				feedBackData.no_qualified_num = exceNum;
				feedBackData.total_num = totalNum;
				feedBackData.type = 1;
				feedBackData.query_time = new Timestamp(sDate.getTime());
				feedBackData.standard = this.standard_2;
				feedBackMapping.put(comId+"_"+feedBackData.type, feedBackData);
			}
			if(operateNum <= 0){
				continue;
			}
			if(noQualifiedNum/operateNum>this.standard_4){
				feedBackData = new FeedBackData();
				feedBackData.company_id = comId;
				term = (Term)termMap.get(feedBackData.company_id);
				feedBackData.company_name = term.term_name;
				feedBackData.no_qualified_num = noQualifiedNum;
				feedBackData.total_num = operateNum;
				feedBackData.type = 3;
				feedBackData.query_time = new Timestamp(sDate.getTime());
				feedBackData.standard = this.standard_4;
				feedBackMapping.put(comId+"_"+feedBackData.type, feedBackData);
			}
		}
	}
	public int queryGpsExceCount(int comId,Date sDate) throws Exception{
		int count = 0;
		DbHandle conn = DbServer.getSingleInstance().getConn();
		StatementHandle stmt = conn.createStatement();
		ResultSet sets = stmt.executeQuery("select count(*) sum from gps_exception_data where company_id="+comId+" and query_time>=to_date('"+GeneralConst.YYYY_MM_DD.format(sDate)+" 00:00:00','yyyy-MM-dd hh24:mi:ss') and query_time<=to_date('"+GeneralConst.YYYY_MM_DD.format(sDate)+" 23:59:59','yyyy-MM-dd hh24:mi:ss')");
		while (sets.next()) {
			count = sets.getInt("sum");
		}
		DbServer.getSingleInstance().releaseConn(conn);
		return count;
	}
	public int queryOperateExceCount(int comId,Date sDate) throws Exception{
		int count = 0;
		DbHandle conn = DbServer.getSingleInstance().getConn();
		StatementHandle stmt = conn.createStatement();
		ResultSet sets = stmt.executeQuery("select count(*) sum from operation_exception_data where company_id="+comId+" and query_time>=to_date('"+GeneralConst.YYYY_MM_DD.format(sDate)+" 00:00:00','yyyy-MM-dd hh24:mi:ss') and query_time<=to_date('"+GeneralConst.YYYY_MM_DD.format(sDate)+" 23:59:59','yyyy-MM-dd hh24:mi:ss')");
		while (sets.next()) {
			count = sets.getInt("sum");
		}
		DbServer.getSingleInstance().releaseConn(conn);
		return count;
	}
	public int queryGpsNoQualifiedCount(int comId,Date sDate) throws Exception{
		int count = 0;
		DbHandle conn = DbServer.getSingleInstance().getConn();
		StatementHandle stmt = conn.createStatement();
		ResultSet sets = stmt.executeQuery("select count(*) sum from no_qualified_data where company_id="+comId+" and query_time>=to_date('"+GeneralConst.YYYY_MM_DD.format(sDate)+" 00:00:00','yyyy-MM-dd hh24:mi:ss') and query_time<=to_date('"+GeneralConst.YYYY_MM_DD.format(sDate)+" 23:59:59','yyyy-MM-dd hh24:mi:ss') and type in (1,2,3,4,8)");
		while (sets.next()) {
			count = sets.getInt("sum");
		}
		DbServer.getSingleInstance().releaseConn(conn);
		return count;
	}
	public int queryOperateNoQualifiedCount(int comId,Date sDate) throws Exception{
		int count = 0;
		DbHandle conn = DbServer.getSingleInstance().getConn();
		StatementHandle stmt = conn.createStatement();
		ResultSet sets = stmt.executeQuery("select count(*) sum from no_qualified_data where company_id="+comId+" and query_time>=to_date('"+GeneralConst.YYYY_MM_DD.format(sDate)+" 00:00:00','yyyy-MM-dd hh24:mi:ss') and query_time<=to_date('"+GeneralConst.YYYY_MM_DD.format(sDate)+" 23:59:59','yyyy-MM-dd hh24:mi:ss') and type in (5,6,7) ");
		while (sets.next()) {
			count = sets.getInt("sum");
		}
		DbServer.getSingleInstance().releaseConn(conn);
		return count;
	}
	public int queryOperateDataCount(int comId,Date sDate) throws Exception{
		int count = 0;
		DbHandle conn = DbServer.getSingleInstance().getConn();
		StatementHandle stmt = conn.createStatement();
		ResultSet sets = stmt.executeQuery("select count(*) sum from single_business_data_bs where taxi_company="+comId+" and date_up>=to_date('"+GeneralConst.YYYY_MM_DD.format(sDate)+" 00:00:00','yyyy-MM-dd hh24:mi:ss') and date_up<=to_date('"+GeneralConst.YYYY_MM_DD.format(sDate)+" 23:59:59','yyyy-MM-dd hh24:mi:ss')");
		while (sets.next()) {
			count = sets.getInt("sum");
		}
		DbServer.getSingleInstance().releaseConn(conn);
		return count;
	}
	public HashMap queryCompany() throws Exception {
		HashMap termMap =  new HashMap();
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			StatementHandle stmt = conn.createStatement();
			ResultSet sets = stmt.executeQuery("select * from term");
			while (sets.next()) {
				Term term = new Term();
				term.term_id = sets.getInt("term_id");
				term.term_name = sets.getString("term_name");
				termMap.put(term.term_id,term);
			}
			
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
		return termMap;
	}

	private void saveFeedBackInfo(){
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			conn.setAutoCommit(false);
			String sql = "insert into no_qualified_feedback_info (id,company_id,company_name,query_time,total_num,no_qualified_num,standard,type,status) values(?,?,?,?,?,?,?,?,?,?)";
			StatementHandle stmt = conn.prepareStatement(sql);
			int count = 0;
			System.out.println("Start FeedBackDataSaveTask:");
			for (Iterator iterator = feedBackMapping.keySet().iterator(); iterator
			.hasNext();) {
				String com_type = (String)iterator.next();
				FeedBackData feedBackData = (FeedBackData)feedBackMapping.get(com_type);
				stmt.setInt(1, (int)DbServer.getSingleInstance().getAvaliableId(conn, "no_qualified_feedback_info", "id"));
				stmt.setInt(2, feedBackData.company_id);
				stmt.setString(3, feedBackData.company_name);
				stmt.setTimestamp(4, feedBackData.query_time);
				stmt.setInt(5, feedBackData.total_num);
				stmt.setInt(6, feedBackData.no_qualified_num);
				stmt.setDouble(7, feedBackData.standard*50);
				stmt.setInt(8, feedBackData.type);
				stmt.setInt(9, 0);
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
			try {
				conn.rollback();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			System.out.println("数据库操作失败！");
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
	}
	private void getDestMapping(){
			HashMap tempMapping = new HashMap();
			DestInfo destInfo = null;
			DbHandle conn = DbServer.getSingleInstance().getConn();
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
	private class FeedBackData{
		public int id;
		public int company_id;
		public String company_name;
		public Timestamp query_time;
		public int total_num;
		public int no_qualified_num;
		public double standard;
		public int type;
		public int status;
	}
	private class Term{
		public int term_id;
		public String term_name;
	}
}
