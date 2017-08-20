package com.fleety.analysis.feedback.task;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;

import javax.management.Query;

import server.db.DbServer;
import server.threadgroup.PoolInfo;
import server.threadgroup.ThreadPoolGroupServer;
import server.track.TrackServer;
import server.var.VarManageServer;

import com.fleety.analysis.AnalysisServer;
import com.fleety.analysis.feedback.DestInfo;
import com.fleety.analysis.feedback.FeedBackAnalysis;
import com.fleety.base.GeneralConst;
import com.fleety.base.InfoContainer;
import com.fleety.base.StrFilter;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.fleety.util.pool.thread.BasicTask;
import com.fleety.util.pool.thread.ThreadPool;
import com.fleety.util.pool.timer.FleetyTimerTask;

public class FeedBackInfoAnalysis implements FeedBackAnalysis {
	private HashMap feedBackMapping = null;
	private HashMap termMap = null;
	private HashMap carNumStatMapping = null;
	private ThreadPool pool = null;
	private String poolName = "feedback_date_save_pool";
	private double standard_1 = 0.05;
	private double standard_2 = 0.05;
	private double standard_3 = 0.05;
	private double standard_4 = 0.05;
	
	public void init(Date sDate,Date eDate){
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
		try {
			this.getDestMapping();
			this.findAllCarNumStat(sDate);
			this.queryCarCount();
			this.queryGpsExceCount(sDate);
			this.queryGpsNoQualifiedCount(sDate);
			this.queryOperateExceCount(sDate);
			this.queryOperateNoQualifiedCount(sDate);
			this.queryOperateDataCount(sDate);
		} catch (Exception e) {
			System.out.println("表数据加载失败！！");
			e.printStackTrace();
		}
	}
	public boolean startAnalysisFeedBack(AnalysisServer parentServer,Date sDate,Date eDate) {
		this.init(sDate,eDate);
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

	public void analysisDestFeedBack(AnalysisServer parentServer,Date sDate,Date eDate) {
		try {
			this.gpsFeedBackAnalysis(sDate, eDate);
			this.operateFeedBackAnalysis(sDate, eDate);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void endAnalysisFeedBack(AnalysisServer parentServer,Date sDate,Date eDate) {
		if(this.feedBackMapping==null){
			return;
		}
		this.saveFeedBackInfo();
	}
	public void gpsFeedBackAnalysis(Date sDate,Date eDate) throws Exception{
		HashMap<Integer, Integer> termNumMap = new HashMap<Integer, Integer>();
		for (Iterator iterator = termMap.keySet().iterator(); iterator.hasNext();) {
			int comId = (Integer)iterator.next();
			termNumMap.put(comId, 0);
		}
		DestInfo destInfo = null; 
		for (Iterator iterator2 = destMapping.keySet().iterator(); iterator2.hasNext();) {
			String car_no = (String) iterator2.next();
			destInfo = (DestInfo)destMapping.get(car_no);
			if(!carNumStatMapping.containsKey(car_no)){
				continue;
			}
			CarNumStat carNumStat = (CarNumStat)carNumStatMapping.get(car_no);
			if(termNumMap.get(destInfo.getCompanyId())!=null){
				int temp = (Integer)termNumMap.get(destInfo.getCompanyId());
				temp+=carNumStat.point_total_num;
				termNumMap.put(destInfo.getCompanyId(), temp);
			}
		}
		FeedBackData feedBackData = null;
		Term term = null;
		for (Iterator iterator = termNumMap.keySet().iterator(); iterator.hasNext();) {
			int comId = (Integer)iterator.next();
			int normalNum = termNumMap.get(comId);
			int exceNum = 0;
			if(gpsExceCount.containsKey(comId)){
				exceNum = gpsExceCount.get(comId);
			}
			int noQualifiedNum = 0;
			if(gpsNoQualifiedCount.containsKey(comId)){
				noQualifiedNum = gpsNoQualifiedCount.get(comId);
			}
			double carNum = 0;
			if(carCount.containsKey(comId)){
				carNum = carCount.get(comId);
			}
			double total = normalNum + exceNum;
			if(total > 0){
				if(exceNum/(total)>this.standard_1){
					feedBackData = new FeedBackData();
					feedBackData.company_id = comId;
					term = (Term)termMap.get(feedBackData.company_id);
					feedBackData.company_name = term.term_name;
					feedBackData.no_qualified_num = exceNum;
					feedBackData.total_num = (int)total;
					feedBackData.type = 0;
					feedBackData.query_time = new Timestamp(sDate.getTime());
					feedBackData.standard = this.standard_1;
					feedBackMapping.put(comId+"_"+feedBackData.type, feedBackData);
				}
			}
			if(carNum > 0){
				if(noQualifiedNum/carNum>this.standard_3){
					feedBackData = new FeedBackData();
					feedBackData.company_id = comId;
					term = (Term)termMap.get(feedBackData.company_id);
					feedBackData.company_name = term.term_name;
					feedBackData.no_qualified_num = noQualifiedNum;
					feedBackData.total_num = (int)carNum;
					feedBackData.type = 2;
					feedBackData.query_time = new Timestamp(sDate.getTime());
					feedBackData.standard = this.standard_3;
					feedBackMapping.put(comId+"_"+feedBackData.type, feedBackData);
				}
			}
		}
	}
	private void operateFeedBackAnalysis(Date sDate, Date eDate) throws Exception{
		FeedBackData feedBackData = null;
		Term term = null;
		int i=0;
		for (Iterator iterator = termMap.keySet().iterator(); iterator.hasNext();) {
			int comId = (Integer)iterator.next();
			int exceNum = 0;
			if(operateExceCount.containsKey(comId)){
				exceNum = operateExceCount.get(comId);
			}
			int noQualifiedNum = 0;
			if(operateNoQualifiedCount.containsKey(comId)){
				noQualifiedNum = operateNoQualifiedCount.get(comId);
			}
			int operateNum = 0;
			if(operateDataCount.containsKey(comId)){
				operateNum = operateDataCount.get(comId);
			}
			double totalNum = exceNum + operateNum;
			double carNum = 0;
			if(carCount.containsKey(comId)){
				carNum = carCount.get(comId);
			}
			if(totalNum > 0){
				if(exceNum/totalNum>this.standard_2){
					feedBackData = new FeedBackData();
					feedBackData.company_id = comId;
					term = (Term)termMap.get(feedBackData.company_id);
					feedBackData.company_name = term.term_name;
					feedBackData.no_qualified_num = exceNum;
					feedBackData.total_num = (int)totalNum;
					feedBackData.type = 1;
					feedBackData.query_time = new Timestamp(sDate.getTime());
					feedBackData.standard = this.standard_2;
					feedBackMapping.put(comId+"_"+feedBackData.type, feedBackData);
				}
			}
			if(carNum > 0){
				if(noQualifiedNum/carNum>this.standard_4){
					feedBackData = new FeedBackData();
					feedBackData.company_id = comId;
					term = (Term)termMap.get(feedBackData.company_id);
					feedBackData.company_name = term.term_name;
					feedBackData.no_qualified_num = noQualifiedNum;
					feedBackData.total_num = (int)carNum;
					feedBackData.type = 3;
					feedBackData.query_time = new Timestamp(sDate.getTime());
					feedBackData.standard = this.standard_4;
					feedBackMapping.put(comId+"_"+feedBackData.type, feedBackData);
				}
			}
		}
	}
	private HashMap<Integer,Integer> gpsExceCount = null;
	public void queryGpsExceCount(Date sDate) throws Exception{
		gpsExceCount = new HashMap<Integer,Integer>();
		DbHandle conn = DbServer.getSingleInstance().getConn();
		StatementHandle stmt = conn.createStatement();
		ResultSet sets = stmt.executeQuery("select count(*) sum,company_id from gps_exception_data " +
				" where query_time>=to_date('"+GeneralConst.YYYY_MM_DD.format(sDate)+" 00:00:00','yyyy-MM-dd hh24:mi:ss') and query_time<=to_date('"+GeneralConst.YYYY_MM_DD.format(sDate)+" 23:59:59','yyyy-MM-dd hh24:mi:ss')"+
				" group by company_id ");
		while (sets.next()) {
			gpsExceCount.put(sets.getInt("company_id"), sets.getInt("sum"));
		}
		DbServer.getSingleInstance().releaseConn(conn);
	}
	private HashMap<Integer, Integer> operateExceCount = null;
	public void queryOperateExceCount(Date sDate) throws Exception{
		operateExceCount = new HashMap<Integer, Integer>();
		DbHandle conn = DbServer.getSingleInstance().getConn();
		StatementHandle stmt = conn.createStatement();
		ResultSet sets = stmt.executeQuery("select count(*) sum,company_id from operation_exception_data " +
				" where query_time>=to_date('"+GeneralConst.YYYY_MM_DD.format(sDate)+" 00:00:00','yyyy-MM-dd hh24:mi:ss') and query_time<=to_date('"+GeneralConst.YYYY_MM_DD.format(sDate)+" 23:59:59','yyyy-MM-dd hh24:mi:ss')"+
				" group by company_id ");
		while (sets.next()) {
			operateExceCount.put(sets.getInt("company_id"), sets.getInt("sum"));
		}
		DbServer.getSingleInstance().releaseConn(conn);
	}
	private HashMap<Integer, Integer> gpsNoQualifiedCount = null;
	public void queryGpsNoQualifiedCount(Date sDate) throws Exception{
		gpsNoQualifiedCount = new HashMap<Integer, Integer>();
		DbHandle conn = DbServer.getSingleInstance().getConn();
		StatementHandle stmt = conn.createStatement();
		ResultSet sets = stmt.executeQuery("select count(*) sum,company_id from (select car_no,min(company_id) company_id from no_qualified_data  " +
				" where query_time>=to_date('"+GeneralConst.YYYY_MM_DD.format(sDate)+" 00:00:00','yyyy-MM-dd hh24:mi:ss') and query_time<=to_date('"+GeneralConst.YYYY_MM_DD.format(sDate)+" 23:59:59','yyyy-MM-dd hh24:mi:ss')  and type<=4 group by car_no) "+
				" group by company_id ");
		while (sets.next()) {
			gpsNoQualifiedCount.put(sets.getInt("company_id"), sets.getInt("sum"));
		}
		DbServer.getSingleInstance().releaseConn(conn);
	}
	private HashMap<Integer, Integer> operateNoQualifiedCount = null;
	public void queryOperateNoQualifiedCount(Date sDate) throws Exception{
		operateNoQualifiedCount = new HashMap<Integer, Integer>();
		DbHandle conn = DbServer.getSingleInstance().getConn();
		StatementHandle stmt = conn.createStatement();
		ResultSet sets = stmt.executeQuery("select count(*) sum,company_id from (select car_no,min(company_id) company_id from no_qualified_data " +
				" where query_time>=to_date('"+GeneralConst.YYYY_MM_DD.format(sDate)+" 00:00:00','yyyy-MM-dd hh24:mi:ss') and query_time<=to_date('"+GeneralConst.YYYY_MM_DD.format(sDate)+" 23:59:59','yyyy-MM-dd hh24:mi:ss')  and type>=5 group by car_no) "+
				" group by company_id ");
		while (sets.next()) {
			operateNoQualifiedCount.put(sets.getInt("company_id"), sets.getInt("sum"));
		}
		DbServer.getSingleInstance().releaseConn(conn);
	}
	private HashMap<Integer, Integer> operateDataCount = null;
	public void queryOperateDataCount(Date sDate) throws Exception{
		operateDataCount = new HashMap<Integer, Integer>();
		DbHandle conn = DbServer.getSingleInstance().getConn();
		StatementHandle stmt = conn.createStatement();
		ResultSet sets = stmt.executeQuery("select count(*) sum,taxi_company company_id from single_business_data_bs " +
				" where date_up>=to_date('"+GeneralConst.YYYY_MM_DD.format(sDate)+" 00:00:00','yyyy-MM-dd hh24:mi:ss') and date_up<=to_date('"+GeneralConst.YYYY_MM_DD.format(sDate)+" 23:59:59','yyyy-MM-dd hh24:mi:ss')" +
				" group by taxi_company");
		while (sets.next()) {
			operateDataCount.put(sets.getInt("company_id"), sets.getInt("sum"));
		}
		DbServer.getSingleInstance().releaseConn(conn);
	}
	private HashMap<Integer, Integer> carCount = null;
	public void queryCarCount() throws Exception{
		carCount = new HashMap<Integer, Integer>();
		DbHandle conn = DbServer.getSingleInstance().getConn();
		StatementHandle stmt = conn.createStatement();
		ResultSet sets = stmt.executeQuery("select count(*) sum,TERM_ID company_id from car group by TERM_ID");
		while (sets.next()) {
			carCount.put(sets.getInt("company_id"), sets.getInt("sum"));
		}
		DbServer.getSingleInstance().releaseConn(conn);
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
			String sql = "insert into no_qualified_feedback_info (id,company_id,company_name,query_time,total_num,no_qualified_num,standard,type,status) values(?,?,?,?,?,?,?,?,?)";
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
				stmt.setDouble(7, feedBackData.standard);
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
	private HashMap destMapping = null;
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
	private void findAllCarNumStat(Date sDate){
		HashMap tempMapping = new HashMap();
		CarNumStat carNumStat = null;
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			String sql = "select * from ANA_CAR_NUM_STAT_DETAIL  where stat_time>=to_date('"+GeneralConst.YYYY_MM_DD.format(sDate)+" 00:00:00','yyyy-MM-dd hh24:mi:ss') and stat_time<=to_date('"+GeneralConst.YYYY_MM_DD.format(sDate)+" 23:59:59','yyyy-MM-dd hh24:mi:ss')";
			StatementHandle stmt = conn.createStatement();
			ResultSet sets = stmt.executeQuery(sql);
			int mdtId;
			while (sets.next()) {
				mdtId = sets.getInt("mdt_id");
				if(mdtId < 10){
					continue;
				}
				mdtId = Integer.parseInt((mdtId+"").substring(1));
				
				carNumStat = new CarNumStat();
				carNumStat.point_total_num = sets.getInt("point_total_num");
				tempMapping.put(sets.getString("car_no"), carNumStat);
			}
			carNumStatMapping = tempMapping;
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("从数据库[ANA_CAR_NUM_STAT_DETAIL]加载车辆信息失败！");
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
	}
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
	private class CarNumStat{
		public int point_total_num;
		
	}
}
