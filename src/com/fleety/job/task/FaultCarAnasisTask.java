package com.fleety.job.task;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;

import org.json.JSONObject;

import server.db.DbServer;

import com.fleety.base.GeneralConst;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.fleety.util.pool.thread.BasicTask;

public class FaultCarAnasisTask extends BasicTask {


	@Override
	public boolean execute() throws Exception {
		saveFaultCar();
		return true;
	}
	private void saveFaultCar() {
		DbHandle conn = null;
		try {
			conn = DbServer.getSingleInstance().getConn();
			
			String sql = "select car_no,company_id,company_name,run_com_name,'有效点数不合格' type,to_char(query_time,'yyyy-MM-dd') query_time,remark,to_char(record_time,'yyyy-MM-dd hh24:mi:ss') record_time from no_qualified_data where type=3   ";
			String sql1="insert into fault_car_count (id,car_no,company_id,company_name,run_com_name,record_time,validnum,com_car_num,normal_time,fault_time) values(SEQ_FAULT_CAR.NEXTVAL,?,?,?,?,?,?,?,?,?)";
			StatementHandle stmts = conn.prepareStatement(sql1);
			StatementHandle stmt = conn.prepareStatement(sql);
			ResultSet rs = stmt.executeQuery();
			while (rs.next()) {
				String car_no=rs.getString("car_no");
				String company_name=rs.getString("company_name");
				String com_name=rs.getString("run_com_name");
				String record_time=rs.getString("record_time");
				int company_id=rs.getInt("company_id");
				JSONObject jsonObject = new JSONObject(rs.getString("remark"));
				int value=jsonObject.getInt("validnum");
				if(!isDbExist(car_no,new java.sql.Date(
						GeneralConst.YYYY_MM_DD.parse(
								record_time)
								.getTime()))){
				stmts.setString(1, car_no);
				stmts.setInt(2, company_id);
				stmts.setString(3, company_name);
				stmts.setString(4, com_name);
				
				stmts.setDate(5, new java.sql.Date(
						GeneralConst.YYYY_MM_DD.parse(
								record_time)
								.getTime()));
				stmts.setInt(6, value>0?1:0);
				stmts.setInt(7,  getCarNum(company_id));
				stmts.setInt(8, value>0?1:0);
				stmts.setInt(9, value>0?0:1);
				stmts.execute();
				}
			}

		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}
	}
	private boolean isDbExist(String carNo, Date lastReportTime) {
		DbHandle conn = null;
		try {
			conn = DbServer.getSingleInstance().getConn();
			
			String sql = "select company_name from fault_car_count where car_no=? and record_time=? ";
			StatementHandle stmt = conn.prepareStatement(sql);
			stmt.setString(1, carNo);
			stmt.setTimestamp(2, new Timestamp(lastReportTime.getTime()/1000*1000));
			ResultSet rs = stmt.executeQuery();
			if (rs.next()) {
				return true;
			}

		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}
		return false;
	}
	private int getCarNum(int company_id){
		String sql="select count(*) from car where term_id= "+company_id+" and mdt_id>-1";
		DbHandle conn=DbServer.getSingleInstance().getConn();
		try {
			StatementHandle stmt =conn.prepareStatement(sql);
			ResultSet set=stmt.executeQuery();
			if(set.next()){
				int num=set.getInt("count(*)");
				return num;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
		return 0;
		
	}
	private void saveNormalCar(){
		String sql="select car_id,term_id,company from car where  mdt_id>-1";
		String sql1="insert into fault_car_count (id,car_no,company_id,company_name,run_com_name,record_time,validnum,com_car_num,normal_time,fault_time) values(SEQ_FAULT_CAR.NEXTVAL,?,?,?,'',sysdate-1,1,?,1,0)";
		DbHandle conn= DbServer.getSingleInstance().getConnWithUseTime(0);
		try {
			StatementHandle stmt= conn.prepareStatement(sql);
			StatementHandle stmt1= conn.prepareStatement(sql1);
			ResultSet sets= stmt.executeQuery();
			while(sets.next()){
				String car_no=sets.getString("car_id");
				int company_id=sets.getInt("term_id");
				String company=sets.getString("company");
				int car_num=getCarNum(company_id);
				if(!isDbExist(car_no,new Date())){
					stmt1.setString(1, car_no);
					stmt1.setInt(2, company_id);
					stmt1.setString(3, company);
					stmt1.setInt(4, car_num);
					stmt1.execute();
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
	}
}
