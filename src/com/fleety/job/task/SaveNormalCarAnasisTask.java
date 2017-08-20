package com.fleety.job.task;

import java.sql.ResultSet;
import java.sql.SQLException;

import server.db.DbServer;

import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.fleety.util.pool.thread.BasicTask;

public class SaveNormalCarAnasisTask extends BasicTask {


	@Override
	public boolean execute() throws Exception {
		saveNormalCar();
		return true;
	}
	private boolean isDbExist(String carNo) {
		DbHandle conn = null;
		try {
			conn = DbServer.getSingleInstance().getConn();
			
			String sql = "select company_name from fault_car_count where car_no=? and record_time=sysdate-1 ";
			StatementHandle stmt = conn.prepareStatement(sql);
			stmt.setString(1, carNo);
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
				if(!isDbExist(car_no)){
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
