package com.labServer.dao;

import java.sql.SQLException;
import java.util.List;

import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.labServer.model.LabDisplayParamter;

import server.db.DbServer;

public class LabDisplayParamterDaoImpl implements LabDisplayParamterDao {
	/**
	 * ������ʾ��ݻ��ܱ�
	 * 
	 */
	public void addListItemsToSumDisplay(List<LabDisplayParamter> list) {
		DbHandle con = DbServer.getSingleInstance().getConn();
		StatementHandle stmt;
		try {
			String sql = "insert into lab_displayparamter (INPUTPROBENUMBER,DISPROBENUMBER,CREATEDON,DISTEMPERATURE,DISHUMIDITY)VALUES(?,?,?,?,?)";
			stmt = con.prepareStatement(sql);
			con.setAutoCommit(false);
			for (int i = 0; i < list.size(); i++) {
				stmt.setString(1, list.get(i).getInputProbeNumber());
				stmt.setString(2, list.get(i).getDisProbeNumber());
				stmt.setString(3, list.get(i).getCreatedOn());
				stmt.setDouble(4, list.get(i).getDisTemperature());
				stmt.setDouble(5, list.get(i).getDisHumidity());
				stmt.addBatch();
			}
			stmt.executeBatch();
			con.commit();
			DbServer.getSingleInstance().releaseConn(con);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	
	public void addListItemsToDiffDisplay(List<LabDisplayParamter> list) {
		DbHandle con = DbServer.getSingleInstance().getConn();
		StatementHandle stmt;
		StatementHandle Stmt2;
		try {
			//String sql = "insert into ? (INPUTPROBENUMBER,DISPROBENUMBER,CREATEDON,DISTEMPERATURE,DISHUMIDITY)VALUES(?,?,?,?,?)";
			//stmt = con.prepareStatement(sql);
			Stmt2=con.createStatement();
			con.setAutoCommit(false);
			LabDisplayParamter lab;
			String sql=null;
			for (int i = 0; i < list.size(); i++) {
				lab=list.get(i);
				/*stmt.setString(1, list.get(i).getDisplayTableName());
				stmt.setString(2, list.get(i).getInputProbeNumber());
				stmt.setString(3, list.get(i).getDisProbeNumber());
				stmt.setString(4, list.get(i).getCreatedOn());
				stmt.setDouble(5, list.get(i).getDisTemperature());
				stmt.setDouble(6, list.get(i).getDisHumidity());*/
				sql="insert into "+lab.getDisplayTableName()+" (INPUTPROBENUMBER,DISPROBENUMBER,CREATEDON,DISTEMPERATURE,DISHUMIDITY)VALUES('"+ lab.getInputProbeNumber()+"','"+lab.getDisProbeNumber()+"','"+lab.getCreatedOn()+"',"+lab.getDisTemperature()+","+lab.getDisHumidity()+")";
				//System.out.println(sql);
				Stmt2.addBatch(sql);				
				//stmt.addBatch();
			}
			Stmt2.executeBatch();
			con.commit();
			DbServer.getSingleInstance().releaseConn(con);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
}
