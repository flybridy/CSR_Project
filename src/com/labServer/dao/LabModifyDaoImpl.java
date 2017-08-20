package com.labServer.dao;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;

import server.db.DbServer;

public class LabModifyDaoImpl implements LabModifyDao {

	@Override
	public ResultSet findLabModify() {
		DbHandle con = DbServer.getSingleInstance().getConn();
		StatementHandle stmt;
		ResultSet sets = null;
		String sql = "select * from lab_modify";
		try {
			stmt = con.prepareStatement(sql);
			sets = stmt.executeQuery();
			DbServer.getSingleInstance().releaseConn(con);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return sets;
	}

}
