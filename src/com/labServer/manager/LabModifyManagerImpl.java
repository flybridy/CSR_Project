package com.labServer.manager;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.labServer.dao.LabModifyDaoImpl;
import com.labServer.model.LabModify;

import server.db.DbServer;

public class LabModifyManagerImpl implements LabModifyManager {

	LabModifyDaoImpl labModifyDaoImpl = new LabModifyDaoImpl();

	/**
	 * 获取校准值表所有数据（现用）
	 *
	 *
	 */
	public Map<String, LabModify> resultSetToMapFromModify() {
		//ResultSet rs = labModifyDaoImpl.findLabModify();

		Map<String, LabModify> modMap = new HashMap<String, LabModify>();
		DbHandle con = DbServer.getSingleInstance().getConn();
		StatementHandle stmt;
		ResultSet rs = null;
		String sql = "select * from lab_modify";
		try {
			stmt = con.prepareStatement(sql);
			rs = stmt.executeQuery();					
			// 得到结果集(rs)的结构信息，比如字段数、字段名等
			while (rs.next()) {
				LabModify lm = new LabModify();
				lm.setInputProbeNumber(rs.getString("inputProbeNumber"));
				lm.setDisProbeNumber(rs.getString("disProbeNumber"));
				lm.setModifyTemp(rs.getDouble("modifyTemp"));
				lm.setModifyHum(rs.getDouble("modifyHum"));	
				modMap.put(lm.getInputProbeNumber(), lm);
			}		
			DbServer.getSingleInstance().releaseConn(con);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return modMap;
	}

}
