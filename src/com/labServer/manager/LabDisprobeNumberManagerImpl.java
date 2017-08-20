package com.labServer.manager;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.labServer.dao.LabDisprobeNumberDao;
import com.labServer.model.LabDisprobeNumber;

import server.db.DbServer;

public class LabDisprobeNumberManagerImpl implements LabDisprobeNumberManager {

	LabDisprobeNumberDao labDisprobeNumberDao;

	public Map<String, LabDisprobeNumber> resultSetToListFromDisProbe() {

		//ResultSet rs = labDisprobeNumberDao.findLabDisprobeNumber();

		DbHandle con = DbServer.getSingleInstance().getConn();
		StatementHandle stmt;
		ResultSet rs=null ;
		String sql = "select * from lab_disprobenumber";				
		try {
			stmt = con.prepareStatement(sql);
			rs = stmt.executeQuery();			
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
		
		
		Map<String, LabDisprobeNumber> disMap = new HashMap<String, LabDisprobeNumber>();
		
		try {
			
				// 得到结果集(rs)的结构信息，比如字段数、字段名等
				//int columnCount = md.getColumnCount(); // 返回此 ResultSet 对象中的列数
				while (rs.next()) {
					LabDisprobeNumber ld = new LabDisprobeNumber();
					ld.setInputProbeNumber(rs.getString("inputProbeNumber"));
					ld.setDisplayProbeNumber(rs.getString("displayProbeNumber"));
					ld.setTab_InputName(rs.getString("tab_InputName"));
					ld.setTab_DisplayName(rs.getString("tab_DisplayName"));
					
					disMap.put(ld.getInputProbeNumber(), ld);
					
					
				}
			
			DbServer.getSingleInstance().releaseConn(con);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return disMap;
	}
}
