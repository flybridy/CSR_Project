package com.fleety.analysis.track.task.key_area;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;

import server.db.DbServer;

import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;

public class KeyAreaInfoUtil {

	private static final int KEY_AREA_TYPE = 4;//重点区域类型
	public static HashMap getAreaInfo() 
	{
		HashMap areaMap = new HashMap();
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try 
		{
			StatementHandle stmt = conn.createStatement();
			ResultSet sets = stmt.executeQuery("select area_id,cname,longitude lo,"
					 + "latitude la from alarm_area where type=" + KEY_AREA_TYPE +" order by area_id,point_index");
			KeyAreaInfo keyAreaInfo = null;
			int areaId ;
			while(sets.next())
			{
				areaId = sets.getInt("area_id");
				keyAreaInfo = (KeyAreaInfo) areaMap.get(areaId);
				if(keyAreaInfo == null)
				{
					keyAreaInfo = new KeyAreaInfo();
					keyAreaInfo.setAreaId(areaId);
					keyAreaInfo.setCname(sets.getString("cname"));
					keyAreaInfo.setType(KEY_AREA_TYPE);
					areaMap.put(areaId, keyAreaInfo);
				}
				keyAreaInfo.los.add(sets.getDouble("lo"));
				keyAreaInfo.las.add(sets.getDouble("la"));
				keyAreaInfo.setPointsNum(keyAreaInfo.getPointsNum() + 1);
			}
			
			Iterator itr=areaMap.values().iterator();
			while(itr.hasNext()){
				keyAreaInfo=(KeyAreaInfo)itr.next();
				keyAreaInfo.initPolygon();
			}
		} 
		catch (SQLException e) 
		{
			e.printStackTrace();
		} 
		finally 
		{
			DbServer.getSingleInstance().releaseConn(conn);
		}
		
		return areaMap;
	}
}
