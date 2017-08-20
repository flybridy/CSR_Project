package com.fleety.analysis.track.task.overspeed_area;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;

import server.db.DbServer;

import com.fleety.server.area.AreaDataLoadServer;
import com.fleety.server.area.AreaInfo;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;

public class OverSpeedAreaInfoUtil {

	 public final static int OVERSPEEDAREATYPE = 102;//超速区域类型（限速区域） 
	
	 /**
	public static HashMap getAreaInfo() 
	{
		HashMap areaMap = new HashMap();
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try 
		{
			StatementHandle stmt = conn.createStatement();
			ResultSet sets = stmt.executeQuery("select area_id,cname,longitude lo,"
					 + "latitude la from alarm_area where type=" + OVERSPEEDAREATYPE +" order by area_id,point_index");
			OverSpeedAreaInfo keyAreaInfo = null;
			int areaId ;
			while(sets.next())
			{
				areaId = sets.getInt("area_id");
				keyAreaInfo = (OverSpeedAreaInfo) areaMap.get(areaId);
				if(keyAreaInfo == null)
				{
					keyAreaInfo = new OverSpeedAreaInfo();
					keyAreaInfo.setAreaId(areaId);
					keyAreaInfo.setCname(sets.getString("cname"));
					keyAreaInfo.setType(OVERSPEEDAREATYPE);
					areaMap.put(areaId, keyAreaInfo);
				}
				keyAreaInfo.los.add(sets.getDouble("lo"));
				keyAreaInfo.las.add(sets.getDouble("la"));
				keyAreaInfo.setPointsNum(keyAreaInfo.getPointsNum() + 1);
			}
			
			Iterator itr=areaMap.values().iterator();
			while(itr.hasNext()){
				keyAreaInfo=(OverSpeedAreaInfo)itr.next();
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
	**/
	public static HashMap getOverSpeedSchemeInfo() 
	{
		HashMap osschemeMap = new HashMap();
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try 
		{
			HashMap areaMap = new HashMap();
//			areaMap = getAreaInfo();
			areaMap = new AreaDataLoadServer().getAreaData(OVERSPEEDAREATYPE, false);
			if(areaMap==null || areaMap.size()==0){
				return null;
			}
			
			StatementHandle stmt = conn.createStatement();
			ResultSet sets = stmt.executeQuery("select a.id,a.area_id,a.area_name,a.start_time,a.end_time,a.overspeed,a.car_status,a.scheme_name"
					 + " from ANA_OVERSPEED_SCHEME a where a.status=1 order by a.id desc");
			AreaInfo keyAreaInfo = null;
			OverSpeedSchemeInfo osscheme = null;
			int areaId ;
			int osschemeId;
			while(sets.next())
			{
				osschemeId = sets.getInt("id");
				areaId = sets.getInt("area_id");
				keyAreaInfo = (AreaInfo) areaMap.get(areaId);
				
				if(keyAreaInfo == null)
				{
					continue;
				}
				osscheme = new OverSpeedSchemeInfo();
				osscheme.setScheme_id(sets.getInt("id"));
				osscheme.setStart_time(sets.getString("start_time"));
				osscheme.setEnd_time(sets.getString("end_time"));
				osscheme.setOverSpeed(sets.getDouble("overspeed"));
				osscheme.setCar_status(sets.getInt("car_status"));
				osscheme.setIsUsed(1);
				osscheme.setOverSpeedAreaInfo(keyAreaInfo);
				osscheme.setScheme_name(sets.getString("scheme_name"));
				osschemeMap.put(osschemeId, osscheme);
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
		
		return osschemeMap;
	}
}
