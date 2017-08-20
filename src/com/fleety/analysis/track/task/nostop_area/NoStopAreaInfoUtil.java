package com.fleety.analysis.track.task.nostop_area;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;

import server.db.DbServer;

import com.fleety.server.area.AreaDataLoadServer;
import com.fleety.server.area.AreaInfo;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;

public class NoStopAreaInfoUtil {

	 public final static int NOSTOPAREATYPE = 104;//超速区域类型（限速区域） 
	
	
	public static HashMap getNoStopSchemeInfo() 
	{
		HashMap osschemeMap = new HashMap();
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try 
		{
			HashMap areaMap = new HashMap();
//			areaMap = getAreaInfo();
			areaMap = new AreaDataLoadServer().getAreaData(NOSTOPAREATYPE, false);
			if(areaMap==null || areaMap.size()==0){
				return null;
			}
			
			StatementHandle stmt = conn.createStatement();
			ResultSet sets = stmt.executeQuery("select id,scheme_name,area_id,status,minspeed,level1_time,level2_time,level3_time,start_time,end_time"
					 + " from ana_nostop_area_scheme where status=1 order by id desc");
			AreaInfo keyAreaInfo = null;
			NoStopSchemeInfo osscheme = null;
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
				osscheme = new NoStopSchemeInfo();
				osscheme.setScheme_id(sets.getInt("id"));
				osscheme.setScheme_name(sets.getString("scheme_name"));
				osscheme.setStatus(sets.getInt("status"));
				osscheme.setMinSpeed(sets.getInt("minspeed"));
				osscheme.setLevel1_time(sets.getInt("level1_time"));
				osscheme.setLevel2_time(sets.getInt("level2_time"));
				osscheme.setLevel3_time(sets.getInt("level3_time"));
				osscheme.setStart_time(sets.getString("start_time"));
				osscheme.setEnd_time(sets.getString("end_time"));
				
				osscheme.setOverSpeedAreaInfo(keyAreaInfo);
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
