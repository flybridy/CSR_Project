package com.fleety.analysis.track.task.capacity_area;

import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import server.db.DbServer;

import com.fleety.server.area.AreaDataLoadServer;
import com.fleety.server.area.AreaInfo;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;

public class CapacityAreaInfoUtil {

	public final static int CAPACITYAREA = 103;//运力区域类型
	public static HashMap getCapacitySchemeInfo() 
	{
		HashMap osschemeMap = new HashMap();
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try 
		{
			HashMap areaMap = new HashMap();
			areaMap = new AreaDataLoadServer().getAreaData(CAPACITYAREA, false);
			
			if(areaMap==null || areaMap.size()==0){
				return null;
			}
			
			StatementHandle stmt = conn.createStatement();
			ResultSet sets = stmt.executeQuery("select id,scheme_name,area_id,order_num,dest_nos,status,start_time,end_time" 
							+" from ANA_CAPACITY_AREA_SCHEME where status=1 ");
			AreaInfo keyAreaInfo = null;
			CapacitySchemeInfo osscheme = null;
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
				osscheme = new CapacitySchemeInfo();
				osscheme.setScheme_id(sets.getInt("id"));
				osscheme.setScheme_name(sets.getString("scheme_name"));
				osscheme.setOrder_num(sets.getInt("order_num"));
				//车牌号
				osscheme.setStatus(1);
				osscheme.setStart_time(sets.getString("start_time"));
				osscheme.setEnd_time(sets.getString("end_time"));
				osscheme.setAreaInfo(keyAreaInfo);
				
				String dest_nos = "";
				Clob clob = sets.getClob("dest_nos");
				if(clob != null){
					dest_nos = clob.getSubString((long)1,(int)clob.length());
				}
				HashSet<String> set = new HashSet<String>();
				for(String dest_no:dest_nos.split(";")){
					set.add(dest_no);
				}
				osscheme.setDestSet(set);
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
