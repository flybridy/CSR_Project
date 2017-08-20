package com.fleety.analysis.track.task.capacity_area;

import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;

import oracle.sql.BLOB;
import oracle.sql.CLOB;

import com.fleety.analysis.AnalysisServer;
import com.fleety.analysis.track.ITrackAnalysis;
import com.fleety.analysis.track.TrackInfo;
import com.fleety.analysis.track.YesterdayTrackAnalysisServer;
import com.fleety.base.GeneralConst;
import com.fleety.base.InfoContainer;
import com.fleety.base.StrFilter;
import com.fleety.track.TrackIO;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;

import server.db.DbServer;
import server.var.VarManageServer;

public class DailyCapacityAreaAction implements ITrackAnalysis {

	private int preTime = 5;// 单位分钟
	private HashMap resultMap = null;   //
	private HashMap schemeMap = null;    //存放需要分析的方案，id做key,OverSpeedSchemeInfo做value

	private final static int STATUS_FREE = 0; // 空车
	private final static int STATUS_LOAD = 1; // 重车
	
	public final static int TYPE_RED=1;
	public static int TYPE_GREEN=2;
	public final static int TYPE_BLUE=3;

	public void init() {
		this.preTime = YesterdayTrackAnalysisServer.getSingleInstance().preTime;
		schemeMap = CapacityAreaInfoUtil.getCapacitySchemeInfo();
	}

	public boolean startAnalysisTrack(AnalysisServer parentServer,
			InfoContainer statInfo) {
		this.init();
		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
		Date eTime = statInfo.getDate(STAT_END_TIME_DATE);
		this.resultMap = null;
		if(schemeMap==null || schemeMap.size()==0){
			//没有需要分析的方案
		}else{
			
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			StatementHandle pstmt = conn
					.prepareStatement("select * from ana_capacity_area_monitor where stat_time between ? and ?");
			pstmt.setDate(1, new java.sql.Date(sTime.getTime() + 1000));
			pstmt.setDate(2, new java.sql.Date(eTime.getTime()));
			ResultSet sets = pstmt.executeQuery();
			if (!sets.next()) {
				resultMap = new HashMap();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}
		}
		if (this.resultMap == null) {
			System.out.println("Not Need Analysis:" + this.toString());
		} else {
			System.out.println("Start Analysis:" + this.toString());
		}
		return resultMap != null;
	}

	public void analysisDestTrack(AnalysisServer parentServer,
			TrackInfo trackInfo) {
		if (this.resultMap == null) {
			return;
		}
		if (trackInfo == null) {
			return;
		}
		String carNo = trackInfo.dInfo.destNo;
		Iterator itt = schemeMap.keySet().iterator();
		CapacitySchemeInfo scheme = null;
		Integer schemeId = null;
		HashMap useSchemeMap = new HashMap();
		while (itt.hasNext()) {
			schemeId = (Integer) itt.next();
			scheme = (CapacitySchemeInfo) schemeMap.get(schemeId);
			if (scheme.getDestSet().contains(carNo)) {  //包含需要分析的车牌号，才分析该车
				useSchemeMap.put(schemeId, scheme);
				}
			}
		if(useSchemeMap.size()==0){
			return ;
		}
	
		
		InfoContainer info = null;
		int lastStatus = -1, curStatus = -1;
		double lo, la;
		int gpsLocation = 0;
		Iterator it;
		CapacitySchemeStatInfo schemeStatInfo;
		
		for (int i = 0; i < trackInfo.trackArr.length; i++) {
			info = trackInfo.trackArr[i];
			lo = info.getDouble(TrackIO.DEST_LO_FLAG);
			la = info.getDouble(TrackIO.DEST_LA_FLAG);

			info.setInfo("DEST_NO", carNo);

			gpsLocation = info.getInteger(TrackIO.DEST_LOCATE_FLAG);
			curStatus = info.getInteger(TrackIO.DEST_STATUS_FLAG) & 0xf;
			if (curStatus != STATUS_FREE && curStatus != STATUS_LOAD) {
				continue;
			}
			if (gpsLocation != 0) {
				continue;
			}

			it = useSchemeMap.keySet().iterator();
			CapacitySchemeInfo scheme_stat = null;
			Integer schemeId_stat = null;
			
			while (it.hasNext()) {
				
				schemeId_stat = (Integer) it.next();
				scheme_stat = (CapacitySchemeInfo) useSchemeMap.get(schemeId_stat);
				if (scheme_stat.isTrackInScheme(info)) {
					schemeStatInfo = (CapacitySchemeStatInfo) resultMap.get(schemeId_stat);
					if (schemeStatInfo == null) {
						schemeStatInfo = new CapacitySchemeStatInfo();
						schemeStatInfo.setSchemeId(scheme_stat.getScheme_id());
						schemeStatInfo.setSchemeName(scheme_stat.getScheme_name()); 
						schemeStatInfo.setOrderNum(scheme_stat.getOrder_num());
						schemeStatInfo.setPreTime(this.preTime);
						resultMap.put(schemeId_stat,schemeStatInfo);
					}
					schemeStatInfo.trackInfo.add(info);
				}
			}
		}

		it = resultMap.keySet().iterator();
		while (it.hasNext()) {
			schemeStatInfo = (CapacitySchemeStatInfo) resultMap.get(it.next());
			schemeStatInfo.stat(trackInfo.dInfo);
		}

	}

	public void endAnalysisTrack(AnalysisServer parentServer,
			InfoContainer statInfo) {
		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
		this.logStatToDB(resultMap,sTime);
	}

	private void logStatToDB(HashMap resultMap,Date sTime) {
		if (resultMap == null) {
			return;
		}
		System.out
				.println("DailyCapacityAreaAction log db resultMap size:"
						+ resultMap.size());
		Iterator it1 = resultMap.keySet().iterator();
		CapacitySchemeStatInfo schemeStatInfo = null;
		while (it1.hasNext()) {
			schemeStatInfo = (CapacitySchemeStatInfo) resultMap.get(it1.next());
			
		}
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DAY_OF_MONTH, -1);

		DbHandle conn = DbServer.getSingleInstance().getConn();
		
		String sql = "insert into ana_capacity_area_monitor(id,scheme_id,scheme_name,stat_time,order_num,arrive_num,arrive_detail,"
				+ "load_num,load_detail,loadarrnum,loadarrdetail,loadoffnum,loadoffdetail)"
				+ " values(?,?,?,?,?,?,empty_clob(),?,empty_clob(),?,empty_clob(),?,empty_clob()) ";

		try {
			StatementHandle pstmt = conn.prepareStatement(sql);
			Iterator it = resultMap.keySet().iterator();
			CapacitySchemeStatInfo areaStatCarInfo = null;
			HashMap map = new HashMap();
			while (it.hasNext()) {
				areaStatCarInfo = (CapacitySchemeStatInfo) resultMap.get(it.next());
				long id = DbServer.getSingleInstance().getAvaliableId(conn,
						"ana_capacity_area_monitor", "id");
				pstmt.setLong(1, id);
				pstmt.setInt(2, areaStatCarInfo.getSchemeId());
				pstmt.setString(3, areaStatCarInfo.getSchemeName());
				pstmt.setDate(
						4,
						new java.sql.Date(sTime.getTime()));
				pstmt.setInt(5, areaStatCarInfo.getOrderNum());
				pstmt.setInt(6, areaStatCarInfo.getArriveCarSet().size());
				pstmt.setInt(7, areaStatCarInfo.getLoadCarSet().size());
				pstmt.setInt(8, areaStatCarInfo.getLoadArrNum());
				pstmt.setInt(9, areaStatCarInfo.getLoadOffNum());
				pstmt.addBatch();
				map.put(id, areaStatCarInfo);
			
			}
			pstmt.executeBatch();
			try {
				conn.setAutoCommit(false);
				pstmt = conn
						.prepareStatement("select arrive_detail,load_detail,loadarrdetail,loadoffdetail from ana_capacity_area_monitor where id=? for update ");
				
				StatementHandle prepapstmt2 = conn
						.prepareStatement("update ana_capacity_area_monitor set arrive_detail=?,load_detail=?,loadarrdetail=?,loadoffdetail=? where id=?");
				
				
				ResultSet rs;
				it = map.keySet().iterator();
				ResultSet sets;
				CLOB clob1,clob2,clob3,clob4;
				
				long id = 0;
				Iterator it2;
				Iterator it3;
				
				while (it.hasNext()) {
					id = (Long) it.next();
					areaStatCarInfo=(CapacitySchemeStatInfo) map.get(id);
					StringBuffer arrdetial = new StringBuffer(256);
					StringBuffer loaddetail = new StringBuffer(256);
					it2=areaStatCarInfo.getArriveCarSet().iterator();
					it3=areaStatCarInfo.getLoadCarSet().iterator();
					
					while(it2.hasNext()){
						arrdetial.append(it2.next()+";");
					}
					while(it3.hasNext()){
						loaddetail.append(it3.next()+";");
					}
					pstmt.setLong(1, id);
					sets = pstmt.executeQuery();
					if(sets.next()){
					     clob1 = (CLOB) sets.getClob("arrive_detail") ;
			             clob2 = (CLOB)sets.getClob("load_detail") ;  
			             clob3 = (CLOB)sets.getClob("loadarrdetail") ;  
			             clob4 = (CLOB)sets.getClob("loadoffdetail") ;  
			             clob1.putString(1,arrdetial.toString() );
			             clob2.putString(1,loaddetail.toString());
			             clob3.putString(1,areaStatCarInfo.getLoadArrDetail().toString());
			             clob4.putString(1,areaStatCarInfo.getLoadOffNumDetail().toString());
			             
			             prepapstmt2.setLong(5, id); 
				         prepapstmt2.setObject(1, clob1);  	
				         prepapstmt2.setObject(2, clob2);
				         prepapstmt2.setObject(3, clob3);
				         prepapstmt2.setObject(4, clob4);
				         prepapstmt2.executeUpdate() ;  
					}
				}
				
				conn.commit();
			} catch (Exception e) {
				try {
					conn.rollback();
				} catch (Exception ex) {
					ex.printStackTrace();
				}
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println("DailyCapacityAreaAction end log db!");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}
	}
}
