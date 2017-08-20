package com.fleety.analysis.track.task;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import server.db.DbServer;
import server.var.VarManageServer;

import com.fleety.analysis.AnalysisServer;
import com.fleety.analysis.track.DestInfo;
import com.fleety.analysis.track.ITrackAnalysis;
import com.fleety.analysis.track.TrackInfo;
import com.fleety.base.InfoContainer;
import com.fleety.base.StrFilter;
import com.fleety.common.redis.BusinessFreeBusyBean;
import com.fleety.server.GlobalUtilServer;
import com.fleety.track.TrackIO;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;

/**
 * 车辆空变重次数统计，去掉不定位点和黄车点，统计空变重次数和重变空次数
 * @author admin
 *
 */
public class VehicleBusinessFreeBusyAnalysis implements ITrackAnalysis {
	private HashMap vehicleMapping = null;
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");


	@Override
	public boolean startAnalysisTrack(AnalysisServer parentServer,
			InfoContainer statInfo) {

		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
		Date eTime = statInfo.getDate(STAT_END_TIME_DATE);

		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			StatementHandle stmt = conn.createStatement();
			StringBuilder sb = new StringBuilder();
			sb.append("select count(*) as sum from ANA_VEHICLE_BUSINESS_STAT ")
					.append(" where STAT_TIME = to_date('")
					.append(sdf.format(sTime)).append("','yyyy-mm-dd')");
			ResultSet sets = stmt.executeQuery(sb.toString());
			if (sets.next()) {
				int sum = sets.getInt("sum");
				if (sum == 0)
					this.vehicleMapping = new HashMap();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}

		if (this.vehicleMapping == null) {
			System.out.println("Not Need Analysis:" + this.toString());
		} else {
			System.out.println("Start Analysis:" + this.toString());
		}
		return this.vehicleMapping != null;
	}

	@Override
	public void analysisDestTrack(AnalysisServer parentServer,
			TrackInfo trackInfo) {
		if (this.vehicleMapping == null) {
			return;
		}
		String plateNo = trackInfo.dInfo.destNo;
		int preStatus=0,status = 0;
		int gpsLocation = 0;
		if (!StrFilter.hasValue(plateNo)) {
			return;
		}
		BusinessFreeBusyBean bean=new BusinessFreeBusyBean();
		bean.setUid(plateNo);
		bean.setLastSystemDate(trackInfo.sDate);
		if (trackInfo.trackArr != null && trackInfo.trackArr.length > 0) {
			for (int i = 0; i < trackInfo.trackArr.length; i++) {
				status = (trackInfo.trackArr[i].getInteger(
						TrackIO.DEST_STATUS_FLAG).intValue() & 0x0f);
				gpsLocation = trackInfo.trackArr[i]
						.getInteger(TrackIO.DEST_LOCATE_FLAG);

				// 不定位和黄车不参与
				if (gpsLocation != 0 || status == 3) {
					continue;
				}
				if (i == 0) {
					preStatus = status;
					continue;
				}
				
				if(preStatus!=0&&status==0){
					bean.setBusy2Free(bean.getBusy2Free()+1);
				}else if(preStatus!=1&&status==1){
					bean.setFree2Busy(bean.getFree2Busy()+1);
				}			
				preStatus = status;
			}
		}
		this.vehicleMapping.put(plateNo, bean);
		
	}

	@Override
	public void endAnalysisTrack(AnalysisServer parentServer,
			InfoContainer statInfo) {
		if (this.vehicleMapping == null) {
			return;
		}
		List insertList=new ArrayList();
		
		Iterator itr=this.vehicleMapping.values().iterator();
		BusinessFreeBusyBean bean=null;
		while(itr.hasNext()){
			bean=(BusinessFreeBusyBean)itr.next();
			insertList.add(bean);
		}
		this.insertDb(insertList);
		
		System.out.println("Finish Analysis:" + this.toString()
				+ " recordNum=" + (insertList.size()));
	}
	
	private void insertDb(List insertList) {
		DbHandle conn = null;
		try {
			conn = DbServer.getSingleInstance().getConn();
			String sql = "insert into ana_vehicle_business_stat "
					+ " (id, car_no, mdt_id, type_id, taxi_company,"
					+ "  taxi_company_name, free_busy, busy_free,"
					+ "  stat_time, recode_time) " + " values "
					+ " (?, ?, ?, ?, ?, ?, ?, ?, ?, sysdate)";
			StatementHandle stmt = conn.prepareStatement(sql);

			DestInfo destInfo = null;
			BusinessFreeBusyBean bean = null;
			int id = 0;
			for (int i = 0; i < insertList.size(); i++) {
				bean = (BusinessFreeBusyBean) insertList.get(i);
				destInfo = GlobalUtilServer.getDestInfo(bean.getUid());
				if(destInfo==null){
					continue;
				}
				id = (int) DbServer.getSingleInstance().getAvaliableId(
						conn, "ana_vehicle_business_stat", "id");
				stmt.setInt(1, id);
				stmt.setString(2, destInfo.destNo);
				stmt.setInt(3, destInfo.mdtId);
				stmt.setInt(4, destInfo.carType);
				stmt.setInt(5, destInfo.companyId);
				stmt.setString(6, destInfo.companyName);
				stmt.setInt(7, bean.getFree2Busy());
				stmt.setInt(8, bean.getBusy2Free());
				stmt.setDate(9, new java.sql.Date(bean.getLastSystemDate()
						.getTime()));
				stmt.addBatch();
				if ((i + 1) % 200 == 0) {
					stmt.executeBatch();
				}
			}
			stmt.executeBatch();
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}
	}

	public String toString() {
		return "VehicleBusinessFreeBusyAnalysis";
	}
}
