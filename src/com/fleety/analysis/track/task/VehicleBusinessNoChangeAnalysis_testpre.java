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
import com.fleety.common.redis.BusinessNoBean;
import com.fleety.server.GlobalUtilServer;
import com.fleety.track.TrackIO;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;

/**
 * 车辆空变重未发生变化统计
 * @author admin
 *
 */
public class VehicleBusinessNoChangeAnalysis_testpre implements ITrackAnalysis {
	private HashMap vehicleMapping = null;
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");


	@Override
	public boolean startAnalysisTrack(AnalysisServer parentServer,
			InfoContainer statInfo) {
		this.vehicleMapping = null;
		
		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
		Date eTime = statInfo.getDate(STAT_END_TIME_DATE);

		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			StatementHandle stmt = conn.createStatement();
			StringBuilder sb = new StringBuilder();
			sb.append("select count(*) as sum from ana_vehicle_nobusiness_stat ")
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
		boolean hasbusiness = false;
		int preStatus=-1,status = 0;
		int gpsLocation = 0;
		if (!StrFilter.hasValue(plateNo)) {
			return;
		}
		
		if (trackInfo.trackArr != null && trackInfo.trackArr.length > 0) {
			
			Date trackStartTime = trackInfo.sDate;
			Date trackEndTime = trackInfo.eDate;
			
			for (int i = 0; i < trackInfo.trackArr.length; i++) {
				status = (trackInfo.trackArr[i].getInteger(
						TrackIO.DEST_STATUS_FLAG).intValue() & 0x0f);
				gpsLocation = trackInfo.trackArr[i]
						.getInteger(TrackIO.DEST_LOCATE_FLAG);

				// 不定位和黄车不参与
//				if (gpsLocation != 0 || status == 3) {
//				黄车不参与
				if (status!=0 && status!=1) {
					continue;
				}
				if (preStatus<0) {
					preStatus = status;
					System.out.println("分析时候，车牌号第一个status为："+plateNo+","+preStatus);
					continue;
				}
				
				if(preStatus!=status){
					hasbusiness=true;
				}
				
				if(!hasbusiness){ //此处逻辑有问题，当连续两个点位置相同的时候，就会进入此处
					DestInfo dest = trackInfo.dInfo ;
					BusinessNoBean bean=new BusinessNoBean();
					bean.setUid(plateNo);
					bean.setLastSystemDate(trackInfo.sDate);
					bean.setMdtid(dest.mdtId);
					bean.setComId(dest.companyId);
					bean.setCompanyName(dest.companyName);
					bean.setTrackNum(trackInfo.trackArr.length);
					String driver_info = this.getDriverInfoByPlate(plateNo);
					bean.setDriverInfo(driver_info);
					bean.setBusStatus(preStatus);
					trackEndTime = trackInfo.trackArr[trackInfo.trackArr.length-1].getDate(TrackIO.DEST_TIME_FLAG) ;
					bean.setTrackStartTime(trackStartTime);
					bean.setTrackEndTime(trackEndTime);
					List<BusinessNoBean> list = (List<BusinessNoBean>) vehicleMapping.get(dest.companyId);
					if(list ==null){
						list = new ArrayList<BusinessNoBean>();
						this.vehicleMapping.put(dest.companyId,list);
					}
					
					list.add(bean);
					break;
				}else{
					
				}
				preStatus = status;
			}
		}
		
	}

	/**
	 * 根据车牌号获取相关驾驶员信息
	 * @param plateNo
	 * @return
	 */
	private String getDriverInfoByPlate(String plateNo) {
		// TODO Auto-generated method stub
		String driver_info = "";
		DbHandle conn = DbServer.getSingleInstance().getConn();
		StringBuilder rsb = new StringBuilder();
		
		try{
			
			StatementHandle stmt = conn.createStatement();
			StringBuilder sb = new StringBuilder();
			sb.append("select a.DRIVER_NAME,a.SERVICE_NO from v_ana_driver_info a where a.CAR_ID='"+plateNo+"'");

			ResultSet sets = stmt.executeQuery(sb.toString());
			String driver1 = "";
			if (sets.next()) {
				driver1 = sets.getString("DRIVER_NAME")+","+sets.getString("SERVICE_NO")+";";
				rsb.append(driver1);
			}
			driver_info  = rsb.toString();
			if(driver_info.length()>0){
				driver_info = driver_info.substring(0,(driver_info.length()-1));
			}
		
		}catch(Exception e){
			e.printStackTrace();
			return driver_info;
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
		return driver_info;
	}

	@Override
	public void endAnalysisTrack(AnalysisServer parentServer,
			InfoContainer statInfo) {
		if (this.vehicleMapping == null) {
			return;
		}
		List insertList=new ArrayList();
		List destList = null;
		
		Iterator itr=this.vehicleMapping.values().iterator();
		BusinessNoBean bean=null;
		while(itr.hasNext()){
			int batchnum = 0;
			int id1= 0;
			DbHandle conn = null;
			try {
			destList = (List) itr.next();
			bean = (BusinessNoBean) destList.get(0);
			conn = DbServer.getSingleInstance().getConn();
			String sql1 = "insert into ana_vehicle_nobusiness_stat "
						+ " (id, taxi_company,"
						+ "  taxi_company_name, nobusiness_num, "
						+ "  stat_time, recode_time) " + " values "
						+ " (?, ?, ?, ?, ?,sysdate)";
			
			String sql2 = "insert into ana_vehicle_nobusiness "
					+ " (id, nobusiness_stat_id,car_no, mdt_id, type_id, taxi_company,"
					+ "  taxi_company_name, track_num, bus_status,"
					+ "  driver_info,stat_time, recode_time) " + " values "
					+ " (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,sysdate,track_start_time,track_end_time)";
				
				StatementHandle stmt = conn.prepareStatement(sql1);
				StatementHandle stmt2 = conn.prepareStatement(sql2);
				
				if(destList.size()>0){
					bean = (BusinessNoBean) destList.get(0);
					id1 = (int) DbServer.getSingleInstance().getAvaliableId(
							conn, "ana_vehicle_nobusiness_stat", "id");
					stmt.setInt(1, id1);
					stmt.setInt(2, bean.getComId());
					stmt.setString(3, bean.getCompanyName());
					stmt.setInt(4, destList.size());
					stmt.setDate(5, new java.sql.Date(bean.getLastSystemDate()
							.getTime()));
					stmt.addBatch();
					for(int i=0;i<destList.size();i++){
						bean = (BusinessNoBean) destList.get(i);
						int id2 = (int) DbServer.getSingleInstance().getAvaliableId(
								conn, "ana_vehicle_nobusiness", "id");
						stmt2.setInt(1, id2);
						stmt2.setInt(2, id1);
						stmt2.setString(3,bean.getUid() );   
						stmt2.setInt(4, bean.getMdtid());    
						stmt2.setInt(5, bean.getCarType());    
						stmt2.setInt(6, bean.getComId());    
						stmt2.setString(7, bean.getCompanyName());    
						stmt2.setInt(8, bean.getTrackNum());    
						stmt2.setInt(9, bean.getBusStatus());    
						stmt2.setString(10, bean.getDriverInfo());    
						stmt2.setDate(11, new java.sql.Date(bean.getLastSystemDate()
							.getTime()));  
						stmt2.setTimestamp(12,new java.sql.Timestamp(bean.getTrackStartTime().getTime()) );
						stmt2.setTimestamp(13,new java.sql.Timestamp(bean.getTrackEndTime().getTime()) );
						stmt2.addBatch();
						batchnum++;
						if(batchnum%200==0){
							stmt.executeBatch();
							stmt2.executeBatch();
						}
					}
					
					stmt.executeBatch();
					stmt2.executeBatch();
					
				}
				
			} catch (Exception ex) {
				ex.printStackTrace();
			} finally {
				DbServer.getSingleInstance().releaseConn(conn);
			}
			
		}
		System.out.println("Finish Analysis:" + this.toString()
				+ " 公司recordNum=" + (this.vehicleMapping.keySet().size()));
	}
	

	public String toString() {
		return "VehicleBusinessNoChangeAnalysis";
	}
}
