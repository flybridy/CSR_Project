package com.fleety.analysis.track.task;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import com.fleety.analysis.AnalysisServer;
import com.fleety.analysis.track.ITrackAnalysis;
import com.fleety.analysis.track.TrackInfo;
import com.fleety.base.GeneralConst;
import com.fleety.base.InfoContainer;
import com.fleety.base.StrFilter;
import com.fleety.common.redis.BusinessLastBusyBean;
import com.fleety.track.TrackIO;
import com.fleety.util.pool.db.redis.RedisConnPoolServer;
import com.fleety.util.pool.db.redis.RedisTableBean;

/**
 * 分析每天最后一个空变重的时间点
 * @author zhengquan.jiang
 *
 */
public class VehicleBusinessLastBusyAnalysis  implements ITrackAnalysis {
	private HashMap vehicleMapping = null;
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	private Date sTime = null;

	public boolean startAnalysisTrack(AnalysisServer parentServer,InfoContainer statInfo) {

		sTime = statInfo.getDate(STAT_START_TIME_DATE);

		BusinessLastBusyBean bean = new BusinessLastBusyBean();

		try {
			List<BusinessLastBusyBean> list = RedisConnPoolServer.getSingleInstance().queryTableRecord(new RedisTableBean[] { bean });
			if(list == null || list.size()==0){
				this.vehicleMapping = new HashMap();
				return true;
			}
			this.vehicleMapping = new HashMap();
			for(int i=0;i<list.size();i++){
				bean = list.get(i);
				if(bean.getTrackDate().equals(sTime)){
					this.vehicleMapping  = null;
					break;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		if (this.vehicleMapping == null) {
			System.out.println("Not Need Analysis:" + this.toString());
		} else {
			System.out.println("Start Analysis:" + this.toString());
		}
		System.out.println("Start Analysis:" + this.toString());
		return this.vehicleMapping != null;
	}

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
		BusinessLastBusyBean bean=new BusinessLastBusyBean();
		bean.setUid(plateNo);
		bean.setCarNo(plateNo);
		bean.setTrackDate(sTime);
		if (trackInfo.trackArr != null && trackInfo.trackArr.length > 0) {
			for (int i = 0; i < trackInfo.trackArr.length; i++) {
				status = (trackInfo.trackArr[i].getInteger(TrackIO.DEST_STATUS_FLAG).intValue() & 0x0f);
				gpsLocation = trackInfo.trackArr[i].getInteger(TrackIO.DEST_LOCATE_FLAG);

				// 不定位和黄车不参与
				if (gpsLocation != 0 || status == 3) {
					continue;
				}
				if (i == 0) {
					preStatus = status;
					continue;
				}
				
				if(preStatus != 1 && status == 1){
					bean.setLastBusyTime(trackInfo.trackArr[i].getDate(TrackIO.DEST_TIME_FLAG));
				}			
				preStatus = status;
			}
			if(bean.getLastBusyTime() == null){
				bean.setLastBusyTime(trackInfo.trackArr[trackInfo.trackArr.length-1].getDate(TrackIO.DEST_TIME_FLAG));
			}
		}else{
			bean.setLastBusyTime(new Date());
		}
		
		this.vehicleMapping.put(plateNo, bean);
		
	}

	public void endAnalysisTrack(AnalysisServer parentServer,InfoContainer statInfo) {
		if (this.vehicleMapping == null) {
			return;
		}
		List insertList=new ArrayList();
		try {
			Iterator itr=this.vehicleMapping.values().iterator();
			BusinessLastBusyBean bean=null;
			while(itr.hasNext()){
				bean=(BusinessLastBusyBean)itr.next();
				insertList.add(bean);
			}
			
			RedisConnPoolServer.getSingleInstance().clearTableRecord(new BusinessLastBusyBean());
	
			RedisTableBean beans[] = new RedisTableBean[insertList.size()];
			insertList.toArray(beans);

			RedisConnPoolServer.getSingleInstance().saveTableRecord(beans);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public String toString() {
		return "VehicleBusinessLastBusyAnalysis";
	}
	
	public static void main(String args[]){
		RedisConnPoolServer.getSingleInstance().addPara("ip", "192.168.100.1");
		RedisConnPoolServer.getSingleInstance().addPara("port", "6379");
		RedisConnPoolServer.getSingleInstance().startServer();
		BusinessLastBusyBean bean = new BusinessLastBusyBean();

		List<BusinessLastBusyBean> list = null;
		try {
			list = RedisConnPoolServer.getSingleInstance().queryTableRecord(new RedisTableBean[] { bean });
		} catch (Exception e) {
			e.printStackTrace();
		}
		for(int i=0;i<list.size();i++){
			bean = list.get(i);
			
			System.out.println((i+1) + " , "+bean.getCarNo()+" , lastTime="+GeneralConst.YYYY_MM_DD_HH_MM_SS.format(bean.getLastBusyTime()) + " , trackDate="+GeneralConst.YYYY_MM_DD.format(bean.getTrackDate()));

		}
	}
}
