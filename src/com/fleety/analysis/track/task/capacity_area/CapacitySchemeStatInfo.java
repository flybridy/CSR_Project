package com.fleety.analysis.track.task.capacity_area;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import server.var.VarManageServer;

import com.fleety.analysis.track.DestInfo;
import com.fleety.analysis.track.task.key_area.DailyStatKeyAreaFreeLoadAction;
import com.fleety.base.GeneralConst;
import com.fleety.base.InfoContainer;
import com.fleety.base.StrFilter;
import com.fleety.track.TrackIO;

public class CapacitySchemeStatInfo {
	
	private final static int STATUS_FREE = 0; // 空车
	private final static int STATUS_LOAD = 1; // 重车
	
	private int schemeId;
	private String schemeName;
	private int preTime = 5;// 单位分钟

	private int orderNum;
	private int arriveNum;

	// private StringBuffer arriveDetail;
	private int totalInNum; //全部进入车次 
	private StringBuffer totalInDetail=new StringBuffer(256);
	
	private int loadNum;
	private int L2FNum;
	private StringBuffer L2FDetail=new StringBuffer(256);
	private int F2LNum;
	private StringBuffer F2LDetail=new StringBuffer(256);
	
	private int loadArrNum;  //送达车次
	private StringBuffer loadArrDetail=new StringBuffer(256);
	private int loadOffNum;  //运离车次
	private StringBuffer loadOffNumDetail=new StringBuffer(256);
	
	

	// 存放到达车辆的车牌号
	HashSet<String> arriveCarSet = new HashSet<String>();
	// 存放载客车辆的车牌号
	HashSet<String> loadCarSet = new HashSet<String>();

	ArrayList trackInfo = new ArrayList();// 所有在方案里面的轨迹点,每辆车分析后，立刻清空

	// HashMap periodMap = new HashMap();//key是自增的,每一小段超速对象

	public CapacitySchemeStatInfo() {
		super();
	}

	public void stat(DestInfo destInfo) {
		
		InfoContainer info = null;
		InfoContainer preInfo = null;

		InfoContainer firstInfo = null, lastInfo = null;

		int curStatus = 0, preStatus = 0;
		Date gpsTime = null, preGpsTime = null;

		boolean isFirst = true;
		// 对该时段的点进行分类，连续的两个位置汇报间隔超过2分钟认定为两次进入
		int count = 0;
		ArrayList tempList = new ArrayList();
		HashMap tempMap = new HashMap();
		int keyAreaIntevalSecond = 300;
		String temp = VarManageServer.getSingleInstance().getVarStringValue(
				"capacity_area_inteval_second");
		if (StrFilter.hasValue(temp)) {
			try {
				keyAreaIntevalSecond = Integer.parseInt(temp);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		int keyAreaMinSecond = 60;
		temp = VarManageServer.getSingleInstance().getVarStringValue(
				"capacity_area_min_second");
		if (StrFilter.hasValue(temp)) {
			try {
				keyAreaMinSecond = Integer.parseInt(temp);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		// 对区域内的点进行分段，5分钟以内的算是一段
		for (int i = 0; i < this.trackInfo.size(); i++) {
			info = (InfoContainer) this.trackInfo.get(i);
			if (i == 0) {
				preInfo = info;
				tempList = new ArrayList(); //一个tempList是一个车次的进入
				tempList.add(preInfo);
				tempMap.put(count, tempList);
				continue;
			}
			gpsTime = info.getDate(TrackIO.DEST_TIME_FLAG);
			preGpsTime = preInfo.getDate(TrackIO.DEST_TIME_FLAG);

			if (gpsTime.getTime() - preGpsTime.getTime() < keyAreaIntevalSecond * 1000) {
				tempList.add(info);
			} else {
				count++;
				tempList = new ArrayList();
				tempList.add(info);
				tempMap.put(count, tempList);
			}
			preInfo = info;
		}
		Iterator itr = tempMap.values().iterator();
		boolean isAddedArrCar = false;
		while (itr.hasNext()) {
			info = null;
			preInfo = null;
			tempList = (ArrayList) itr.next();
			if (tempList == null || tempList.size() == 0) {
				continue;
			}
			long totalTime = 0;
			boolean isStatusChange = false;
			for (int i = 0; i < tempList.size(); i++) {
				info = (InfoContainer) tempList.get(i);
				if (i == 0) {
					preInfo = info;
					firstInfo = info;
					continue;
				}
				if (i == tempList.size() - 1) {
					lastInfo = info;
				}
				gpsTime = info.getDate(TrackIO.DEST_TIME_FLAG);
				preGpsTime = preInfo.getDate(TrackIO.DEST_TIME_FLAG);
				curStatus = info.getInteger(TrackIO.DEST_STATUS_FLAG) & 0xf;
				preStatus = preInfo.getInteger(TrackIO.DEST_STATUS_FLAG) & 0xf;

				totalTime += gpsTime.getTime() - preGpsTime.getTime();
				if (curStatus != preStatus) {
					isStatusChange = true;   
				}
			}

			if ( totalTime > keyAreaMinSecond * 1000) {
				//把该车加人到达车辆中
				if(!isAddedArrCar){
					arriveCarSet.add(destInfo.destNo);
					isAddedArrCar=true;
				}
				
				// 所有进入区域车辆总数
				// 计算空车进入区域车辆总数
				// 计算重车进入区域车辆总数
				// 计算空车离开区域车辆总数
				// 计算重车离开区域车辆总数
				this.statFreeLoad(isStatusChange,tempList);
//				(destInfo, firstInfo,tempList);
			}
		}
	
		// 清除该车在该区域内的所有点
		this.clear();
	}
	
	/**
	 * 统计空重变化
	 * 
	 */
	private void statFreeLoad(boolean isChangeStatus,ArrayList trackList) {
		
		InfoContainer info = null;
		int firstStatus = 0;
		int curStatus = 0, lastStatus = 0,preStatus = 0;
		
		Date gpsTime = null;
		double lo = 0, la;
		boolean isFirst = true;
		String carNo = "";
		
		InfoContainer firstInfo = (InfoContainer) trackList.get(0);
		InfoContainer lastInfo = (InfoContainer) trackList.get(trackList.size()-1);
		Date firstGpsTime = null, lastGpsTime = null;
		double firstLo = 0, firstLa = 0, lastLo = 0, lastLa = 0;
		carNo = firstInfo.getString("DEST_NO");
		
		firstStatus = firstInfo.getInteger(TrackIO.DEST_STATUS_FLAG) & 0xf;
		firstGpsTime = firstInfo.getDate(TrackIO.DEST_TIME_FLAG);
		firstLo = firstInfo.getDouble(TrackIO.DEST_LO_FLAG);
		firstLa = firstInfo.getDouble(TrackIO.DEST_LA_FLAG);
		preStatus=firstStatus;

		lastStatus = lastInfo.getInteger(TrackIO.DEST_STATUS_FLAG) & 0xf;
		lastGpsTime = lastInfo.getDate(TrackIO.DEST_TIME_FLAG);
		lastLo = lastInfo.getDouble(TrackIO.DEST_LO_FLAG);
		lastLa = lastInfo.getDouble(TrackIO.DEST_LA_FLAG);
		if(firstStatus==STATUS_FREE && !isChangeStatus){
			//空进空出
		}else {
			this.loadCarSet.add(carNo);
		}

		if(firstStatus==STATUS_LOAD ){ //送达车次  && isChangeStatus 重进重车，算不算送达？
			this.loadArrNum++;
			this.loadArrDetail.append(carNo + ","
					+ GeneralConst.YYYYMMDDHHMMSS.format(firstGpsTime) + ","
					+ firstLo + "," + firstLa + ";");
		}
		
		if(lastStatus==STATUS_LOAD ){ //运离车次 && isChangeStatus重进重出，算不算一次送离？
			this.loadOffNum++;
			this.loadOffNumDetail.append(carNo + ","
					+ GeneralConst.YYYYMMDDHHMMSS.format(lastGpsTime) + ","
					+ firstLo + "," + firstLa + ";");
		}
		
		
		for (int i = 1; i < trackList.size(); i++) {
			
			info = (InfoContainer) trackList.get(i);
			curStatus = info.getInteger(TrackIO.DEST_STATUS_FLAG);
			gpsTime = info.getDate(TrackIO.DEST_TIME_FLAG);
			lo = info.getDouble(TrackIO.DEST_LO_FLAG);
			la = info.getDouble(TrackIO.DEST_LA_FLAG);
			
			
			if (curStatus != preStatus) {
				if (curStatus == STATUS_FREE) {
					
					this.L2FNum++;
					this.L2FDetail.append(carNo + ","
							+ GeneralConst.YYYYMMDDHHMMSS.format(gpsTime) + ","
							+ lo + "," + la + ";");

				} else if (curStatus == STATUS_LOAD) {
					this.F2LNum++;
					this.F2LDetail.append(carNo + ","
							+ GeneralConst.YYYYMMDDHHMMSS.format(gpsTime) + ","
							+ lo + "," + la + ";");
				}
				preStatus = curStatus;
			}
		}
		
		
		
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	private void clear() {
		this.trackInfo.clear();
	}

	public int getSchemeId() {
		return schemeId;
	}

	public void setSchemeId(int schemeId) {
		this.schemeId = schemeId;
	}

	public String getSchemeName() {
		return schemeName;
	}

	public void setSchemeName(String schemeName) {
		this.schemeName = schemeName;
	}

	public int getPreTime() {
		return preTime;
	}

	public void setPreTime(int preTime) {
		this.preTime = preTime;
	}

	public int getOrderNum() {
		return orderNum;
	}

	public void setOrderNum(int orderNum) {
		this.orderNum = orderNum;
	}

	public int getArriveNum() {
		return arriveNum;
	}

	public void setArriveNum(int arriveNum) {
		this.arriveNum = arriveNum;
	}

	public int getLoadNum() {
		return loadNum;
	}

	public void setLoadNum(int loadNum) {
		this.loadNum = loadNum;
	}

	public int getL2FNum() {
		return L2FNum;
	}

	public void setL2FNum(int l2fNum) {
		L2FNum = l2fNum;
	}

	public StringBuffer getL2FDetail() {
		return L2FDetail;
	}

	public void setL2FDetail(StringBuffer l2fDetail) {
		L2FDetail = l2fDetail;
	}

	public int getF2LNum() {
		return F2LNum;
	}

	public void setF2LNum(int f2lNum) {
		F2LNum = f2lNum;
	}

	public StringBuffer getF2LDetail() {
		return F2LDetail;
	}

	public void setF2LDetail(StringBuffer f2lDetail) {
		F2LDetail = f2lDetail;
	}

	public HashSet<String> getArriveCarSet() {
		return arriveCarSet;
	}

	public void setArriveCarSet(HashSet<String> arriveCarSet) {
		this.arriveCarSet = arriveCarSet;
	}

	public HashSet<String> getLoadCarSet() {
		return loadCarSet;
	}

	public void setLoadCarSet(HashSet<String> loadCarSet) {
		this.loadCarSet = loadCarSet;
	}

	public ArrayList getTrackInfo() {
		return trackInfo;
	}

	public void setTrackInfo(ArrayList trackInfo) {
		this.trackInfo = trackInfo;
	}

	public int getTotalInNum() {
		return totalInNum;
	}

	public void setTotalInNum(int totalInNum) {
		this.totalInNum = totalInNum;
	}

	public StringBuffer getTotalInDetail() {
		return totalInDetail;
	}

	public void setTotalInDetail(StringBuffer totalInDetail) {
		this.totalInDetail = totalInDetail;
	}

	public int getLoadArrNum() {
		return loadArrNum;
	}

	public void setLoadArrNum(int loadArrNum) {
		this.loadArrNum = loadArrNum;
	}

	public StringBuffer getLoadArrDetail() {
		return loadArrDetail;
	}

	public void setLoadArrDetail(StringBuffer loadArrDetail) {
		this.loadArrDetail = loadArrDetail;
	}

	public int getLoadOffNum() {
		return loadOffNum;
	}

	public void setLoadOffNum(int loadOffNum) {
		this.loadOffNum = loadOffNum;
	}

	public StringBuffer getLoadOffNumDetail() {
		return loadOffNumDetail;
	}

	public void setLoadOffNumDetail(StringBuffer loadOffNumDetail) {
		this.loadOffNumDetail = loadOffNumDetail;
	}

	
}
