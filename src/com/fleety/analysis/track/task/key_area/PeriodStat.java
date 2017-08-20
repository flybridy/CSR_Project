package com.fleety.analysis.track.task.key_area;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;

import server.var.VarManageServer;

import com.fleety.analysis.track.DestInfo;
import com.fleety.base.GeneralConst;
import com.fleety.base.InfoContainer;
import com.fleety.base.StrFilter;
import com.fleety.track.TrackIO;

public class PeriodStat {
	private final static int STATUS_FREE = 0; // 空车
	private final static int STATUS_LOAD = 1; // 重车

	int preTime = 5;
	int period = 0;
	ArrayList periodTrackInfo = new ArrayList();

	int totalF2LNum = 0;// 空变重数
	int totalL2FNum = 0;// 重变空数
	int avgTotalCarNum = 0;// 总车数
	int avgFreeCarNum = 0;// 空车数
	int avgLoadCarNum = 0;// 重车数
	String detailF2L = "";// 空变重详情
	String detailL2F = "";// 重变空详情

	int totalInNum = 0;// 进入该区域内的车辆
	String detailTotalIn = "";

	int totalFreeInNum = 0;// 空车进入数
	String detailTotalFreeIn = "";
	int totalFreeInRedNum = 0;// 红的空车进入数
	int totalFreeInGreenNum = 0;// 绿的空车进入数

	int totalLoadInNum = 0;// 重车进入数
	String detailTotalLoadIn = "";
	int totalLoadInRedNum = 0;// 红的重车进入数
	int totalLoadInGreenNum = 0;// 绿的重车进入数

	int totalFreeOutNum = 0;// 空车离开数
	String detailTotalFreeOut = "";
	int totalFreeOutRedNum = 0;// 红的空车离开数
	int totalFreeOutGreenNum = 0;// 绿的空车离开数

	int totalLoadOutNum = 0;// 重车离开数
	String detailTotalLoadOut = "";
	int totalLoadOutRedNum = 0;// 红的重车离开数
	int totalLoadOutGreenNum = 0;// 绿的重车离开数

	HashMap totalMap = new HashMap();
	HashMap freeMap = new HashMap();
	HashMap loadMap = new HashMap();

	public PeriodStat() {
		// 每5分钟采样，统计总车数、空车数、重车数，取平均值
		for (int i = 0; i < 60; i += 5) {
			int temp = i;
			temp += 5;
			totalMap.put("total_" + i + "_" + temp, 0);
			freeMap.put("free_" + i + "_" + temp, 0);
			loadMap.put("load_" + i + "_" + temp, 0);
		}
	}

	public void stat(DestInfo destInfo) {
		this.statTotalIn(destInfo);
		statFreeLoad();
		statCarStatusNum();
		this.clear();
	}

	/**
	 * 统计所有进入过该区域的车辆
	 */
	private void statTotalIn(DestInfo destInfo) {
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
				"key_area_inteval_second");
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
				"key_area_min_second");
		if (StrFilter.hasValue(temp)) {
			try {
				keyAreaMinSecond = Integer.parseInt(temp);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		boolean isCheckStatusChange = !Boolean.parseBoolean(VarManageServer
				.getSingleInstance().getVarStringValue(
						"key_area_not_check_status_change"));

		// 对区域内的点进行分段，5分钟以内的算是一段
		for (int i = 0; i < this.periodTrackInfo.size(); i++) {
			info = (InfoContainer) this.periodTrackInfo.get(i);
			if (i == 0) {
				preInfo = info;
				tempList = new ArrayList();
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
			if (!isCheckStatusChange) {
				isStatusChange = true;
			}
			if (isStatusChange && (totalTime > keyAreaMinSecond * 1000)) {
				// 所有进入区域车辆总数
				// 计算空车进入区域车辆总数
				// 计算重车进入区域车辆总数
				// 计算空车离开区域车辆总数
				// 计算重车离开区域车辆总数
				this.countNum(destInfo, firstInfo, lastInfo);
			}
		}
	}

	/**
	 * 
	 * 在这里可以增加判断空车进入、重车进入、空车离开、重车离开数量判断 空车进入 重车进入 空车离开 重车离开 上客数=重车离开 下客数=重车进入
	 * 进站总数=空车进入+重车进入 离站总数=空车离开+重车离开
	 * 
	 * @param destInfo
	 * @param firstInfo
	 * @param lastInfo
	 */
	private void countNum(DestInfo destInfo, InfoContainer firstInfo,
			InfoContainer lastInfo) {
		int firstStatus = 0, lastStatus = 0;
		Date firstGpsTime = null, lastGpsTime = null;
		double firstLo = 0, firstLa = 0, lastLo = 0, lastLa = 0;

		firstStatus = firstInfo.getInteger(TrackIO.DEST_STATUS_FLAG) & 0xf;
		firstGpsTime = firstInfo.getDate(TrackIO.DEST_TIME_FLAG);
		firstLo = firstInfo.getDouble(TrackIO.DEST_LO_FLAG);
		firstLa = firstInfo.getDouble(TrackIO.DEST_LA_FLAG);

		lastStatus = lastInfo.getInteger(TrackIO.DEST_STATUS_FLAG) & 0xf;
		lastGpsTime = lastInfo.getDate(TrackIO.DEST_TIME_FLAG);
		lastLo = lastInfo.getDouble(TrackIO.DEST_LO_FLAG);
		lastLa = lastInfo.getDouble(TrackIO.DEST_LA_FLAG);

		this.totalInNum++;
		this.detailTotalIn += destInfo.destNo + ","
				+ GeneralConst.YYYYMMDDHHMMSS.format(firstGpsTime) + ","
				+ firstLo + "," + firstLa + "," + firstStatus + ","
				+ GeneralConst.YYYYMMDDHHMMSS.format(lastGpsTime) + ","
				+ lastLo + "," + lastLa + "," + lastStatus + ","
				+ destInfo.companyName + "," + destInfo.companyId + ";";

		// 计算空车进入
		if (firstStatus == STATUS_FREE) {
			this.totalFreeInNum++;
			this.detailTotalFreeIn += destInfo.destNo + ","
					+ GeneralConst.YYYYMMDDHHMMSS.format(firstGpsTime) + ","
					+ firstLo + "," + firstLa + "," + firstStatus + ","
					+ GeneralConst.YYYYMMDDHHMMSS.format(lastGpsTime) + ","
					+ lastLo + "," + lastLa + "," + lastStatus + ","
					+ destInfo.companyName + "," + destInfo.companyId + ";";

			if (destInfo.carType == DailyStatKeyAreaFreeLoadAction.TYPE_GREEN) {
				this.totalFreeInGreenNum++;
			} else {
				this.totalFreeInRedNum++;
			}
		}
		// 计算重车进入
		if (firstStatus == STATUS_LOAD) {
			this.totalLoadInNum++;
			this.detailTotalLoadIn += destInfo.destNo + ","
					+ GeneralConst.YYYYMMDDHHMMSS.format(firstGpsTime) + ","
					+ firstLo + "," + firstLa + "," + firstStatus + ","
					+ GeneralConst.YYYYMMDDHHMMSS.format(lastGpsTime) + ","
					+ lastLo + "," + lastLa + "," + lastStatus + ","
					+ destInfo.companyName + "," + destInfo.companyId + ";";

			if (destInfo.carType == DailyStatKeyAreaFreeLoadAction.TYPE_GREEN) {
				this.totalLoadInGreenNum++;
			} else {
				this.totalLoadInRedNum++;
			}
		}
		// 计算空车离开
		if (lastStatus == STATUS_FREE) {
			this.totalFreeOutNum++;
			this.detailTotalFreeOut += destInfo.destNo + ","
					+ GeneralConst.YYYYMMDDHHMMSS.format(firstGpsTime) + ","
					+ firstLo + "," + firstLa + "," + firstStatus + ","
					+ GeneralConst.YYYYMMDDHHMMSS.format(lastGpsTime) + ","
					+ lastLo + "," + lastLa + "," + lastStatus + ","
					+ destInfo.companyName + "," + destInfo.companyId + ";";

			if (destInfo.carType == DailyStatKeyAreaFreeLoadAction.TYPE_GREEN) {
				this.totalFreeOutGreenNum++;
			} else {
				this.totalFreeOutRedNum++;
			}
		}
		// 计算重车离开
		if (lastStatus == STATUS_LOAD) {
			this.totalLoadOutNum++;
			this.detailTotalLoadOut += destInfo.destNo + ","
					+ GeneralConst.YYYYMMDDHHMMSS.format(firstGpsTime) + ","
					+ firstLo + "," + firstLa + "," + firstStatus + ","
					+ GeneralConst.YYYYMMDDHHMMSS.format(lastGpsTime) + ","
					+ lastLo + "," + lastLa + "," + lastStatus + ","
					+ destInfo.companyName + "," + destInfo.companyId + ";";

			if (destInfo.carType == DailyStatKeyAreaFreeLoadAction.TYPE_GREEN) {
				this.totalLoadOutGreenNum++;
			} else {
				this.totalLoadOutRedNum++;
			}
		}

	}

	/**
	 * 统计空重变化
	 * 
	 */
	private void statFreeLoad() {
		InfoContainer info = null;
		int curStatus = 0, lastStatus = 0;
		Date gpsTime = null;
		double lo = 0, la;
		boolean isFirst = true;
		String carNo = "";

		for (int i = 0; i < this.periodTrackInfo.size(); i++) {
			info = (InfoContainer) this.periodTrackInfo.get(i);
			curStatus = info.getInteger(TrackIO.DEST_STATUS_FLAG);
			if (isFirst) {
				lastStatus = curStatus;
				isFirst = false;
				continue;
			}
			gpsTime = info.getDate(TrackIO.DEST_TIME_FLAG);
			lo = info.getDouble(TrackIO.DEST_LO_FLAG);
			la = info.getDouble(TrackIO.DEST_LA_FLAG);
			carNo = info.getString("DEST_NO");
			if (curStatus != lastStatus) {
				if (curStatus == STATUS_FREE) {

					this.totalL2FNum++;
					this.detailL2F += carNo + ","
							+ GeneralConst.YYYYMMDDHHMMSS.format(gpsTime) + ","
							+ lo + "," + la + ";";

				} else if (curStatus == STATUS_LOAD) {
					this.totalF2LNum++;
					this.detailF2L += carNo + ","
							+ GeneralConst.YYYYMMDDHHMMSS.format(gpsTime) + ","
							+ lo + "," + la + ";";
				}
				lastStatus = curStatus;
			}
		}
	}

	/**
	 * 统计总车数、空车数、重车数
	 * 
	 */
	private void statCarStatusNum() {
		InfoContainer info = null;
		Date gpsTime = null;
		int minute = 0, status = -1;
		boolean b = false;
		// 每5分钟采样一次
		for (int i = 0; i < 60;) {
			int temp = i;
			temp += 5;
			for (int j = 0; j < this.periodTrackInfo.size(); j++) {
				info = (InfoContainer) this.periodTrackInfo.get(j);
				gpsTime = info.getDate(TrackIO.DEST_TIME_FLAG);
				minute = gpsTime.getMinutes();
				if (minute >= (temp - this.preTime) && minute <= temp) {
					b = true;
					status = info.getInteger(TrackIO.DEST_STATUS_FLAG);
				}
			}
			if (b) {
				int num = 0;
				if (status == this.STATUS_FREE) {
					num = (Integer) this.freeMap.get("free_" + i + "_" + temp);
					this.freeMap.put("free_" + i + "_" + temp, ++num);

					num = (Integer) this.totalMap
							.get("total_" + i + "_" + temp);
					this.totalMap.put("total_" + i + "_" + temp, ++num);
				} else if (status == this.STATUS_LOAD) {
					num = (Integer) this.loadMap.get("load_" + i + "_" + temp);
					this.loadMap.put("load_" + i + "_" + temp, ++num);

					num = (Integer) this.totalMap
							.get("total_" + i + "_" + temp);
					this.totalMap.put("total_" + i + "_" + temp, ++num);
				}
			}
			b = false;
			i += 5;
		}
	}

	public void statAvg() {
		Iterator it = this.totalMap.keySet().iterator();
		int total = 0, temp = 0;
		while (it.hasNext()) {
			temp = (Integer) totalMap.get(it.next());
			total += temp;
		}
		this.avgTotalCarNum = total / totalMap.size();

		it = this.freeMap.keySet().iterator();
		int free = 0;
		while (it.hasNext()) {
			temp = (Integer) freeMap.get(it.next());
			free += temp;
		}
		this.avgFreeCarNum = free / freeMap.size();

		it = this.loadMap.keySet().iterator();
		int load = 0;
		while (it.hasNext()) {
			temp = (Integer) loadMap.get(it.next());
			load += temp;
		}
		this.avgLoadCarNum = load / loadMap.size();
		System.out.println("PeriodStat statAvg: total " + total + " free "
				+ free + " " + " load " + load + " avgTotalCarNum="
				+ this.avgTotalCarNum + "  avgFreeCarNum=" + this.avgFreeCarNum
				+ " avgLoadCarNum=" + this.avgLoadCarNum);
	}

	private void clear() {
		this.periodTrackInfo.clear();
	}
}
