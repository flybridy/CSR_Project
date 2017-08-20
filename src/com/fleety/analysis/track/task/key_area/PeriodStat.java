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
	private final static int STATUS_FREE = 0; // �ճ�
	private final static int STATUS_LOAD = 1; // �س�

	int preTime = 5;
	int period = 0;
	ArrayList periodTrackInfo = new ArrayList();

	int totalF2LNum = 0;// �ձ�����
	int totalL2FNum = 0;// �ر����
	int avgTotalCarNum = 0;// �ܳ���
	int avgFreeCarNum = 0;// �ճ���
	int avgLoadCarNum = 0;// �س���
	String detailF2L = "";// �ձ�������
	String detailL2F = "";// �ر������

	int totalInNum = 0;// ����������ڵĳ���
	String detailTotalIn = "";

	int totalFreeInNum = 0;// �ճ�������
	String detailTotalFreeIn = "";
	int totalFreeInRedNum = 0;// ��Ŀճ�������
	int totalFreeInGreenNum = 0;// �̵Ŀճ�������

	int totalLoadInNum = 0;// �س�������
	String detailTotalLoadIn = "";
	int totalLoadInRedNum = 0;// ����س�������
	int totalLoadInGreenNum = 0;// �̵��س�������

	int totalFreeOutNum = 0;// �ճ��뿪��
	String detailTotalFreeOut = "";
	int totalFreeOutRedNum = 0;// ��Ŀճ��뿪��
	int totalFreeOutGreenNum = 0;// �̵Ŀճ��뿪��

	int totalLoadOutNum = 0;// �س��뿪��
	String detailTotalLoadOut = "";
	int totalLoadOutRedNum = 0;// ����س��뿪��
	int totalLoadOutGreenNum = 0;// �̵��س��뿪��

	HashMap totalMap = new HashMap();
	HashMap freeMap = new HashMap();
	HashMap loadMap = new HashMap();

	public PeriodStat() {
		// ÿ5���Ӳ�����ͳ���ܳ������ճ������س�����ȡƽ��ֵ
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
	 * ͳ�����н����������ĳ���
	 */
	private void statTotalIn(DestInfo destInfo) {
		InfoContainer info = null;
		InfoContainer preInfo = null;

		InfoContainer firstInfo = null, lastInfo = null;

		int curStatus = 0, preStatus = 0;
		Date gpsTime = null, preGpsTime = null;

		boolean isFirst = true;
		// �Ը�ʱ�εĵ���з��࣬����������λ�û㱨�������2�����϶�Ϊ���ν���
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

		// �������ڵĵ���зֶΣ�5�������ڵ�����һ��
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
				// ���н�������������
				// ����ճ���������������
				// �����س���������������
				// ����ճ��뿪����������
				// �����س��뿪����������
				this.countNum(destInfo, firstInfo, lastInfo);
			}
		}
	}

	/**
	 * 
	 * ��������������жϿճ����롢�س����롢�ճ��뿪���س��뿪�����ж� �ճ����� �س����� �ճ��뿪 �س��뿪 �Ͽ���=�س��뿪 �¿���=�س�����
	 * ��վ����=�ճ�����+�س����� ��վ����=�ճ��뿪+�س��뿪
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

		// ����ճ�����
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
		// �����س�����
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
		// ����ճ��뿪
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
		// �����س��뿪
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
	 * ͳ�ƿ��ر仯
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
	 * ͳ���ܳ������ճ������س���
	 * 
	 */
	private void statCarStatusNum() {
		InfoContainer info = null;
		Date gpsTime = null;
		int minute = 0, status = -1;
		boolean b = false;
		// ÿ5���Ӳ���һ��
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
