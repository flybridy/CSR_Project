package com.fleety.analysis.track.task.key_area;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import com.fleety.analysis.AnalysisServer;
import com.fleety.analysis.track.ITrackAnalysis;
import com.fleety.analysis.track.TrackInfo;
import com.fleety.analysis.track.YesterdayTrackAnalysisServer;
import com.fleety.base.InfoContainer;
import com.fleety.base.StrFilter;
import com.fleety.track.TrackIO;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;

import server.db.DbServer;
import server.var.VarManageServer;

public class StatKeyAreaInOutAction implements ITrackAnalysis {

	private HashMap resultMap = null;
	private HashMap areaMap = null;
	private AllAreaResultInfoContainer allResult = null;
	private int preTime = 20;// ��λ����
	private final static int STATUS_FREE = 0; // �ճ�
	private final static int STATUS_LOAD = 1; // �س�

	private static final int FREEIN = 0;
	private static final int LOADIN = 1;
	private static final int FREEOUT = 2;
	private static final int LOADOUT = 3;

	public void init() {
		this.preTime = YesterdayTrackAnalysisServer.getSingleInstance().preTime;
		areaMap = KeyAreaInfoUtil.getAreaInfo();
		allResult = new AllAreaResultInfoContainer();
	}

	public boolean startAnalysisTrack(AnalysisServer parentServer,
			InfoContainer statInfo) {

		this.init();
		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
		Date eTime = statInfo.getDate(STAT_END_TIME_DATE);
		this.resultMap = null;
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			StatementHandle pstmt = conn
					.prepareStatement("select * from ana_key_area_car_inout_stat where stat_time between ? and ?");
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
		if (this.resultMap == null) {
			System.out.println("Not Need Analysis:" + this.toString());
		} else {
			System.out.println("Start Analysis:" + this.toString());
		}
		return resultMap != null;
	}

	public void analysisDestTrack(AnalysisServer parentServer,
			TrackInfo trackInfo) {
		AllAreaStatInfoContainer tempContainer = new AllAreaStatInfoContainer();
		if (this.resultMap == null) {
			return;
		}
		if (trackInfo == null) {
			return;
		}

		InfoContainer info = null;
		int curStatus = -1;
		double lo, la;
		int gpsLocation = 0;
		Date gpsTime, lastGpsTime;
		Iterator itr = null;
		AreaStatCarInfo statInfo;
		String destNo = trackInfo.dInfo.destNo;
		for (int i = 0; i < trackInfo.trackArr.length; i++) {
			info = trackInfo.trackArr[i];
			lo = info.getDouble(TrackIO.DEST_LO_FLAG);
			la = info.getDouble(TrackIO.DEST_LA_FLAG);
			gpsLocation = info.getInteger(TrackIO.DEST_LOCATE_FLAG);
			gpsTime = info.getDate(TrackIO.DEST_TIME_FLAG);
			curStatus = info.getInteger(TrackIO.DEST_STATUS_FLAG)&0xf;
			if (curStatus != STATUS_FREE && curStatus != STATUS_LOAD) {
				continue;
			}
			if (gpsLocation != 0) {
				continue;
			}

			itr = areaMap.keySet().iterator();
			Integer areaId = null;
			KeyAreaInfo keyAreaInfo = null;
			InfoContainer lastPointInfo = null;
			boolean isInArea = false, lastIsInArea = false;
			while (itr.hasNext()) {
				areaId = (Integer) itr.next();
				keyAreaInfo = (KeyAreaInfo) areaMap.get(areaId);

				// ��һ�����飬�жϵ������Ĺ�ϵ
				isInArea = keyAreaInfo.isInArea(lo, la);
				info.setInfo("is_in", isInArea);
				lastPointInfo = (InfoContainer) keyAreaInfo.statMap.get(destNo);
				// �ڶ������飬����ǵ�һ����ֻ��Ҫ����˵�
				if (lastPointInfo == null) {
					lastPointInfo = info;
					keyAreaInfo.statMap.put(destNo, lastPointInfo);
					continue;
				}
				// ���������飬���ǰһ����͵�ǰ���ʱ�����涨ʱ�䣬���õ�ǰ����Ϊ���һ�ε�
				lastGpsTime = lastPointInfo.getDate(TrackIO.DEST_TIME_FLAG);
				if (gpsTime.getTime() - lastGpsTime.getTime() > this.preTime * 60 * 1000) {
					keyAreaInfo.statMap.put(destNo, info);
					continue;
				}

				lastIsInArea = lastPointInfo.getBoolean("is_in");
				if (lastIsInArea != isInArea) {
					CarEvnetInfo event = new CarEvnetInfo();
					event.info = info;
					event.time = gpsTime.getTime();
					// ����ǵ�ǰ�������ڣ��ǽ���������Ҫ������ʱ�ĳ���״̬��ʱ��㣬������־
					if (isInArea) {
						// �����ǰ���ǿճ����ճ���������
						// �����ǰ�����س����س���������
						if (curStatus == STATUS_FREE) {
							event.flag = FREEIN;
						} else if (curStatus == STATUS_LOAD) {
							event.flag = LOADIN;
						}
					} else {
						// �����ǰ���ǿճ����ճ��뿪����
						// �����ǰ�����س����س��뿪����
						if (curStatus == STATUS_FREE) {
							event.flag = FREEOUT;
						} else if (curStatus == STATUS_LOAD) {
							event.flag = LOADOUT;
						}
					}
					tempContainer.addCarEventInfo(areaId, event);
				}
			}
		}

		// ����ǰͳ�ƽ���ۼӵ��ܽ����ȥ
		HourResultInfo hourResultInfo = null;
		itr = areaMap.keySet().iterator();
		Integer areaId = null;
		while (itr.hasNext()) {
			areaId = (Integer) itr.next();
			if(tempContainer.getAreaCarEventCount(areaId)==0){
				continue;
			}
			for (int i = 0; i < 24; i++) {
				hourResultInfo = tempContainer.getHourResultInfo(areaId, i);
				if (hourResultInfo != null) {
					allResult.addResultInfo(areaId, i, hourResultInfo);
				}
			}
		}

	}

	public void endAnalysisTrack(AnalysisServer parentServer,
			InfoContainer statInfo) {
		// ���ܽ����¼�����ݿ���
		if (this.allResult == null) {
			return;
		}
		DbHandle conn = null;
		try {
			conn = DbServer.getSingleInstance().getConn();
			conn.setAutoCommit(false);
			String sql = "insert into ana_key_area_car_inout_stat(id, area_id,area_name, hour,"
					+ " free_in, free_out, load_in, load_out,"
					+ " stat_time, create_time, car_type) "
					+ " values (?, ?,?, ?, ?, ?, ?, ?, ?, sysdate, 1)";
			StatementHandle psmt = conn.prepareCall(sql);

			Iterator itr = this.areaMap.keySet().iterator();
			Integer areaId = null;
			int id = 0;
			Date sDate = statInfo.getDate(STAT_START_TIME_DATE);
			int count = 1;
			KeyAreaInfo areaInfo=null;
			HourResultInfo totalResultInfo=null;
			while (itr.hasNext()) {
				areaId = (Integer) itr.next();
				areaInfo = (KeyAreaInfo) this.areaMap.get(areaId);

				if (areaInfo != null) {
					totalResultInfo = new HourResultInfo();
					for (int i = 0; i < 24; i++) {
						HourResultInfo resultInfo = this.allResult
								.getResultInfo(areaId, i);
						if (resultInfo != null) {
							id = (int) DbServer
									.getSingleInstance()
									.getAvaliableId(conn,
											"ana_key_area_car_inout_stat", "id");
							psmt.setInt(1, id);
							psmt.setInt(2, areaId);
							psmt.setString(3, areaInfo.getCname());
							psmt.setInt(4, i);
							psmt.setInt(5, resultInfo.freeInNum);
							psmt.setInt(6, resultInfo.freeOutNum);
							psmt.setInt(7, resultInfo.loadInNum);
							psmt.setInt(8, resultInfo.loadOutNum);
							psmt.setDate(9, new java.sql.Date(sDate.getTime()));
							totalResultInfo.freeInNum += resultInfo.freeInNum;
							totalResultInfo.freeOutNum += resultInfo.freeOutNum;
							totalResultInfo.loadInNum += resultInfo.loadInNum;
							totalResultInfo.loadOutNum += resultInfo.loadOutNum;
							psmt.addBatch();
							count++;
						}
					}
					id = (int) DbServer.getSingleInstance().getAvaliableId(
							conn, "ana_key_area_car_inout_stat", "id");
					psmt.setInt(1, id);
					psmt.setInt(2, areaId);
					psmt.setString(3, areaInfo.getCname());
					psmt.setInt(4, 24);
					psmt.setInt(5, totalResultInfo.freeInNum);
					psmt.setInt(6, totalResultInfo.freeOutNum);
					psmt.setInt(7, totalResultInfo.loadInNum);
					psmt.setInt(8, totalResultInfo.loadOutNum);
					psmt.setDate(9, new java.sql.Date(sDate.getTime()));
					psmt.addBatch();

					if (count % 200 == 0) {
						psmt.executeBatch();
					}
				}
			}
			psmt.executeBatch();
			conn.commit();

		} catch (Exception ex) {
			ex.printStackTrace();
			try {
				conn.rollback();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}

	}

	/************************************** �������Զ�������ݽṹ *********************************/

	/**
	 * ��������Ĺ���ĳ̨�������н����¼�����
	 * 
	 * @author admin
	 * 
	 */
	public class AllAreaStatInfoContainer {
		private HashMap<Integer, AreaCarEnventContainer> map = null;

		public AllAreaStatInfoContainer() {
			this.map = new HashMap<Integer, AreaCarEnventContainer>();
		}
		public int getAreaCarEventCount(int areaId){
			AreaCarEnventContainer carAreaStatInfo = this.map.get(areaId);
			if(carAreaStatInfo==null){
				return 0;
			}else{
				return carAreaStatInfo.getCarEventCount();
			}
		}

		/**
		 * �����������������¼�
		 * 
		 * @param areaId
		 * @param event
		 */
		public void addCarEventInfo(int areaId, CarEvnetInfo event) {
			AreaCarEnventContainer carAreaStatInfo = this.map.get(areaId);
			if (carAreaStatInfo == null) {
				carAreaStatInfo = new AreaCarEnventContainer(areaId);				
			}
			carAreaStatInfo.addCarEventInfo(event);
			this.map.put(areaId, carAreaStatInfo);
		}

		/**
		 * �õ�������ĳ������ĳ��ʱ�ε�ͳ����Ϣ
		 * 
		 * @param areaId
		 * @param hour
		 * @return
		 */
		public HourResultInfo getHourResultInfo(int areaId, int hour) {
			AreaCarEnventContainer carAreaStatInfo = this.map.get(areaId);
			if (carAreaStatInfo != null) {
				return carAreaStatInfo.getHourResultInfoByHour(hour);
			}
			return null;
		}

	}

	/**
	 * ĳ�������ĳ̨�������н����¼�����
	 * 
	 * @author admin
	 * 
	 */
	public class AreaCarEnventContainer {
		private int areaId = 0;
		private List<CarEvnetInfo> carEventList = null;

		public AreaCarEnventContainer(int areaId) {
			this.areaId = areaId;
			carEventList = new ArrayList<CarEvnetInfo>(60);
		}

		public int getAreaId() {
			return this.areaId;
		}

		public void addCarEventInfo(CarEvnetInfo event) {
			this.carEventList.add(event);
		}
		public int getCarEventCount(){
			return this.carEventList.size();
		}

		/**
		 * �õ�������ó�һ��Сʱ��ͳ����Ϣ
		 * 
		 * @param hour
		 * @return
		 */
		public HourResultInfo getHourResultInfoByHour(int hour) {
			CarEvnetInfo event = null;
			Calendar cal = Calendar.getInstance();

			HourResultInfo resultInfo = new HourResultInfo();
			for (int i = 0; i < this.carEventList.size(); i++) {
				event = this.carEventList.get(i);
				cal.setTimeInMillis(event.time);
				if (cal.get(Calendar.HOUR_OF_DAY) == hour) {
					switch (event.flag) {
					case FREEIN:
						resultInfo.freeInNum++;
						break;
					case LOADIN:
						resultInfo.loadInNum++;
						break;
					case FREEOUT:
						resultInfo.freeOutNum++;
						break;
					case LOADOUT:
						resultInfo.loadOutNum++;
						break;
					default:
						break;
					}
				}
			}
			int maxTimes = 100;
			String temp=VarManageServer.getSingleInstance().getVarStringValue("key_area_maxtimes");
			if(StrFilter.hasValue(temp)){
				try {
					maxTimes=Integer.parseInt(temp);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (resultInfo.freeInNum > maxTimes
					|| resultInfo.freeOutNum > maxTimes
					|| resultInfo.loadInNum > maxTimes
					|| resultInfo.loadOutNum > maxTimes) {
				return null;
			}
			return resultInfo;
		}

	}

	/**
	 * ����ÿ�η����¼�����Ϣ
	 * 
	 * @author admin
	 * 
	 */
	public class CarEvnetInfo {
		public int flag = 0;
		public long time = 0;
		public InfoContainer info = null;
	}

	/**
	 * ���������24Сʱͳ�ƽ����Ϣ��
	 * 
	 * @author admin
	 * 
	 */
	public class AllAreaResultInfoContainer {
		private HashMap<Integer, List> map = null;

		public AllAreaResultInfoContainer() {
			this.map = new HashMap<Integer, List>();
		}

		/**
		 * �õ����ս��
		 * 
		 * @return
		 */
		public HashMap<Integer, List> getResultMap() {
			return this.map;
		}

		/**
		 * �õ�ĳ������ĳ��Сʱ��ͳ�ƽ��
		 * 
		 * @param areaId
		 * @param hour
		 * @return
		 */
		public HourResultInfo getResultInfo(int areaId, int hour) {
			List hourList = this.map.get(areaId);
			if (hourList != null) {
				return (HourResultInfo) hourList.get(hour);
			}else{
				return new HourResultInfo();
			}
		}

		/**
		 * Ϊĳ�������ĳ��Сʱ���н���ۼ�
		 * 
		 * @param areaId
		 * @param hour
		 * @param hourResultInfo
		 */
		public void addResultInfo(int areaId, int hour,
				HourResultInfo hourResultInfo) {
			if (hourResultInfo == null) {
				return;
			}
			List hourList = this.map.get(areaId);
			if (hourList == null) {
				hourList = new ArrayList(24);
				for(int i=0;i<24;i++){
					hourList.add(new HourResultInfo());
				}
				this.map.put(areaId, hourList);
			}
			HourResultInfo tempResultInfo = (HourResultInfo) hourList.get(hour);
			if (tempResultInfo == null) {
				tempResultInfo = hourResultInfo;
				hourList.add(hour, tempResultInfo);
			} else {
				tempResultInfo.freeInNum += hourResultInfo.freeInNum;
				tempResultInfo.loadInNum += hourResultInfo.loadInNum;
				tempResultInfo.freeOutNum += hourResultInfo.freeOutNum;
				tempResultInfo.loadOutNum += hourResultInfo.loadOutNum;
			}
		}
	}

	/**
	 * Сʱͳ�ƽ��������
	 * 
	 * @author admin
	 * 
	 */
	public class HourResultInfo {
		public int hour = 0;
		public int freeInNum = 0;
		public int loadInNum = 0;
		public int freeOutNum = 0;
		public int loadOutNum = 0;

		/**
		 * ��������
		 * 
		 * @return
		 */
		public int getInTotalNum() {
			return this.freeInNum + this.loadInNum;
		}

		/**
		 * �뿪����
		 * 
		 * @return
		 */
		public int getOutTotalNum() {
			return this.freeOutNum + this.loadOutNum;
		}
	}

}
