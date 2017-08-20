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
	private int preTime = 20;// 单位分钟
	private final static int STATUS_FREE = 0; // 空车
	private final static int STATUS_LOAD = 1; // 重车

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

				// 第一件事情，判断点和区域的关系
				isInArea = keyAreaInfo.isInArea(lo, la);
				info.setInfo("is_in", isInArea);
				lastPointInfo = (InfoContainer) keyAreaInfo.statMap.get(destNo);
				// 第二件事情，如果是第一个点只需要缓存此点
				if (lastPointInfo == null) {
					lastPointInfo = info;
					keyAreaInfo.statMap.put(destNo, lastPointInfo);
					continue;
				}
				// 第三件事情，如果前一个点和当前点的时间差超过规定时间，采用当前点作为最后一次点
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
					// 如果是当前在区域内，是进入区域，需要传出此时的车辆状态，时间点，进出标志
					if (isInArea) {
						// 如果当前点是空车，空车进入区域
						// 如果当前点是重车，重车进入区域
						if (curStatus == STATUS_FREE) {
							event.flag = FREEIN;
						} else if (curStatus == STATUS_LOAD) {
							event.flag = LOADIN;
						}
					} else {
						// 如果当前点是空车，空车离开区域
						// 如果当前点是重车，重车离开区域
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

		// 将当前统计结果累加到总结果中去
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
		// 将总结果记录到数据库中
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

	/************************************** 以下是自定义的数据结构 *********************************/

	/**
	 * 所有区域的关于某台车辆所有进出事件容器
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
		 * 车辆发生进出区域事件
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
		 * 得到车辆在某个区域某个时段的统计信息
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
	 * 某个区域的某台车辆所有进出事件容器
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
		 * 得到该区域该车一个小时的统计信息
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
	 * 车辆每次发生事件的信息
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
	 * 所有区域的24小时统计结果信息类
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
		 * 得到最终结果
		 * 
		 * @return
		 */
		public HashMap<Integer, List> getResultMap() {
			return this.map;
		}

		/**
		 * 得到某个区域某个小时的统计结果
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
		 * 为某个区域的某个小时进行结果累加
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
	 * 小时统计结果对象类
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
		 * 进入总数
		 * 
		 * @return
		 */
		public int getInTotalNum() {
			return this.freeInNum + this.loadInNum;
		}

		/**
		 * 离开总数
		 * 
		 * @return
		 */
		public int getOutTotalNum() {
			return this.freeOutNum + this.loadOutNum;
		}
	}

}
