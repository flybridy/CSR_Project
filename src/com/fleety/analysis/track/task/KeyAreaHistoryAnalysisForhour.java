package com.fleety.analysis.track.task;

import java.awt.Polygon;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;

import server.db.DbServer;

import com.fleety.analysis.AnalysisServer;
import com.fleety.analysis.realtime.OverRateRealTimeBean;
import com.fleety.analysis.track.ITrackAnalysis;
import com.fleety.analysis.track.TrackInfo;
import com.fleety.base.GeneralConst;
import com.fleety.base.InfoContainer;
import com.fleety.track.TrackIO;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;

public class KeyAreaHistoryAnalysisForhour implements ITrackAnalysis {
	private HashMap comMapping = null;
	private int statDuration = 10 * 60 * 1000;
	private int KEY_AREA_TYPE = 4; // 重点区域编号
    private HashMap<String, OverRateRealTimeBean> resMap=new HashMap<String, OverRateRealTimeBean>();
	@Override
	public boolean startAnalysisTrack(AnalysisServer parentServer,
			InfoContainer statInfo) {
		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
		Date eTime = statInfo.getDate(STAT_END_TIME_DATE);
		this.comMapping = null;
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			StatementHandle stmt = conn
					.prepareStatement("select * from area_wranning_parameter where record_time between ? and ?");

			stmt.setTimestamp(1, new Timestamp(sTime.getTime() + 1000));
			stmt.setTimestamp(2, new Timestamp(eTime.getTime()));
			ResultSet sets = stmt.executeQuery();
			if (!sets.next()) {
				this.comMapping = new HashMap();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}

		if (this.comMapping == null) {
			System.out.println("Not Need Analysis:" + this.toString());

		} else {
			System.out.println("Start Analysis:" + this.toString());
		}

		return this.comMapping != null;
	}

	@Override
	public void analysisDestTrack(AnalysisServer parentServer,
			TrackInfo trackInfo) {
		if (this.comMapping == null) {
			return;
		}
		if (trackInfo.trackArr == null) {
			return;
		}
		// 获取所有重点区域
		ArrayList destList = new ArrayList(1024);
		HashMap areaMap = new HashMap();
		DbHandle conn = DbServer.getSingleInstance().getConn();
		StatementHandle stmt = null;
		KeyAreaInfo keyAreaInfo;
		try {
			stmt = conn.prepareStatement("select area_id,cname,longitude lo,"
					+ "latitude la from alarm_area where type=" + KEY_AREA_TYPE
					+ " order by area_id,point_index");

			keyAreaInfo = null;
			ResultSet areaSets = stmt.executeQuery();
			int areaId;
			while (areaSets.next()) {
				areaId = areaSets.getInt("area_id");
				keyAreaInfo = (KeyAreaInfo) areaMap.get(areaId);
				if (keyAreaInfo == null) {
					keyAreaInfo = new KeyAreaInfo();
					keyAreaInfo.setAreaId(areaId);
					keyAreaInfo.setCname(areaSets.getString("cname"));
					keyAreaInfo.setType(KEY_AREA_TYPE);
					areaMap.put(areaId, keyAreaInfo);
				}
				keyAreaInfo.los.add(areaSets.getDouble("lo"));
				keyAreaInfo.las.add(areaSets.getDouble("la"));
				keyAreaInfo.setPointsNum(keyAreaInfo.getPointsNum() + 1);
			}
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
		Iterator itr = areaMap.values().iterator();
		while (itr.hasNext()) {
			keyAreaInfo = (KeyAreaInfo) itr.next();
			keyAreaInfo.initPolygon();
			// 到这里areaMap中存放的是一个个重点区域对象keyAreaInfo,并且每个区域对象都根据经纬度初始化成了多边形
		}

		// 将存放重点区域的map传入getAreaId方法，判断该轨迹信息是否在某个区域内，
		int area_id = this.getAreaId(trackInfo.trackArr, areaMap);
		if(area_id==-1){
			//若area_id=-1,则表示该轨迹信息不在重点区域内
			DbServer.getSingleInstance().releaseConn(conn);
			return;
		}
		int comId = trackInfo.dInfo.companyId;
		String key =  comId+ "_" + area_id;
		
		//这里判断全局的comMaping中是否已经有前面创建好的结果集对象（即以 区域_公司 为key存放一条数据对象）。
		AreaDataInfo dataInfo = (AreaDataInfo) this.comMapping.get(key);
		if (dataInfo == null) {
			dataInfo = new AreaDataInfo();
			dataInfo.group_index=key;
			dataInfo.area_id=area_id;
			dataInfo.company_id=comId;
			this.comMapping.put(key, dataInfo);
		}
		//创建实体类的方式，并放在集合中。
		/*OverRateRealTimeBean bean=null;
		if (resMap.containsKey(key)) {
			bean = resMap.get(key);
		} else {
			bean = new OverRateRealTimeBean();
			bean.setCompany_id(comId);
			bean.setArea_id(area_id);
		}
		resMap.put(key, bean);//取值运算完成之后添加*/
		
		/**
		 * 每辆车按照轨迹时间顺序计算过去，每10分钟采样一次车辆状态并按照机构记录
		 * 记录时每小时取所有10分钟的均值进行记录，如果单个10分钟的采样点样本数据不足总数的一半，那么则放弃该采样点信息。
		 * */
		long limitDuration = 180000;
		long sTime = trackInfo.sDate.getTime();
		int statIndex = 0, curIndex = 0;
		long preTime = -1;
		Calendar time = Calendar.getInstance();
		int status, preStatus = -1;
		int type_id = 0;
		String car_no = trackInfo.dInfo.destNo;
		StatementHandle stmt1 = null;
		try {
			stmt1 = conn.createStatement();
			ResultSet sets = stmt1
					.executeQuery("select type_id from car where car_id = '"
							+ car_no + "'");
			if (sets.next()) {
				type_id = sets.getInt("type_id");
			}
			sets.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		DbServer.getSingleInstance().releaseConn(conn);
		for (int i = 0; i < trackInfo.trackArr.length; i++) {
			time.setTime(trackInfo.trackArr[i].getDate(TrackIO.DEST_TIME_FLAG));
			curIndex = (int) ((time.getTimeInMillis() - sTime) / statDuration);
			status = trackInfo.trackArr[i].getInteger(TrackIO.DEST_STATUS_FLAG)
					.intValue() & 0x0F;
			if (curIndex > statIndex) {
				if (preTime > 0) {
					time.setTimeInMillis(sTime + (statIndex + 1)
							* this.statDuration);

					if (time.getTimeInMillis() - preTime < limitDuration) {
						if (preStatus == 0 || preStatus == 8) {// 空车数
							if (type_id == 1)
								dataInfo.Empty_num_red[statIndex]++;
							else if (type_id == 2)
								dataInfo.Empty_num_green[statIndex]++;
							else if (type_id == 3)
								dataInfo.Empty_num_electric[statIndex]++;
							else if (type_id == 4)
								dataInfo.Empty_num_accessible[statIndex]++;
						} else if (preStatus == 1 || preStatus == 9) {// 重车数
							if (type_id == 1)
								dataInfo.Overload_num_red[statIndex]++;
							else if (type_id == 2)
								dataInfo.Overload_num_green[statIndex]++;
							else if (type_id == 3)
								dataInfo.Overload_num_electric[statIndex]++;
							else if (type_id == 4)
								dataInfo.Overload_num_accessible[statIndex]++;
						} else if (preStatus == 2) {
							if (type_id == 1)
								dataInfo.Task_num_red[statIndex]++;
							else if (type_id == 2)
								dataInfo.Task_num_green[statIndex]++;
							else if (type_id == 3)
								dataInfo.Task_num_electric[statIndex]++;
							else if (type_id == 4)
								dataInfo.Task_num_accessible[statIndex]++;
						} else {
							if (type_id == 1)
								dataInfo.Other_num_red[statIndex]++;
							else if (type_id == 2)
								dataInfo.Other_num_green[statIndex]++;
							else if (type_id == 3)
								dataInfo.Other_num_electric[statIndex]++;
							else if (type_id == 4)
								dataInfo.Other_num_accessible[statIndex]++;
						}
					}
				}

				statIndex = curIndex;
			}

			preTime = time.getTimeInMillis();
			preStatus = status;
		}

		if (preTime > 0) {
			time.setTimeInMillis(sTime + (curIndex + 1) * this.statDuration);
			if (time.getTimeInMillis() - preTime < limitDuration) {
				if (preStatus == 0) {
					if (type_id == 1)
						dataInfo.Empty_num_red[curIndex]++;
					else if (type_id == 2)
						dataInfo.Empty_num_green[curIndex]++;
					else if (type_id == 3)
						dataInfo.Empty_num_electric[curIndex]++;
					else if (type_id == 4)
						dataInfo.Empty_num_accessible[curIndex]++;
				} else if (preStatus == 1) {
					if (type_id == 1)
						dataInfo.Overload_num_red[curIndex]++;
					else if (type_id == 2)
						dataInfo.Overload_num_green[curIndex]++;
					else if (type_id == 3)
						dataInfo.Overload_num_electric[curIndex]++;
					else if (type_id == 4)
						dataInfo.Overload_num_accessible[curIndex]++;
				} else if (preStatus == 2) {
					if (type_id == 1)
						dataInfo.Task_num_red[curIndex]++;
					else if (type_id == 2)
						dataInfo.Task_num_green[curIndex]++;
					else if (type_id == 3)
						dataInfo.Task_num_electric[curIndex]++;
					else if (type_id == 4)
						dataInfo.Task_num_accessible[curIndex]++;
				} else {
					if (type_id == 1)
						dataInfo.Other_num_red[curIndex]++;
					else if (type_id == 2)
						dataInfo.Other_num_green[curIndex]++;
					else if (type_id == 3)
						dataInfo.Other_num_electric[curIndex]++;
					else if (type_id == 4)
						dataInfo.Other_num_accessible[curIndex]++;
				}
			}
		}
	}

	@Override
	public void endAnalysisTrack(AnalysisServer parentServer,
			InfoContainer statInfo) {
		if (this.comMapping == null) {
			return;
		}
		

		int recordNum = 0;
		Date sDate = statInfo.getDate(STAT_START_TIME_DATE);
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			conn.setAutoCommit(false);
			Calendar cal = Calendar.getInstance();
			String datakey;
			int totalNum, count, Overload_num_red, Empty_num_red, Task_num_red, Other_num_red, Overload_num_green, Empty_num_green, Task_num_green, Other_num_green, Overload_num_electric, Empty_num_electric, Task_num_electric, Other_num_electric, Overload_num_accessible, Empty_num_accessible, Task_num_accessible, Other_num_accessible;

			AreaDataInfo dataInfo;
			int step = (int) (GeneralConst.ONE_HOUR_TIME / this.statDuration);

			StatementHandle stmt = conn
					.prepareStatement("insert into area_wranning_parameter(id ,Area_id,Company_id ,record_time ,Overload_num_red ,Empty_num_red ,Task_num_red ,Other_num_red ,Overload_num_green ,Empty_num_green ,Task_num_green ,Other_num_green ,Overload_num_electric ,Empty_num_electric ,Task_num_electric ,Other_num_electric ,Overload_num_accessible ,Empty_num_accessible ,Task_num_accessible ,Other_num_accessible ) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");

			for (Iterator itr = this.comMapping.keySet().iterator(); itr
					.hasNext();) {
				datakey = (String) itr.next();
				dataInfo = (AreaDataInfo) this.comMapping.get(datakey);
				cal.setTime(sDate);
				for (int i = 0; i < 24; i++) {
					cal.set(Calendar.HOUR_OF_DAY, i);
					Overload_num_red = Empty_num_red = Task_num_red = Other_num_red = Overload_num_green = Empty_num_green = Task_num_green = Other_num_green = Overload_num_electric = Empty_num_electric = Task_num_electric = Other_num_electric = Overload_num_accessible = Empty_num_accessible = Task_num_accessible = Other_num_accessible = count = 0;
					for (int x = 0, index = i * step; x < step; x++, index++) {
						totalNum = dataInfo.Overload_num_red[index]
								+ dataInfo.Overload_num_green[index]
								+ dataInfo.Overload_num_electric[index]
								+ dataInfo.Overload_num_accessible[index]
								+ dataInfo.Empty_num_red[index]
								+ dataInfo.Empty_num_green[index]
								+ dataInfo.Empty_num_electric[index]
								+ dataInfo.Empty_num_accessible[index]
								+ dataInfo.Task_num_red[index]
								+ dataInfo.Task_num_green[index]
								+ dataInfo.Task_num_electric[index]
								+ dataInfo.Task_num_accessible[index]
								+ dataInfo.Other_num_red[index]
								+ dataInfo.Other_num_green[index]
								+ dataInfo.Other_num_electric[index]
								+ dataInfo.Other_num_accessible[index];
						if (totalNum > 0) {
							
						
							Overload_num_red += dataInfo.Overload_num_red[index];
							Empty_num_red += dataInfo.Empty_num_red[index];
							Task_num_red += dataInfo.Task_num_red[index];
							Other_num_red += dataInfo.Other_num_red[index];

							Overload_num_green += dataInfo.Overload_num_green[index];
							Empty_num_green += dataInfo.Empty_num_green[index];
							Task_num_green += dataInfo.Task_num_green[index];
							Other_num_green += dataInfo.Other_num_green[index];

							Overload_num_electric += dataInfo.Overload_num_electric[index];
							Empty_num_electric += dataInfo.Empty_num_electric[index];
							Task_num_electric += dataInfo.Task_num_electric[index];
							Other_num_electric += dataInfo.Other_num_electric[index];

							Overload_num_accessible += dataInfo.Overload_num_accessible[index];
							Empty_num_accessible += dataInfo.Empty_num_accessible[index];
							Task_num_accessible += dataInfo.Task_num_accessible[index];
							Other_num_accessible += dataInfo.Other_num_accessible[index];
							count++;
						}
					}
					if (count > 0) {
						stmt.setInt(1,(int) DbServer.getSingleInstance().getAvaliableId(conn,"area_wranning_parameter", "id"));
						stmt.setInt(2, dataInfo.area_id);
						stmt.setInt(3,dataInfo.company_id);
						stmt.setTimestamp(4,
								new Timestamp(cal.getTimeInMillis()));
						stmt.setInt(5, (int) Math.ceil(Overload_num_red*1.0 / count));
						stmt.setInt(6, (int) Math.ceil(Empty_num_red*1.0 / count));
						stmt.setInt(7, (int) Math.ceil(Task_num_red*1.0 / count));
						stmt.setInt(8, (int) Math.ceil(Other_num_red*1.0 / count));
						stmt.setInt(9, (int) Math.ceil(Overload_num_green*1.0 /count));
						stmt.setInt(10, (int) Math.ceil(Empty_num_green*1.0 / count));
						stmt.setInt(11, (int) Math.ceil(Task_num_green*1.0 / count));
						stmt.setInt(12, (int) Math.ceil(Other_num_green*1.0 / count));
						stmt.setInt(13, (int) Math.ceil(Overload_num_electric*1.0 / count));
						stmt.setInt(14, (int) Math.ceil(Empty_num_electric*1.0 / count));
						stmt.setInt(15, (int) Math.ceil(Task_num_electric*1.0 / count));
						stmt.setInt(16, (int) Math.ceil(Other_num_electric*1.0 / count));
						stmt.setInt(17, (int) Math.ceil(Overload_num_accessible*1.0 / count));
						stmt.setInt(18, (int) Math.ceil(Empty_num_accessible*1.0 / count));
						stmt.setInt(19, (int) Math.ceil(Task_num_accessible*1.0 / count));
						stmt.setInt(20, (int) Math.ceil(Other_num_accessible*1.0 / count));
						stmt.addBatch();	
						recordNum++;
					}
					
				}
				stmt.executeBatch();
			}
			conn.commit();
		} catch (Exception e) {
			e.printStackTrace();
			if (conn != null) {
				try {
					conn.rollback();
				} catch (Exception ee) {
					ee.printStackTrace();
				}
			}
			recordNum = 0;
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}
		System.out.println("Finish KeyAreaHistoryAnalysisForhour!!"
				+ " recordNum=" + recordNum);
	}

	public String toString() {
		return "KeyAreaHistoryAnalysisForhour";
	}

	private int getAreaId(InfoContainer[] info, HashMap areaMap) {
		if (info == null || info.length == 0) {
			return -1;// 如果没有轨迹文件，area_id设置为-1
		}
		InfoContainer ifc = info[info.length - 1];
		double la = ifc.getDouble(TrackIO.DEST_LA_FLAG);
		double lo = ifc.getDouble(TrackIO.DEST_LO_FLAG);
		Iterator it = areaMap.keySet().iterator();
		int areaId;
		KeyAreaInfo keyArea;
		while (it.hasNext()) {
			areaId = (Integer) it.next();
			keyArea = (KeyAreaInfo) areaMap.get(areaId);
			if (keyArea.isInArea(lo, la))
				return areaId;
		}
		return -1; // 如果轨迹点不在重点区域，area_id设置为-1
	}

	private class KeyAreaInfo {
		private int areaId;
		private String cname;
		private int pointsNum = 0;
		protected ArrayList<Double> los = new ArrayList<Double>();
		protected ArrayList<Double> las = new ArrayList<Double>();
		private int type;
		public HashMap statMap = new HashMap();

		public final static double delta = 1E7;
		private Polygon polygon;

		public int getAreaId() {
			return areaId;
		}

		public void setAreaId(int areaId) {
			this.areaId = areaId;
		}

		public String getCname() {
			return cname;
		}

		public void setCname(String cname) {
			this.cname = cname;
		}

		public int getPointsNum() {
			return pointsNum;
		}

		public void setPointsNum(int pointsNum) {
			this.pointsNum = pointsNum;
		}

		public int getType() {
			return type;
		}

		public void setType(int type) {
			this.type = type;
		}

		public void initPolygon() {
			int npoints = los.size();
			polygon = new Polygon();
			for (int i = 0; i < npoints; i++) {
				polygon.addPoint((int) (los.get(i) * delta),
						(int) (las.get(i) * delta));
			}
		}

		public boolean isInArea(double lo, double la) {
			return polygon.contains(lo * delta, la * delta);
		}

	}

	private class AreaDataInfo {
       public String group_index;
		public int company_id;
		public int area_id;
		public int[] Overload_num_red = new int[(int) (GeneralConst.ONE_DAY_TIME / statDuration)];
		public int[] Empty_num_red = new int[(int) (GeneralConst.ONE_DAY_TIME / statDuration)];
		public int[] Task_num_red = new int[(int) (GeneralConst.ONE_DAY_TIME / statDuration)];
		public int[] Other_num_red = new int[(int) (GeneralConst.ONE_DAY_TIME / statDuration)];
		public int[] Overload_num_green = new int[(int) (GeneralConst.ONE_DAY_TIME / statDuration)];
		public int[] Empty_num_green = new int[(int) (GeneralConst.ONE_DAY_TIME / statDuration)];
		public int[] Task_num_green = new int[(int) (GeneralConst.ONE_DAY_TIME / statDuration)];
		public int[] Other_num_green = new int[(int) (GeneralConst.ONE_DAY_TIME / statDuration)];
		public int[] Overload_num_electric = new int[(int) (GeneralConst.ONE_DAY_TIME / statDuration)];
		public int[] Empty_num_electric = new int[(int) (GeneralConst.ONE_DAY_TIME / statDuration)];
		public int[] Task_num_electric = new int[(int) (GeneralConst.ONE_DAY_TIME / statDuration)];
		public int[] Other_num_electric = new int[(int) (GeneralConst.ONE_DAY_TIME / statDuration)];
		public int[] Overload_num_accessible = new int[(int) (GeneralConst.ONE_DAY_TIME / statDuration)];
		public int[] Empty_num_accessible = new int[(int) (GeneralConst.ONE_DAY_TIME / statDuration)];
		public int[] Task_num_accessible = new int[(int) (GeneralConst.ONE_DAY_TIME / statDuration)];
		public int[] Other_num_accessible = new int[(int) (GeneralConst.ONE_DAY_TIME / statDuration)];
		
	}
	

}
