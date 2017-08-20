package com.fleety.job.task;

import java.awt.Polygon;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import server.db.DbServer;

import com.fleety.analysis.realtime.OrdinaryAreaCarBean;
import com.fleety.common.redis.Gps_Pos;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.fleety.util.pool.db.redis.RedisConnPoolServer;
import com.fleety.util.pool.db.redis.RedisTableBean;
import com.fleety.util.pool.thread.BasicTask;

public class OrdinaryAreaCarAnalysisTask extends BasicTask {
	private int offLineInterval = 15;
	private int faultLineInterval = 24 * 60;
	private int duration = 60 * 1000;

	@Override
	public boolean execute() throws Exception {
		HashMap<Integer,KeyAreaInfo> areaMap = new HashMap<Integer,KeyAreaInfo>();// ��������ͨ������
		List<ResObj> list = new ArrayList<ResObj>();
		Map<String, Integer> termMap = new HashMap<String, Integer>();//���泵�ƺţ���˾ID�ļ���
		DbHandle conn = DbServer.getSingleInstance().getConnWithUseTime(0);
		StatementHandle stmt = conn
				.prepareStatement("select area_id,cname,longitude lo,"
						+ "latitude la from alarm_area where type=" + 1
						+ " order by area_id,point_index");
		ResultSet areaSets = stmt.executeQuery();
		int areaId = 0;
		KeyAreaInfo OrdinaryAreaInfo = null;
		while (areaSets.next()) {
			areaId = areaSets.getInt("area_id");
			OrdinaryAreaInfo = (KeyAreaInfo) areaMap.get(areaId);
			if (OrdinaryAreaInfo == null) {
				OrdinaryAreaInfo = new KeyAreaInfo();
				OrdinaryAreaInfo.setAreaId(areaId);
				OrdinaryAreaInfo.setCname(areaSets.getString("cname"));
				OrdinaryAreaInfo.setType(1);
				areaMap.put(areaId, OrdinaryAreaInfo);
			}
			OrdinaryAreaInfo.los.add(areaSets.getDouble("lo"));
			OrdinaryAreaInfo.las.add(areaSets.getDouble("la"));
//			OrdinaryAreaInfo.setPointsNum(OrdinaryAreaInfo.getPointsNum() + 1);
		}
		// ����areaMap,��ÿ����ͨ������γɶ����
		Iterator itr = areaMap.values().iterator();
		while (itr.hasNext()) {
			OrdinaryAreaInfo = (KeyAreaInfo) itr.next();
			OrdinaryAreaInfo.initPolygon();
		}
		//��ȡÿ�����Ĺ�˾id
		StatementHandle stmt1 = conn.createStatement();
		String sql = "select dest_no,company_id from v_ana_dest_info";
		ResultSet res = stmt1.executeQuery(sql);
		while (res.next()) {
			termMap.put(res.getString("dest_no"), res.getInt("company_id"));
		}
		DbServer.getSingleInstance().releaseConn(conn);
		// ������г�����gps��Ϣ
		Map gpsMap = getVehicleGPSInfo();
		Iterator gpsit = gpsMap.keySet().iterator();
		KeyAreaInfo area = null;
		ResObj resobj = null;
		while (gpsit.hasNext()) {// ����GPSλ����Ϣ
			String car_no = (String) gpsit.next();
			if(termMap.get(car_no)!=null){
			GpsInfo gpsInfo = (GpsInfo) gpsMap.get(car_no);
			double lo = gpsInfo.lo;
			double la = gpsInfo.la;
			Iterator it = areaMap.keySet().iterator();
			while (it.hasNext()) {
				areaId = (Integer) it.next();
				area = (KeyAreaInfo) areaMap.get(areaId);
				if (area.isInArea(lo, la)) {// ��ĳ��������
					resobj = new ResObj();
					resobj.area_id = areaId;
					resobj.car_no = car_no;
					resobj.FreeLoadStatu = gpsInfo.FreeLoadStatu;
					resobj.OnlieStatu = gpsInfo.OnlieStatu;
					resobj.come_id = termMap.get(car_no);
					resobj.lo=lo;
					resobj.la=la;
					list.add(resobj);
				}
			}
			}
		}
		// �����ݴ洢��redis��
		OrdinaryAreaCarBean beans[] = new OrdinaryAreaCarBean[list.size()];
		for (int i = 0; i < list.size(); i++) {
			ResObj obj = list.get(i);
			OrdinaryAreaCarBean bean = new OrdinaryAreaCarBean();
			bean.setUid("OrdinaryCar_" + obj.car_no+obj.area_id);
			bean.setCar_no(obj.car_no);
			bean.setFreeLoadStatu(obj.FreeLoadStatu);
			bean.setOnlieStatu(obj.OnlieStatu);
			bean.setCome_id(obj.come_id);
			bean.setArea_id(obj.area_id);
			bean.setLo(obj.lo);
			bean.setLa(obj.la);
			beans[i] = bean;
		}
		this.deleteInfo();
		RedisConnPoolServer.getSingleInstance().saveTableRecord(beans);
		return false;
	}

	private void deleteInfo() throws Exception {
		OrdinaryAreaCarBean bean = new OrdinaryAreaCarBean();
		Set<String> keySet = RedisConnPoolServer.getSingleInstance()
				.getAllIdsForTable(bean);
		Iterator<String> it = keySet.iterator();
		String uid = "";
		List<OrdinaryAreaCarBean> list = new ArrayList<OrdinaryAreaCarBean>();
		while (it.hasNext()) {
			bean = new OrdinaryAreaCarBean();
			uid = it.next();
			bean.setUid(uid);
			list.add(bean);
		}
		if (list.size() > 0) {
			RedisTableBean[] beanArr = new OrdinaryAreaCarBean[list.size()];
			list.toArray(beanArr);
			RedisConnPoolServer.getSingleInstance().deleteTableRecord(beanArr);
		}
	}

	// ��ȡ������redis�е�GPSλ����Ϣ
	public HashMap getVehicleGPSInfo() {
		HashMap<String,GpsInfo> GpsInfoMap = new HashMap<String,GpsInfo>();
		try {
			Gps_Pos bean = new Gps_Pos();
			bean.setUid(null);
			List<Gps_Pos> gpsPosList = RedisConnPoolServer.getSingleInstance()
					.queryTableRecord(new RedisTableBean[] { bean });
			Date curDate = new Date();
			long currentTime = curDate.getTime();
			for (Gps_Pos gps : gpsPosList) {
				String plateNo = gps.getUid();
				GpsInfo Info = new GpsInfo();

				Info.car_no = plateNo;
				Info.lo = (new BigDecimal(gps.getLo() / 10000000.00).setScale(
						7, BigDecimal.ROUND_HALF_UP).doubleValue());
				Info.la = (new BigDecimal(gps.getLa() / 10000000.00).setScale(
						7, BigDecimal.ROUND_HALF_UP).doubleValue());

				if (gps.getState() == 0)
					Info.FreeLoadStatu = 0;// �ճ�
				else if (gps.getState() == 1)

					Info.FreeLoadStatu = 1;// �س�
				else if (gps.getState() == 2)
					Info.FreeLoadStatu = 2;// �������

				Date gpsreDate = (gps.getSysDate() == null ? gps.getDt() : gps
						.getSysDate());

				long gpsTime = gpsreDate.getTime();
				long interval = Math.abs(currentTime - gpsTime);
				if (interval >=faultLineInterval * duration) {
					Info.OnlieStatu = "����";
				} else if (interval >= offLineInterval * duration) {
					Info.OnlieStatu = "����";
				} else {
					Info.OnlieStatu = "����";
				}
				GpsInfoMap.put(plateNo, Info);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return GpsInfoMap;
	}

	private class GpsInfo {
		private String car_no;
		private int FreeLoadStatu;// 0�ճ���1Ϊ�س�
		private String OnlieStatu;// 0Ϊ���ߣ�2Ϊ���ߣ�3���ϡ�
		private String company_name;
		private double lo;// ����
		private double la;// γ��
	}

	private class ResObj {
		private String car_no;
		private int FreeLoadStatu;// 0�ճ���1Ϊ�س�
		private String OnlieStatu;// ����,���ߣ�����
		private int area_id;
		private int come_id;
		private double lo;//����
		private double la;//γ��
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
}
