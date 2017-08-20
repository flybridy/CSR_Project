package com.fleety.job.task;

import java.awt.Polygon;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import server.db.DbServer;
import server.track.TrackServer;

import com.fleety.analysis.realtime.AreaCompanyCarBean;
import com.fleety.analysis.realtime.OverRateRealTimeBean;
import com.fleety.analysis.realtime.StopCarMessageBean;
import com.fleety.analysis.track.DestInfo;
import com.fleety.analysis.track.TrackInfo;
import com.fleety.base.InfoContainer;
import com.fleety.server.event.listener.CarBusinessStatFinishListener;
import com.fleety.track.TrackIO;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.fleety.util.pool.db.redis.RedisConnPoolServer;
import com.fleety.util.pool.db.redis.RedisTableBean;
import com.fleety.util.pool.thread.BasicTask;

/*本分析服务是将重载率最近前一个整点1小时的数据存储在redis数据库上，距离最近时间超出一个小时的数据存储在oracle数据库中。如，当前时间为8：30，则redis上存储8：00-8：30的数据，
 *8点及之前的整点数据被存储在oracle数据库。redis数据库一共储存6条数据，每十分钟一条（不包括起始），整点的数据选取该 小时最后一条数据插入数据库。
 */

public class OverloadRateRealTimeAnalysisTask extends BasicTask {
	private long refresh_interval = 10 * 60 * 1000;// 刷新间隔是10分钟
	private int KEY_AREA_TYPE = 4; // 重点区域编号
    private int company_id;
    private int del_flag;//删除redis数据的判断标志，第一次执行该类时候进行删除之前的数据。
    List<Integer> stopcar = null;
    AreaCompanyCarBean carbean = null;
   
    public OverloadRateRealTimeAnalysisTask(int company_id,int del_flag){
    	super();
    	this.company_id = company_id;
    	this.del_flag=del_flag;
    }
    
	public boolean execute() throws Exception {
		DbHandle conn = DbServer.getSingleInstance().getConnWithUseTime(0);
		DbHandle conn2 = DbServer.getSingleInstance().getConnWithUseTime(0);
		StatementHandle stmt2 = conn2
				.prepareStatement("insert into today_clock_overload(company_id,area_id,overload_num_red,empty_num_red,task_num_red,other_num_red,overload_num_green,empty_num_green,task_num_green,other_num_green,overload_num_electric,empty_num_electric,task_num_electric,other_num_electric,overload_num_accessible,empty_num_accessible,task_num_accessible,other_num_accessible,total_num,stop_num,index_num) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
		try {
			DestInfo dInfo;
			ArrayList destList = new ArrayList(1024);
			HashMap areaMap = new HashMap();
			try {
				StatementHandle stmt = conn
						.prepareStatement("select mdt_id,dest_no,company_id,company_name,type_id,gps_run_com_id,gps_run_com_name from v_ana_dest_info where company_id = "+this.company_id);
				ResultSet sets = stmt.executeQuery();
				while (sets.next()) {
					dInfo = new DestInfo();
					dInfo.mdtId = sets.getInt("mdt_id");
					dInfo.destNo = sets.getString("dest_no");
					dInfo.companyId = sets.getInt("company_id");
					dInfo.companyName = sets.getString("company_name");
					dInfo.gpsRunComId = sets.getInt("gps_run_com_id");
					dInfo.gpsRunComName = sets.getString("gps_run_com_name");
					dInfo.carType = sets.getInt("type_id");
					destList.add(dInfo);
				}
				//获取所有重点区域
				stmt = conn
						.prepareStatement("select area_id,cname,longitude lo,"
								+ "latitude la from alarm_area where type="
								+ KEY_AREA_TYPE
								+ " order by area_id,point_index");
				KeyAreaInfo keyAreaInfo = null;
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
				Iterator itr=areaMap.values().iterator();
				while(itr.hasNext()){
					keyAreaInfo=(KeyAreaInfo)itr.next();
					keyAreaInfo.initPolygon();
				}
			} catch (Exception e) {
				throw e;
			}
			// 每十分钟取样一次，以当前时间前一小时到当前时间轨迹速度为0，且位移为0作为判断依据
			
			Calendar now = Calendar.getInstance();// 当前时间
			now.set(Calendar.MINUTE,
					(int) (now.getTimeInMillis() % 3600000) / 600000 * 10);
			now.set(Calendar.SECOND, 0);
			now.set(Calendar.MILLISECOND, 0);
			Date eDate = now.getTime();
			
			int flag = 0;
			long temp1 = eDate.getTime();
			long temp2 = eDate.getTime() % 3600000;
			
			if ((eDate.getTime() % 3600000) >= 0
					&& (eDate.getTime() % 3600000) < 600000) {
				flag = 1;
			}
			now.setTimeInMillis(now.getTimeInMillis() - 300*1000 + 1000);
			Date sDate = now.getTime();
			InfoContainer queryInfo = new InfoContainer();
			queryInfo.setInfo(TrackServer.START_DATE_FLAG, sDate);
			queryInfo.setInfo(TrackServer.END_DATE_FLAG, eDate);
			TrackInfo trackInfo;
			HashMap<String, OverRateRealTimeBean> list = new HashMap<String, OverRateRealTimeBean>();
			HashMap<String, AreaCompanyCarBean> carlist = new HashMap<String, AreaCompanyCarBean>();
			HashMap<String,StopCarMessageBean> stoplist=new HashMap<String, StopCarMessageBean>();
			OverRateRealTimeBean bean = null;
			StopCarMessageBean stopbean=null;//停驶车辆数据的bean
			 
			for (Iterator itr = destList.iterator(); itr.hasNext();) {
				carbean = new AreaCompanyCarBean();
				stopbean=null;
				bean = null;
				
				dInfo = (DestInfo) itr.next();
				carbean.setCar_no(dInfo.destNo);
				queryInfo.setInfo(TrackServer.DEST_NO_FLAG, dInfo.destNo);

				trackInfo = new TrackInfo();
				trackInfo.dInfo = dInfo;
				trackInfo.sDate = sDate;
				trackInfo.eDate = eDate;
				trackInfo.trackArr = TrackServer.getSingleInstance()
						.getTrackInfo(queryInfo);
				int status = this.analysisTrack(trackInfo.trackArr);
				
				String service_no=this.getServiceNo(trackInfo.trackArr);
				carbean.setService_no(service_no);
				carbean.setStatus(status);
				List<Integer> area_id = this.getAreaId(trackInfo.trackArr,areaMap);
				for(int k=0;k<area_id.size();k++){
					if(area_id.get(k)==-1)
						continue;         //如果区域id为-1，就不做判断
				//用企业id,区域id组合区分不同区域，不同企业的数据(公司id_区域id)
				if (list.containsKey(String.valueOf(dInfo.companyId)+"_"+String.valueOf(area_id.get(k)))) {
					bean = list.get(String.valueOf(dInfo.companyId)+"_"+String.valueOf(area_id.get(k)));
					bean.setStop_num(bean.getStop_num()+stopcar.get(k));//这里bean.stopcar的值+1
				} else {
					bean = new OverRateRealTimeBean();
					bean.setCompany_id(dInfo.companyId);
					bean.setArea_id(area_id.get(k));
					bean.setStop_num(stopcar.get(k));
				}
				//判定停驶车辆的信息
				if (stopcar.get(k)==1){//list中值为1是停驶的车辆
					stopbean=new StopCarMessageBean();
					stopbean.setUid(String.valueOf(area_id.get(k))+"_"+String.valueOf(dInfo.companyId)+"_"+dInfo.destNo+ "_"
							+ String.valueOf(((eDate.getTime() % 3600000) / 600000 + 6) % 6));
					stopbean.setArea_id(area_id.get(k));
					stopbean.setCar_id(dInfo.destNo);
					stopbean.setCar_type(dInfo.carType);
					stopbean.setCompany_id(dInfo.companyId);
					stopbean.setCom_name(dInfo.companyName);
					stopbean.setService_no(service_no);
					stopbean.setIndex((int) (((eDate.getTime() % 3600000) / 600000 + 6) % 6));
					stoplist.put(String.valueOf(dInfo.companyId)+"_"+String.valueOf(area_id.get(k))+"_"+dInfo.destNo+"_"+ String.valueOf(((eDate.getTime() % 3600000) / 600000 + 6) % 6), 
							stopbean);
				} 
				carbean.setArea_id(area_id.get(k));
				carbean.setCompany_id(dInfo.companyId);
				carbean.setCar_type(trackInfo.dInfo.carType);
				carbean.setUid(String.valueOf(area_id.get(k))+"_"+String.valueOf(dInfo.companyId)+"_"+carbean.getCar_no()+ "_"
						+ String.valueOf(((eDate.getTime() % 3600000) / 600000 + 6) % 6));
				bean.setUid(String.valueOf(area_id.get(k))+"_"+String.valueOf(dInfo.companyId)
						+ "_"
						+ String.valueOf(((eDate.getTime() % 3600000) / 600000 + 6) % 6));// uid为“区域号_企业号_点序号”
				bean.setIndex((int) (((eDate.getTime() % 3600000) / 600000 + 6) % 6));// 在一个小时中，每10分钟一个点的点序号
				carbean.setIndex((int) (((eDate.getTime() % 3600000) / 600000 + 6) % 6));
				if (status == 0||status == 8) {
					switch (trackInfo.dInfo.carType) {
					case 1:
						bean.setEmpty_num_red(bean.getEmpty_num_red() + 1);
						break;
					case 2:
						bean.setEmpty_num_green(bean.getEmpty_num_green() + 1);
						break;
					case 3:
						bean.setEmpty_num_electric(bean.getEmpty_num_electric() + 1);
						break;
					case 4:
						bean.setEmpty_num_accessible(bean
								.getEmpty_num_accessible() + 1);
						break;
					}
					bean.setTotal_num(bean.getTotal_num() + 1);
				} else if (status == 1||status == 9) {
					switch (trackInfo.dInfo.carType) {
					case 1:
						bean.setOverload_num_red(bean.getOverload_num_red() + 1);
						break;
					case 2:
						bean.setOverload_num_green(bean.getOverload_num_green() + 1);
						break;
					case 3:
						bean.setOverload_num_electric(bean
								.getOverload_num_electric() + 1);
						break;
					case 4:
						bean.setOverload_num_accessible(bean
								.getOverload_num_accessible() + 1);
						break;
					}
					bean.setTotal_num(bean.getTotal_num() + 1);
				} else if (status == 2) {
					switch (trackInfo.dInfo.carType) {
					case 1:
						bean.setTask_num_red(bean.getTask_num_red() + 1);
						break;
					case 2:
						bean.setTask_num_green(bean.getTask_num_green() + 1);
						break;
					case 3:
						bean.setTask_num_electric(bean.getTask_num_electric() + 1);
						break;
					case 4:
						bean.setTask_num_accessible(bean
								.getTask_num_accessible() + 1);
						break;
					}
					bean.setTotal_num(bean.getTotal_num() + 1);
				} else if(status != -1) {
					switch (trackInfo.dInfo.carType) {
					case 1:
						bean.setOther_num_red(bean.getOther_num_red() + 1);
						break;
					case 2:
						bean.setOther_num_green(bean.getOther_num_green() + 1);
						break;
					case 3:
						bean.setOther_num_electric(bean.getOther_num_electric() + 1);
						break;
					case 4:
						bean.setOther_num_accessible(bean
								.getOther_num_accessible() + 1);
						break;
					}
					bean.setTotal_num(bean.getTotal_num() + 1);
				}
				list.put(String.valueOf(dInfo.companyId)+"_"+String.valueOf(area_id.get(k)), bean);
				carlist.put(String.valueOf(area_id.get(k))+"_"+String.valueOf(dInfo.companyId)+"_"+carbean.getCar_no(), carbean);
				}
			}
			OverRateRealTimeBean beans[] = new OverRateRealTimeBean[list.size()];
			AreaCompanyCarBean  carbeans[] = new AreaCompanyCarBean[carlist.size()];
			StopCarMessageBean stopbeans[] =new StopCarMessageBean[stoplist.size()];
			if (((eDate.getTime() / 3600000) % 24 + 8 ) % 24 == 0
					&& eDate.getTime() % (3600 * 1000) >= 0
					&& eDate.getTime() % (3600 * 1000) < 10 * 60 * 1000
					&& this.company_id == 3)// 如果过了0点，前一天的数据被删除
			{
				StatementHandle stmt = conn
						.prepareStatement("delete from today_clock_overload");
				stmt.execute();
				System.out.println("清除前一天数据成功！");
			}
			int i = 0;
			for (Iterator it = list.keySet().iterator(); it.hasNext();) {
				beans[i] = (OverRateRealTimeBean) list.get(it.next());
				if (flag == 1) {
					stmt2.setLong(1, beans[i].getCompany_id());
					stmt2.setLong(2, beans[i].getArea_id());
					stmt2.setLong(3, beans[i].getOverload_num_red());
					stmt2.setLong(4, beans[i].getEmpty_num_red());
					stmt2.setLong(5, beans[i].getTask_num_red());
					stmt2.setLong(6, beans[i].getOther_num_red());
					stmt2.setLong(7, beans[i].getOverload_num_green());
					stmt2.setLong(8, beans[i].getEmpty_num_green());
					stmt2.setLong(9, beans[i].getTask_num_green());
					stmt2.setLong(10, beans[i].getOther_num_green());
					stmt2.setLong(11, beans[i].getOverload_num_electric());
					stmt2.setLong(12, beans[i].getEmpty_num_electric());
					stmt2.setLong(13, beans[i].getTask_num_electric());
					stmt2.setLong(14, beans[i].getOther_num_electric());
					stmt2.setLong(15, beans[i].getOverload_num_accessible());
					stmt2.setLong(16, beans[i].getEmpty_num_accessible());
					stmt2.setLong(17, beans[i].getTask_num_accessible());
					stmt2.setLong(18, beans[i].getOther_num_accessible());
					stmt2.setLong(19, beans[i].getTotal_num());
					stmt2.setLong(20, beans[i].getStop_num());
					stmt2.setInt(
							21,
							(int) ((eDate.getTime() / 3600000) % 24 + 8 ) % 24);// 在一天中，每个小时一个点的点序号(当正好在整点上时，算在前一个时段，如九点算出来的数据序号是8)
					stmt2.addBatch();
					if (i % 200 == 0)
						stmt2.executeBatch();
				}
				i++;
			}
			stmt2.executeBatch();
			
			int k=0;
			for(Iterator it2 = carlist.keySet().iterator(); it2.hasNext();){
				carbeans[k] = carlist.get(it2.next());
				k++;
			}
			
			//把停驶车辆的数据保存近redis中
			int j=0;
			for(Iterator it3 = stoplist.keySet().iterator(); it3.hasNext();){
				stopbeans[j] = stoplist.get(it3.next());
				j++;
			}
			/*System.out.println("delete condition:"+((eDate.getTime() % 3600000) >= 0)
					+((eDate.getTime() % 3600000) <= 600000)
					+(this.del_flag == 0));*/
			
			if ((eDate.getTime() % 3600000) >= 0
					&& (eDate.getTime() % 3600000) < 600000
					&& this.del_flag == 0) {
				System.out.println("delete in condition ");
				this.deleteInfo();
				this.deleteCarInfo();
				this.deleteStopInfo();
			}
			
			RedisConnPoolServer.getSingleInstance().saveTableRecord(carbeans);
			RedisConnPoolServer.getSingleInstance().saveTableRecord(beans);
			RedisConnPoolServer.getSingleInstance().saveTableRecord(stopbeans);
		} catch (Exception e) {
			throw e;
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
			DbServer.getSingleInstance().releaseConn(conn2);
		}
		return false;
	}

	private List<Integer> getAreaId(InfoContainer[] info,HashMap areaMap) {
		List<Integer> list = new ArrayList();
		stopcar = new ArrayList<Integer>();
		if (info == null||info.length==0) {
			list.add(-1);
			stopcar.add(0);
			return list;// 如果没有轨迹文件，area_id设置为-1
		}
		InfoContainer ifc = info[info.length - 1];//倒数第一个轨迹点
		double la = ifc.getDouble(TrackIO.DEST_LA_FLAG);//
		double lo = ifc.getDouble(TrackIO.DEST_LO_FLAG);
		carbean.setLa(ifc.getDouble(TrackIO.DEST_LA_FLAG));
		carbean.setLo(ifc.getDouble(TrackIO.DEST_LO_FLAG));
		Iterator it = areaMap.keySet().iterator();
		int areaId = -1;
		KeyAreaInfo keyArea;
		while(it.hasNext())
		{
			areaId = (Integer)it.next();
			keyArea = (KeyAreaInfo) areaMap.get(areaId);
			if(keyArea.isInArea(lo, la)){//判定最后一个点在该区域内，继续往前判断其余点。
				int i;
				float k=0;
				for(i=0;i<info.length-1;i++){
//					if(!(Math.abs(info[i].getDouble(TrackIO.DEST_LA_FLAG)-info[info.length-1].getDouble(TrackIO.DEST_LA_FLAG))<0.0001)||!(Math.abs(info[i].getDouble(TrackIO.DEST_LA_FLAG)-info[info.length-1].getDouble(TrackIO.DEST_LA_FLAG))<0.0001)||info[i].getDouble(TrackIO.DEST_SPEED_FLAG)!=0)
					if(keyArea.isInArea(info[i].getDouble(TrackIO.DEST_LO_FLAG),info[i].getDouble(TrackIO.DEST_LA_FLAG)))
						k=k+1;
				}
				if((k/info.length)>0.8){//轨迹中百分之八十的轨迹点速度为零则作为停驶车辆
					stopcar.add(1);
				}else{
					stopcar.add(0);
				}
				list.add(areaId);
//				else 
//					list.add(-1);
			}
		}
		//最后一个点不在区域范围内
		if(list.size()==0){
			list.add(-1);
			stopcar.add(0);
			}
      return list;
	}

	private int analysisTrack(InfoContainer[] info) {
		if (info == null||info.length==0) {
			return -1;// 如果没有轨迹文件，状态设置为-1
		}
		InfoContainer ifc = info[info.length - 1];
		return ifc.getInteger(TrackIO.DEST_STATUS_FLAG).intValue() &0x0F;
	}
	private String getServiceNo(InfoContainer[] info) {
		if (info == null||info.length==0) {
			return "-1";// 如果没有轨迹文件，驾驶员证号为null
		}
		InfoContainer ifc1 = info[info.length - 1];
		if(ifc1.getString(TrackIO.DRIVER_NO_INFO_FLAG)!=null && ifc1.getString(TrackIO.DRIVER_NO_INFO_FLAG).length()!=0 ){
			return ifc1.getString(TrackIO.DRIVER_NO_INFO_FLAG).intern();
		}else{
			return "-1";
			}
	}
	private void deleteInfo() throws Exception {
		OverRateRealTimeBean bean = new OverRateRealTimeBean();
		Set<String> keySet = RedisConnPoolServer.getSingleInstance()
				.getAllIdsForTable(bean);
		Iterator<String> it = keySet.iterator();
		String uid = "";
		List<OverRateRealTimeBean> list = new ArrayList<OverRateRealTimeBean>();
		while (it.hasNext()) {
			bean = new OverRateRealTimeBean();
			uid = it.next();
			bean.setUid(uid);
			list.add(bean);
		}
		if (list.size() > 0) {
			RedisTableBean[] beanArr = new OverRateRealTimeBean[list.size()];
			list.toArray(beanArr);
			RedisConnPoolServer.getSingleInstance().deleteTableRecord(beanArr);
		}
	}
	
	private void deleteCarInfo() throws Exception{
		AreaCompanyCarBean carbean = new AreaCompanyCarBean();
		List<AreaCompanyCarBean> carlist = new ArrayList<AreaCompanyCarBean>();
		Set<String> carkeySet = RedisConnPoolServer.getSingleInstance()
				.getAllIdsForTable(carbean);
		Iterator<String> carit = carkeySet.iterator();
		String uid = "";
		while (carit.hasNext()) {
			carbean = new AreaCompanyCarBean();
			uid = carit.next();
			carbean.setUid(uid);
			carlist.add(carbean);
		}
		if (carlist.size() > 0) {
			RedisTableBean[] carbeanArr = new AreaCompanyCarBean[carlist.size()];
			carlist.toArray(carbeanArr);
			RedisConnPoolServer.getSingleInstance().deleteTableRecord(carbeanArr);
		}
	}

	private void deleteStopInfo() throws Exception{
		StopCarMessageBean stopbean = new StopCarMessageBean();
		List<StopCarMessageBean> slist = new ArrayList<StopCarMessageBean>();
		Set<String> stopkeySet = RedisConnPoolServer.getSingleInstance()
				.getAllIdsForTable(stopbean);
		Iterator<String> stopit = stopkeySet.iterator();
		String uid = "";
		while (stopit.hasNext()) {
			stopbean = new StopCarMessageBean();
			uid = stopit.next();
			stopbean.setUid(uid);
			slist.add(stopbean);
		}
		if (slist.size() > 0) {
			RedisTableBean[] stopbeanArr = new StopCarMessageBean[slist.size()];
			slist.toArray(stopbeanArr);
			RedisConnPoolServer.getSingleInstance().deleteTableRecord(stopbeanArr);
		}
	}
	
	public String getDesc() {
		return "重载率实时分析";
	}

	public Object getFlag() {
		return "OverloadRateRealTimeAnalysisTask";
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
		
		public void initPolygon(){
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
