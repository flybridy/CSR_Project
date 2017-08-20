package com.fleety.server.area;

import java.awt.Polygon;
import java.awt.Shape;
import java.awt.geom.Ellipse2D;
import java.awt.geom.Rectangle2D;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import server.db.DbServer;

import com.fleety.base.GeneralConst;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;

public class AreaDataLoadServer {
	private int areaType_1 = 1;//圆与椭圆
	private int areaType_2 = 2;//矩形
	private int areaType_3 = 3;//多边形
	/**
	 * 查询所有区域
	 * @param type 区域类型 驻点区域类型为100
	 * @param isPlan 是否查询驻点区域计划与绑定车辆
	 * @return
	 */
	public HashMap<Integer, AreaInfo> getAreaData(int type,boolean isPlan){
		HashMap<Integer, AreaInfo> areaMapping = new HashMap<Integer, AreaInfo>(); 
		DbHandle conn =DbServer.getSingleInstance().getConn();
		try {
			String sql = "select area_id,cname,point_index,longitude,latitude,message,message2,type,area_type from alarm_area where 1=1 and type=" + type + " order by area_id,point_index";
			StatementHandle stmt = conn.createStatement();
			ResultSet sets = stmt.executeQuery(sql);
			AreaInfo areaInfo = null;
			while (sets.next()) {
				int areaId = sets.getInt(1);
				String cname = sets.getString(2);
				int areaType = sets.getInt(9);
				if(areaMapping.containsKey(areaId)){
					areaInfo = areaMapping.get(areaId);
				}else {
					areaInfo = new AreaInfo(areaId, cname, sets.getInt(9));
				}
				if(areaType==this.areaType_1){
					areaInfo.addPointEllipse(sets.getDouble(4), sets.getDouble(5));
				}else if (areaType==this.areaType_2) {
					areaInfo.addPointRectangle(sets.getDouble(4), sets.getDouble(5));
				}else if (areaType==this.areaType_3) {
					areaInfo.addPointPolygon((int)(sets.getDouble(4)*10000000),(int)(sets.getDouble(5)*10000000));
				}
				areaMapping.put(areaId, areaInfo);
			}
			if(isPlan){
				this.getAreaPlanBind(areaMapping);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}
		return areaMapping;
	}
	/**
	 * 查询单个区域信息
	 * @param area_id
	 * @return
	 */
	public AreaInfo getAreaData(int area_id){
		AreaInfo areaInfo = null; 
		DbHandle conn =DbServer.getSingleInstance().getConn();
		try {
			String sql = "select area_id,cname,point_index,longitude,latitude,message,message2,type,area_type from alarm_area where 1=1 and area_id="+area_id+" order by area_id,point_index";
			StatementHandle stmt = conn.createStatement();
			ResultSet sets = stmt.executeQuery(sql);
			while (sets.next()) {
				int areaId = sets.getInt(1);
				String cname = sets.getString(2);
				int areaType = sets.getInt(9);
				if(areaInfo==null){
					areaInfo = new AreaInfo(areaId, cname, sets.getInt(9));
				}
				if(areaType==this.areaType_1){
					areaInfo.addPointEllipse(sets.getDouble(4), sets.getDouble(5));
				}else if (areaType==this.areaType_2) {
					areaInfo.addPointRectangle(sets.getDouble(4), sets.getDouble(5));
				}else if (areaType==this.areaType_3) {
					areaInfo.addPointPolygon((int)(sets.getDouble(4)*10000000),(int)(sets.getDouble(5)*10000000));
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}
		return areaInfo;
	}
	/**
	 * 查询驻点区域计划以及绑定车辆
	 * @param areaMapping
	 */
	public void getAreaPlanBind(HashMap<Integer, AreaInfo> areaMapping){
		DbHandle conn =DbServer.getSingleInstance().getConn();
		try {
			String sql = "select b.plan_id,b.car_no,p.plan_name,p.start_time,p.end_time,p.area_id,p.status,p.start_time1,p.end_time1 from car_area_plan_bind_info b inner join car_area_plan_info p on b.plan_id=p.id where sysdate-1>=p.start_time and sysdate-1<p.end_time+1 order by p.area_id,b.plan_id";
			StatementHandle stmt = conn.createStatement();
			ResultSet sets = stmt.executeQuery(sql);
			AreaInfo areaInfo = null;
			PlanInfo planInfo = null;
			BindCarInfo bindCarInfo = null;
			while (sets.next()) {
				int areaId = sets.getInt("area_id");
				String carNo = sets.getString("car_no");
				if(areaMapping.containsKey(areaId)){
					areaInfo = areaMapping.get(areaId);
					if(areaInfo==null){
						return;
					}
					HashMap<Integer,PlanInfo> pInfos = areaInfo.getpInfos();
					if(pInfos==null){
						pInfos = new HashMap<Integer,PlanInfo>();
					}
					int planId = sets.getInt("plan_id");
					if(pInfos.containsKey(planId)){
						planInfo = pInfos.get(planId);
					}else {
						planInfo = new PlanInfo();
					}
					planInfo.setPlan_id(planId);
					planInfo.setArea_id(areaId);
					planInfo.setPlan_name(sets.getString("plan_name"));
					planInfo.setEnd_time(sets.getDate("end_time"));
					planInfo.setStart_time(sets.getDate("start_time"));
					planInfo.setStatus(sets.getInt("status"));
					planInfo.setStart_time1(sets.getString("start_time1"));
					planInfo.setEnd_time1(sets.getString("end_time1"));
					HashMap<String,BindCarInfo> bInfos = planInfo.getbInfos();
					if(bInfos==null){
						bInfos = new HashMap<String,BindCarInfo>();
					}
					bindCarInfo = new BindCarInfo(planId, carNo);
					bInfos.put(carNo,bindCarInfo);
					planInfo.setbInfos(bInfos);
					pInfos.put(planId, planInfo);
					areaInfo.setpInfos(pInfos);
				}
				areaMapping.put(areaId, areaInfo);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}
	}
	public static void main(String[] args) {
		DbServer.getSingleInstance().addPara("driver", "oracle.jdbc.driver.OracleDriver");
		DbServer.getSingleInstance().addPara("url", "jdbc:oracle:thin:@192.168.0.145:1521:ORCL");
		DbServer.getSingleInstance().addPara("user", "itop");
		DbServer.getSingleInstance().addPara("pwd", "itop");
		DbServer.getSingleInstance().addPara("init_num", "1");
		DbServer.getSingleInstance().startServer();
		AreaDataLoadServer aa = new AreaDataLoadServer();
		HashMap<Integer,AreaInfo> areaData = aa.getAreaData(5, true);
		for (Iterator iterator = areaData.values().iterator(); iterator.hasNext();) {
			AreaInfo areaInfo = (AreaInfo) iterator.next();
			HashMap<Integer,PlanInfo> pInfos = areaInfo.getpInfos();
			if(pInfos==null){
				continue;
			}
			for (Iterator iterator2 = pInfos.values().iterator(); iterator2.hasNext();) {
				PlanInfo planInfo = (PlanInfo) iterator2.next();
				HashMap<String,BindCarInfo> bInfos = planInfo.getbInfos();
				if(bInfos==null){
					continue;
				}
				for (Iterator iterator3 = bInfos.values().iterator(); iterator3.hasNext();) {
					BindCarInfo bInfo = (BindCarInfo)iterator3.next();
					Shape areaShape = areaInfo.getAreaShape();
					if (areaShape instanceof Ellipse2D.Double) {
						Ellipse2D.Double ellipse = (Ellipse2D.Double) areaShape;
						System.out.println("ellipse::::::::::"+ellipse.x+" "+ellipse.y+" "+ellipse.height+" "+ellipse.width+" "+areaInfo.getAreaId()+" "+areaInfo.getCname()+" "+areaInfo.getAreaType()+" "+planInfo.getPlan_id()+" "+planInfo.getPlan_name()+" "+GeneralConst.YYYY_MM_DD.format(planInfo.getStart_time())+" "+GeneralConst.YYYY_MM_DD.format(planInfo.getEnd_time())+" "+planInfo.getStatus()+" "+bInfo.getCar_no());
					}else if(areaShape instanceof Rectangle2D.Double){
						Rectangle2D.Double rectangle =  (Rectangle2D.Double)areaShape;
						System.out.println("rectangle::::::::::"+rectangle.x+" "+rectangle.y+" "+rectangle.height+" "+rectangle.width+" "+areaInfo.getAreaId()+" "+areaInfo.getCname()+" "+areaInfo.getAreaType()+" "+planInfo.getPlan_id()+" "+planInfo.getPlan_name()+" "+GeneralConst.YYYY_MM_DD.format(planInfo.getStart_time())+" "+GeneralConst.YYYY_MM_DD.format(planInfo.getEnd_time())+" "+planInfo.getStatus()+" "+bInfo.getCar_no());
					}else {
						Polygon polygon = (Polygon) areaShape;
						System.out.println("polygon::::::::::"+polygon.npoints+" "+areaInfo.getAreaId()+" "+areaInfo.getCname()+" "+areaInfo.getAreaType()+" "+planInfo.getPlan_id()+" "+planInfo.getPlan_name()+" "+GeneralConst.YYYY_MM_DD.format(planInfo.getStart_time())+" "+GeneralConst.YYYY_MM_DD.format(planInfo.getEnd_time())+" "+planInfo.getStatus()+" "+bInfo.getCar_no());
					}
				}
			}
		}
	}
}
