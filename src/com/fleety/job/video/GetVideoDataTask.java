package com.fleety.job.video;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import server.db.DbServer;

import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.fleety.util.pool.db.redis.RedisConnPoolServer;
import com.fleety.util.pool.db.redis.RedisTableBean;
import com.fleety.util.pool.thread.BasicTask;

/**
 * 用于深圳视频获取新终端的车辆的相关信息
 * 
 * @author Administrator
 * 
 */
public class GetVideoDataTask extends BasicTask {

 HashMap<String, OrgInfo> ORG_INFO = new HashMap<String, OrgInfo>();

	public boolean execute() throws Exception {
		this.getVehicleInfo();
		this.getNewMdtOrg();
		return false;
	}

	public String getDesc() {
		return "获取车辆和机构的基本信息用于视频";
	}

	public Object getFlag() {
		return "GetVideoDataTask";
	}

	/**
	 * 获取所有的新终端的车辆信息
	 */
	public void getVehicleInfo() {
		Vehicle bean = new Vehicle();
		List<CarInfo> VEHICLE_INFO = new ArrayList<CarInfo>();
		CarInfo info = null;
		try {
			List list = RedisConnPoolServer.getSingleInstance()
					.queryTableRecord(new RedisTableBean[] { bean });
			String car_no = null;
			String channel = null;
			String parent_id = null;
			if (list != null && list.size() > 0) {
				for (int i = 0; i < list.size(); i++) {
					bean = (Vehicle) list.get(i);
					if (comMdtType(bean.getMdtId()) == 1) {
						parent_id = bean.getOid() + "";
						car_no = bean.getUid();
						channel = bean.getVideoCode();
						info = new CarInfo(parent_id, car_no, "", channel);
						VEHICLE_INFO.add(info);
					}
				}
				System.out.println("车辆的数据量大小："+VEHICLE_INFO.size());
				this.saveNewMdt(VEHICLE_INFO);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 将所有新终端的车辆信息保存到数据库
	 * 
	 * @param list
	 */
	public void saveNewMdt(List list) {
		DbHandle conn = DbServer.getSingleInstance().getConnWithUseTime(0);
		StatementHandle stmt = null;
		StatementHandle stmt1 = null;
		CarInfo info = null;
		try {
			conn.setAutoCommit(false);
			stmt1 = conn
					.prepareStatement("select * from car_newmdt where car_no=?");
			stmt = conn
					.prepareStatement("insert into car_newmdt (id,car_no,parent_id,channels) values(seq_newmdt.nextval,?,?,?)");
			for (int i = 0; i < list.size(); i++) {
				info = (CarInfo) list.get(i);
				stmt1.setString(1, info.getCarno());
				ResultSet setss = stmt1.executeQuery();
				if(!setss.next()){
				stmt.setString(1, info.getCarno());
				stmt.setString(2, info.getParentid());
				stmt.setString(3, info.getChannels());
				stmt.executeUpdate();
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}
	}

	/**
	 * 获取所有新终端的机构以及父机构
	 * 
	 * @return
	 * @throws Exception
	 */
	public void getNewMdtOrg() throws Exception {
		List<String> new_org = new ArrayList<String>();
		DbHandle conn = DbServer.getSingleInstance().getConnWithUseTime(0);
		StatementHandle stmt = null;
		StatementHandle stmt1 = null;
		try {
			stmt = conn
					.prepareStatement("select distinct parent_id from car_newmdt");
			ResultSet sets = stmt.executeQuery();
			while (sets.next()) {
				String parent_id = sets.getString("parent_id");
				new_org.add(parent_id);
			}
			Org org = new Org();
			String org_id;
			String org_name = null;
			String parent_id = null;
			OrgInfo info = null;
			List list1 = RedisConnPoolServer.getSingleInstance()
					.queryTableRecord(new RedisTableBean[] { org });
			if (list1 != null && list1.size() > 0) {
				for (int i = 0; i < list1.size(); i++) {
					org = (Org) list1.get(i);
					org_id = org.getUid();
					org_name = org.getName();
					parent_id = org.getFid() + "";
					info = new OrgInfo(org_id, org_name, parent_id);
					ORG_INFO.put(org_id, info);
					System.out.println(new OrgInfo(org_id, org_name, parent_id)
							.toString());
				}
			}
			for (int i = 0; i < new_org.size(); i++) {
				OrgInfo info1 = ORG_INFO.get(new_org.get(i));
				System.out.println(info1.toString());
				if (info1 != null && info1.getParentid().equals("0")) {
					getRoot(info1);
				} else if (!info1.getParentid().equals("0")) {
					OrgInfo info2 = ORG_INFO.get(info1.getParentid());
					if (info2 != null && info2.getParentid().equals("0")) {
						getRoot(info1);
						getRoot(info2);
					} else if (!info2.getParentid().equals("0")) {
						OrgInfo info3 = ORG_INFO.get(info2.getParentid());
						if (info3 != null && info3.getParentid().equals("0")) {
							getRoot(info1);
							getRoot(info2);
							getRoot(info3);
						}else if (!info3.getParentid().equals("0")) {
							OrgInfo info4 = ORG_INFO.get(info3.getParentid());
							if (info4 != null && info4.getParentid().equals("0")) {
								getRoot(info1);
								getRoot(info2);
								getRoot(info3);
								getRoot(info4);
							}
						}
					}
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}
	}

	public void getRoot(OrgInfo info) {
		StatementHandle stmt1 = null;
		DbHandle conn = DbServer.getSingleInstance().getConnWithUseTime(0);
		try {
			stmt1 = conn
					.prepareStatement("select * from org_newmdt where id=?");
			stmt1.setString(1, info.getId());
			ResultSet sets = stmt1.executeQuery();
			if (!sets.next()) {
				stmt1 = conn
						.prepareStatement("insert into org_newmdt (id,name,parent_id) values(?,?,?)");
				stmt1.setString(1, info.getId());
				stmt1.setString(2, info.getName());
				stmt1.setString(3, info.getParentid());
				stmt1.executeUpdate();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}
	}

	/*
	 * 判断深圳终端是新终端还是老终端，厂商编号为0,1,2为老终端，其他为新终端 return 0:默认为老终端，1：为新终端
	 */
	public static int comMdtType(int bigMdtId) {

		String newmdtid = "" + bigMdtId;
		int type = 0;
		try {
			if (newmdtid.length()==8) {
				type = 1;
			} else {
				type=0;
//				int factorycode = Integer.parseInt((newmdtid).substring(1,
//						(newmdtid.length() - 6)));
//				factorycode = factorycode / 2 - 1;
//				if (factorycode == 0 || factorycode == 1 || factorycode == 2) {
//					type = 0;
//				} else {
//					type = 1;
//				}
			}
			return type;
		} catch (Exception e) {
			System.out.println("判断是否为新终端异常：" + bigMdtId);
			e.printStackTrace();
			return type;
		}
	}

}
