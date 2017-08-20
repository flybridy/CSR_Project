package com.fleety.server;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import server.db.DbServer;
import server.threadgroup.ThreadPoolGroupServer;

import com.fleety.base.StrFilter;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.fleety.util.pool.timer.FleetyTimerTask;

public class UserInfoSyncServer extends BasicServer {
	private long syncCycle = 3600000;
	private DbServer nbDbServer = null;

	public boolean startServer() {
		String tempStr = null;
		try {
			tempStr = this.getStringPara("collect");

			tempStr = this.getStringPara("sync_cycle");
			if (StrFilter.hasValue(tempStr)) {
				try {
					syncCycle = Integer.parseInt(tempStr) * 60 * 1000;
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			}

			nbDbServer = new DbServer();
			nbDbServer.addPara("driver", this.getStringPara("source_driver"));
			nbDbServer.addPara("url", this.getStringPara("source_url"));
			nbDbServer.addPara("user", this.getStringPara("source_user"));
			nbDbServer.addPara("pwd", this.getStringPara("source_pwd"));
			nbDbServer.addPara("init_num", this.getStringPara("source_init_num"));
			nbDbServer.addPara("heart_sql", this.getStringPara("source_heart_sql"));
			nbDbServer.addPara("enable_stack", "true");
			nbDbServer.startServer();

			long delay = 60000;
			ThreadPoolGroupServer.getSingleInstance().createTimerPool("user_data_sync_timer", 1).schedule(new FleetyTimerTask() {
				
				public void run() {
					syncUserInfo();
					
				}
			}, delay, syncCycle);

			this.isRunning = true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

		return this.isRunning();
	}

	public void stopServer() {
		this.nbDbServer.stopServer();
		super.stopServer();
	}

	private void syncUserInfo() {
		System.out.println("-------��ʼͬ���������⳵����ƽ̨�û���Ϣ----------");
		DbHandle conn = DbServer.getSingleInstance().getConn();
		DbHandle nbConn = this.nbDbServer.getConn();
		HashMap userMap = new HashMap();

		try {
			StatementHandle stmt = conn.createStatement();
			StatementHandle stmt1 = conn.createStatement();
			conn.setAutoCommit(false);
			ResultSet sets = null;

			sets = stmt.executeQuery("select account,user_name,password from mtaxi_user_info");

			while (sets.next()) {
				userMap.put(sets.getString("account"), sets.getString("account"));
			}

			StatementHandle nbStmt = nbConn.createStatement();
			ResultSet nbSets = null;

			StringBuffer nbStr = new StringBuffer();
			nbStr.append("select usercode,username,userpwd from v_userinfo");

			nbSets = nbStmt.executeQuery(nbStr.toString());
			String insertSql = "", updateSql = "";

			while (nbSets.next()) {
				String account = nbSets.getString("usercode");
				String userName = nbSets.getString("usercode");
				String pwd = nbSets.getString("userpwd");
				if (!userMap.containsKey(account)) {
					int id = (int) DbServer.getSingleInstance().getAvaliableId(stmt, "mtaxi_user_info", "id");
					insertSql = "insert into mtaxi_user_info(id,account,user_name,password) values (" + id + ",'" + account + "','" + userName + "','" + pwd + "')";
					stmt.addBatch(insertSql);
				} else {
					updateSql = "update mtaxi_user_info set user_name='" + userName + "',password='" + pwd + "' where account='" + account + "'";
					stmt1.addBatch(updateSql);
				}
			}

			stmt.executeBatch();
			stmt1.executeBatch();
			conn.commit();

		} catch (Exception e) {
			try {
				conn.rollback();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			e.printStackTrace();
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
			this.nbDbServer.releaseConn(nbConn);
			System.out.println("-------ͬ���������⳵����ƽ̨�û���Ϣ����----------");
		}
	}
}
