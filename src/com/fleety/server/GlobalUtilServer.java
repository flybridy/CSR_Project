package com.fleety.server;

import java.io.File;
import java.sql.ResultSet;
import java.util.HashMap;

import server.db.DbServer;
import server.mail.MailServer;
import server.threadgroup.PoolInfo;
import server.threadgroup.ThreadPoolGroupServer;

import com.fleety.analysis.track.DestInfo;
import com.fleety.server.sms.SmsServer;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.fleety.util.pool.thread.ThreadPool;
import com.fleety.util.pool.timer.FleetyTimerTask;
import com.fleety.util.pool.timer.TimerPool;

public class GlobalUtilServer {

	private static HashMap<String, DestInfo> destInfoMap = new HashMap<String, DestInfo>();
	public static TimerPool globalTimerPool = null;
	public static ThreadPool globalThreadPool = null;
	static {
		try {
			globalTimerPool = ThreadPoolGroupServer.getSingleInstance()
					.createTimerPool("globalTimerPool");
			
			PoolInfo poolInfo = new PoolInfo(ThreadPool.SINGLE_TASK_LIST_POOL,
					20, 2000, true);
			globalThreadPool = ThreadPoolGroupServer.getSingleInstance()
					.createThreadPool("globalThreadPool", poolInfo);
			//定时加载车辆信息
			loadDestInfo();
			globalTimerPool.schedule(new FleetyTimerTask() {
				public void run() {
					// TODO Auto-generated method stub
					loadDestInfo();
				}
			}, 60000, 900000);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * 加载车辆信息
	 */
	private static void loadDestInfo() {
		HashMap<String, DestInfo> map = new HashMap<String, DestInfo>();
		DbHandle conn = DbServer.getSingleInstance().getConn();
		DestInfo dInfo;
		try {
			StatementHandle stmt = conn
					.prepareStatement("select mdt_id,dest_no,company_id,company_name,type_id from v_ana_dest_info where mdt_id>-1");
			ResultSet sets = stmt.executeQuery();
			while (sets.next()) {
				dInfo = new DestInfo();
				dInfo.mdtId = sets.getInt("mdt_id");
				dInfo.destNo = sets.getString("dest_no");
				dInfo.companyId = sets.getInt("company_id");
				dInfo.companyName = sets.getString("company_name");
				dInfo.carType = sets.getInt("type_id");
				map.put(dInfo.destNo, dInfo);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}
		synchronized (destInfoMap) {
			destInfoMap = map;
		}
	}

	public static DestInfo getDestInfo(String carNo) {
		synchronized (destInfoMap) {
			return destInfoMap.get(carNo);
		}
	}
	public static DestInfo[] getAllDestInfoArr(){
		synchronized (destInfoMap) {
			DestInfo[] result=new DestInfo[destInfoMap.size()];
			destInfoMap.values().toArray(result);
			return result;
		}
	}
	public static HashMap getDestInfoMapClone(){
		synchronized (destInfoMap) {			
			return (HashMap)destInfoMap.clone();
		}
	}
	
	public static boolean sendMails(String[] receiverArr,String head,String content,File[] attachArr){
		if(!MailServer.getSingleInstance().isRunning()){
			return false;
		}else{
			if(attachArr==null){
				MailServer.getSingleInstance().sendMail(receiverArr, head, content);
			}else{
				MailServer.getSingleInstance().sendMail(receiverArr, head, content, attachArr);
			}
			
			return true;
		}
	}
	public static boolean sendSmsMessage(String seq, String tel, String content){
		if(SmsServer.getSingleInstance().isRunning){
			return false;
		}else{
			return SmsServer.getSingleInstance().sendSms(seq, tel, content);
		}
	}

}
