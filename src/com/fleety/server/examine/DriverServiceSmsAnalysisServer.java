package com.fleety.server.examine;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;

import server.db.DbServer;
import server.threadgroup.PoolInfo;
import server.threadgroup.ThreadPoolGroupServer;

import com.fleety.base.GeneralConst;
import com.fleety.db.SqlDbServer;
import com.fleety.server.BasicServer;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.fleety.util.pool.thread.ThreadPool;
import com.fleety.util.pool.timer.FleetyTimerTask;
import com.fleety.util.pool.timer.TimerPool;
import com.sun.org.apache.xpath.internal.operations.And;

public class DriverServiceSmsAnalysisServer extends BasicServer {
	private TimerTask task = null;
	private TimerPool timer = null;
	private long time = 10 * 1000l;
	private int reply_time = 10*60*1000;//�ظ���Чʱ��
	private int preTime = 30;
	private HashMap<Integer, DriverSmsExamine> smsMapSend = null;//���������ѡ����
	private HashMap<Integer, DriverSmsExamine> smsMapSave = null;//���������ѡ����
	private HashMap<Integer, TblSmsRecvtmp> smsMapReply = null;//���������ѡ����
	public boolean isRun = false;
	
	private static DriverServiceSmsAnalysisServer singleInstance = null;

	public static DriverServiceSmsAnalysisServer getSingleInstance() {
		if (singleInstance == null) {
			synchronized (DriverServiceSmsAnalysisServer.class) {
				if (singleInstance == null) {
					singleInstance = new DriverServiceSmsAnalysisServer();
				}
			}
		}
		return singleInstance;
	}
	
	private DriverServiceSmsAnalysisServer(){
		
	}
	
	@Override
	public boolean startServer() {
		
		
		String temp = this.getStringPara("time");
		if (temp != null && !temp.equals("")) {
			time = this.getIntegerPara("time").intValue()*1000l;
		}
		temp = this.getStringPara("dura");
		if (temp != null && !temp.equals("")) {
			preTime = this.getIntegerPara("dura").intValue();
		}
		temp = this.getStringPara("reply_time");
		if (temp != null && !temp.equals("")) {
			reply_time = this.getIntegerPara("reply_time").intValue()*60*1000;
		}
		
		Integer timerThreadNum = 1;
		this.timer = ThreadPoolGroupServer.getSingleInstance().createTimerPool(
				"OperatorServiceSmsExamineServer_Timer",
				timerThreadNum.intValue(), false);

		if (this.timer != null) {
			this.isRunning = true;
		}

		this.isRunning = this.scheduleTask(this.task = new TimerTask(), time,time);

		return true;
	}

	public void stopServer() {
		if (this.task != null) {
			this.task.cancel();
		}
		super.stopServer();
	}
	
	private class TimerTask extends FleetyTimerTask {

		public void run() {
			try {
				Calendar currentDate = Calendar.getInstance();
				querySMSSendInfo(currentDate);
				querySMSReplyInfo(currentDate);
				if(!(smsMapReply!=null&&smsMapReply.size()>0&&
						smsMapSend!=null&&smsMapSend.size()>0)){
					return;
				}
				Date dExamineDate = null;
				Date recvtmpDate = null;
				String smsContent = "";
				smsMapSave = new HashMap<Integer, DriverSmsExamine>();
				for (Iterator iterator = smsMapSend.keySet().iterator(); iterator
						.hasNext();) {
					int id = (Integer) iterator.next();
					DriverSmsExamine dExamine = smsMapSend.get(id);
					dExamineDate = dExamine.sendTime;
					for (Iterator iterator2 = smsMapReply.values().iterator(); iterator2
							.hasNext();) {
						TblSmsRecvtmp recvtmp = (TblSmsRecvtmp) iterator2.next();

						recvtmpDate = recvtmp.sms_recvtime;
						smsContent = recvtmp.sms_content;
						if(recvtmp.userMblphone.equals(dExamine.phone)
								&&recvtmpDate.getTime()-dExamineDate.getTime()<=reply_time
								&&recvtmpDate.getTime()-dExamineDate.getTime()>=0){
							dExamine.isReply = 1;
							dExamine.reply = smsContent.trim();
							dExamine.reply_time = recvtmpDate;
							smsMapSave.put(id, dExamine);
						}
					}
				}
				saveDriverSmsExamine(smsMapSave);
			} catch (Exception e) {
				e.printStackTrace();
			}
			
		}

	}
	private void saveDriverSmsExamine(HashMap<Integer, DriverSmsExamine> smsMapSave) throws Exception {
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			conn.setAutoCommit(false);
			int count = 1;
			StatementHandle stmt = conn.prepareStatement("update driver_sms_examine set is_reply=?,reply=?,reply_time=? where id=?");
			for (Iterator iterator = smsMapSave.keySet().iterator(); iterator
					.hasNext();) {
				int id = (Integer) iterator.next();
				DriverSmsExamine dExamine = smsMapSave.get(id);
				stmt.setInt(1, dExamine.isReply);
				stmt.setString(2, dExamine.reply);
				stmt.setTimestamp(3, new Timestamp(dExamine.reply_time.getTime()));
				stmt.setInt(4, id);
				stmt.addBatch();
				if(count%200==0){
					stmt.executeBatch();
				}
				count++;
			}
			stmt.executeBatch();
			conn.commit();
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
	}
	private void querySMSSendInfo(Calendar currentDate) throws Exception {
		smsMapSend = new HashMap<Integer, DriverSmsExamine>();
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(currentDate.getTimeInMillis());
		cal.add(Calendar.MINUTE, (0-preTime));
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			StatementHandle stmt = conn.createStatement();
			String sql = "select * from driver_sms_examine where 1=1 and is_reply=0 and send_time<to_date('"+
			GeneralConst.YYYY_MM_DD_HH_MM_SS.format(currentDate.getTimeInMillis())+"','yyyy-MM-dd hh24:mi:ss') "+
			" and send_time >= to_date('"+GeneralConst.YYYY_MM_DD_HH_MM_SS.format(cal.getTimeInMillis())+"','yyyy-MM-dd hh24:mi:ss') ";
			ResultSet sets = stmt.executeQuery(sql);
			DriverSmsExamine dExamine = null;
			while (sets.next()) {
				dExamine = new DriverSmsExamine();
				dExamine.id = sets.getInt("id");
				dExamine.dest_no = sets.getString("dest_no");
				dExamine.driver_id = sets.getString("driver_id");
				dExamine.phone = sets.getString("phone");
				dExamine.sendTime = GeneralConst.YYYY_MM_DD_HH_MM_SS.parse(sets.getString("send_time"));
				dExamine.sms_content = sets.getString("sms_content");
				smsMapSend.put(dExamine.id, dExamine);
			}
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
	}
	private void querySMSReplyInfo(Calendar currentDate) throws Exception {
		smsMapReply = new HashMap<Integer, TblSmsRecvtmp>();
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(currentDate.getTimeInMillis());
		cal.add(Calendar.MINUTE, (0-preTime));
		DbHandle conn = SqlDbServer.getSingleInstance().getConn();
		try {
			StatementHandle stmt = conn.createStatement();
			String sql = "select * from TBL_SMS_RECVTMP where 1=1 and SMS_RECVTIME<CONVERT(DATETIME,'"+
			GeneralConst.YYYY_MM_DD_HH_MM_SS.format(currentDate.getTimeInMillis())+"',120) "+
			" and SMS_RECVTIME >= CONVERT(DATETIME,'"+GeneralConst.YYYY_MM_DD_HH_MM_SS.format(cal.getTimeInMillis())+"',120) ";
			ResultSet sets = stmt.executeQuery(sql);
			TblSmsRecvtmp tRecvtmp = null;
			while (sets.next()) {
				tRecvtmp = new TblSmsRecvtmp();
				tRecvtmp.id = sets.getInt("recv_id");
				tRecvtmp.userMblphone = sets.getString("user_mblphone");

				tRecvtmp.sms_recvtime = GeneralConst.YYYY_MM_DD_HH_MM_SS.parse(sets.getString("sms_recvtime"));
				tRecvtmp.sms_content = sets.getString("SMS_CONTENT");
				smsMapReply.put(tRecvtmp.id, tRecvtmp);
			}
		}finally{
			SqlDbServer.getSingleInstance().releaseConn(conn);
		}
	}
	
	/**
	 * ��ĳ��������붨ʱִ���У��������ִ��ʱ��ϳ���Ӧ�ò����µ�������õ�ִ��������н���ִ��
	 * 
	 * @param timerTask
	 *            ��������ִ�е�����
	 * @param delay
	 *            �ӳ�ִ��ʱ������λ����
	 * @param period
	 *            ִ�����ڣ���λ����
	 * @return
	 */
	public boolean scheduleTask(FleetyTimerTask timerTask, long delay,
			long period) {
		if (!this.isRunning()) {
			return false;
		}

		this.timer.schedule(timerTask, delay, period);
		return true;
	}
	public class DriverSmsExamine{
		public int id;
		public String phone;
		public String dest_no;
		public String driver_id;
		public String sms_content;
	    public Date sendTime;
	    public int isReply;
	    public String reply;
	    public Date reply_time;
	}
	public class TblSmsRecvtmp{
		public int id;
		public String userMblphone;
		public String sms_content;
		public Date sms_recvtime;
	}
}
