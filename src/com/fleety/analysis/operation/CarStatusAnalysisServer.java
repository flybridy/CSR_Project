package com.fleety.analysis.operation;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import server.db.DbServer;

import com.fleety.analysis.AnalysisServer;
import com.fleety.analysis.operation.task.DriverLoginOutBean;
import com.fleety.base.GeneralConst;
import com.fleety.common.redis.Gps_Pos;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.fleety.util.pool.db.redis.RedisConnPoolServer;
import com.fleety.util.pool.db.redis.RedisTableBean;
import com.fleety.util.pool.thread.BasicTask;
import com.fleety.util.pool.timer.FleetyTimerTask;

public class CarStatusAnalysisServer extends AnalysisServer {
	
	private int offLineInterval = 15;
	private int faultLineInterval = 24*60;
	private int duration = 60 * 1000;
	private TimerTask task = null;
	public int circleTime = 10;
	public static CarStatusAnalysisServer instance = null;
	
	public CarStatusAnalysisServer (){}
	public static CarStatusAnalysisServer getSingleInstance(){
		if(instance == null){
			instance = new CarStatusAnalysisServer();
		}
		return instance;
	}
	
	public boolean startServer() {

		System.out.println("1111111111");
		super.startServer();
		if(!this.isRunning()){
			return this.isRunning();
		}		
		circleTime = this.getIntegerPara("circle_time").intValue();
		long period = circleTime * 60 * 1000;
		this.isRunning = this.scheduleTask(this.task = new TimerTask(), 5000, period);
		
		return this.isRunning();
	}

	public void stopServer(){
		if(this.task != null){
			this.task.cancel();
		}
		super.stopServer();
	}
	
	private void executeTask(Calendar anaDate) throws Exception{
		System.out.println("begin task");
		HashMap<String, Integer> carTeamMap = new HashMap<String, Integer>();
		HashMap<Integer, CarStateInfo> carStateMap = new HashMap<Integer, CarStateInfo>();
		Date curDate = new Date();
		long currentTime = curDate.getTime();
		
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try{
			String insertSql = "insert into ana_vehicle_state "
					+ " (id, team_id, k_num, z_num, gz_num, online_num, offline_num, stat_time, car_num) "
					+ " values(?,?,?,?,?,?,?,sysdate,?)";
			StatementHandle stmt1 = conn.prepareStatement(insertSql);
			StatementHandle stmt = conn.prepareStatement("select car_id,team_id,term_id from car");
			ResultSet sets = stmt.executeQuery();
			while(sets.next()) {
				String car_id = sets.getString("car_id");
				int team_id = sets.getInt("team_id");
				carTeamMap.put(car_id, team_id);
			}
		
			
		Gps_Pos bean = new Gps_Pos();
		List gpsPosList = RedisConnPoolServer.getSingleInstance().queryTableRecord(new RedisTableBean[]{bean});
		int team_id = 0;
//		for (Gps_Pos gps : gpsPosList) {
		if(gpsPosList!=null&&gpsPosList.size()>0){
			for(int i=0;i<gpsPosList.size();i++){
				bean = (Gps_Pos)gpsPosList.get(i);
				int state = bean.getState();
				Date gpsreDate = (bean.getSysDate() == null ? bean.getDt() : bean.getSysDate());
				long gpsTime = gpsreDate.getTime();
				long interval = Math.abs(currentTime - gpsTime);
				String carId = bean.getUid();
				System.out.println("���ƺ�%%%%"+carId);
				if(carTeamMap.containsKey(carId)){
					 team_id = carTeamMap.get(carId);
				}
				System.out.println("����id%%%%"+team_id);
				if(carStateMap.containsKey(team_id)){
					if (interval > faultLineInterval * duration) {// ����
						System.out.println("����:"+team_id+" ������+1");
						carStateMap.get(team_id).setGuzhangNum(carStateMap.get(team_id).getGuzhangNum() + 1);
					} else if (interval > offLineInterval * duration) {// ����
						System.out.println("����:"+team_id+" ������+1");
						carStateMap.get(team_id).setOfflineNum(carStateMap.get(team_id).getOfflineNum() + 1);
					} else {// ����
						System.out.println("����:"+team_id+" ������+1");
						carStateMap.get(team_id).setOnlineNum(carStateMap.get(team_id).getOnlineNum() + 1);
					}
					if (state == 0){//�ճ�
						System.out.println("����:"+team_id+" �ճ���+1");
						carStateMap.get(team_id).setKnum(carStateMap.get(team_id).getKnum() + 1);
					} else if (state == 1){//�س�
						System.out.println("����:"+team_id+" �س���+1");
						carStateMap.get(team_id).setZnum(carStateMap.get(team_id).getZnum() + 1);
					}
					System.out.println("����:"+team_id+" �ܳ�����+1");
					carStateMap.get(team_id).setCarNum(carStateMap.get(team_id).getCarNum() + 1);
				}else{
					CarStateInfo ca = new CarStateInfo();
					ca.setTeamId(team_id);
					carStateMap.put(team_id, ca);
				}
			}
			
		}
		
		int id = 0;
		Iterator iter = carStateMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Entry) iter.next();
			int teamId = (Integer) entry.getKey();
			CarStateInfo info = (CarStateInfo) entry.getValue();
			int kNum = info.getKnum();
			int zNum = info.getZnum();
			int gzNum = info.getGuzhangNum();
			int onlineNum = info.getOnlineNum();
			int offlineNum = info.getOfflineNum();
			int carNum = info.getCarNum();
			System.out.println("����id:" + teamId + "������:"+ gzNum + "�ճ���:" + kNum + "�س���:" + zNum + "������:" + offlineNum + "������:" + onlineNum);
			id = (int) DbServer.getSingleInstance().getAvaliableId(conn,
					"ana_vehicle_state", "id");
			//id, team_id, k_num, z_num, gz_num, online_num, offline_num, car_num
			stmt1.setInt(1, id);
			stmt1.setInt(2, teamId);
			stmt1.setInt(3, kNum);
			stmt1.setInt(4, zNum);
			stmt1.setInt(5, gzNum);
			stmt1.setInt(6, onlineNum);
			stmt1.setInt(7, offlineNum);
			stmt1.setInt(8, carNum);
			stmt1.addBatch();
		}
		stmt1.executeBatch();
		conn.commit();
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}
	}

	private class TimerTask extends FleetyTimerTask{
		public void run(){
			Calendar cal = Calendar.getInstance();			
			System.out.println("Fire ExecTask CarStatusAnalysisServer:"+GeneralConst.YYYY_MM_DD_HH.format(cal.getTime()));
			CarStatusAnalysisServer.this.addExecTask(new ExecTask(cal));
		}
	}
	private class ExecTask extends BasicTask{
		private Calendar anaDate = null;
		public ExecTask(Calendar anaDate){
			this.anaDate = anaDate;
		}
		
		public boolean execute() throws Exception{
			CarStatusAnalysisServer.this.executeTask(this.anaDate);
			return true;
		}
		
		public String getDesc(){
			return "ʵʱ���ݷ���";
		}
		public Object getFlag(){
			return "CarStatusAnalysisServer";
		}
	}
}

