package com.fleety.analysis.realtime;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import org.json.JSONArray;
import org.json.JSONObject;

import server.db.DbServer;

import com.fleety.analysis.RealTimeAnalysisServer;
import com.fleety.base.Util;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.fleety.util.pool.db.redis.BasicRedisObserver;
import com.fleety.util.pool.db.redis.IRedisObserver;
import com.fleety.util.pool.db.redis.RedisConnPoolServer;
import com.fleety.util.pool.db.redis.RedisTableBean;
import com.fleety.util.pool.thread.BasicTask;

public class AccOnWithNoLoginAnalysisServer extends RealTimeAnalysisServer{
	private IRedisObserver observer = null;
	private String notifyInfo = null;
	private long limitDuration = 600000;
	private long execDuration = 1800000;
	
	public boolean startServer() {
		this.isRunning = super.startServer();
		if(!this.isRunning){
			return false;
		}
		
		this.notifyInfo = this.getStringPara("notify_info");
		if(this.notifyInfo != null && this.notifyInfo.trim().length() == 0){
			this.notifyInfo = null;
		}
		
		String tempStr = this.getStringPara("limit_duration");
		if(tempStr != null && tempStr.trim().length() > 0){
			this.limitDuration = Integer.parseInt(tempStr.trim())*60000l;
		}
		tempStr = this.getStringPara("exec_duration");
		if(tempStr != null && tempStr.trim().length() > 0){
			this.execDuration = Integer.parseInt(tempStr.trim())*60000l;
		}
		
		ArrayList patternList = new ArrayList(2);
		patternList.add("D_REALTIME_VEHICLE_INFO_CHANNEL_*");
		this.observer = new BasicRedisObserver(patternList){
			public void msgArrived(String pattern,String msg,String content){
				if(content != null){
					AccOnWithNoLoginAnalysisServer.this.addExecTask(new LocationAnalysis(content));
				}
			}
		};
		RedisConnPoolServer.getSingleInstance().addListener(this.observer);
		
		return this.isRunning();
	}

	public void stopServer(){
		RedisConnPoolServer.getSingleInstance().removeListener(this.observer);
		super.stopServer();
	}
	
	private HashMap destMapping = new HashMap();
	private void updateAccON(String taxiNo,long gpsTime) throws Exception{
		DestInfo dInfo = null;
		synchronized(this.destMapping){
			dInfo = (DestInfo)this.destMapping.get(taxiNo);
			if(dInfo == null){
				dInfo = new DestInfo();
				this.destMapping.put(taxiNo, dInfo);
				dInfo.startTime = gpsTime;
			}
			
			//相邻两个位置汇报超过5分钟，则抛弃之前的信息，重新开始计时
			if(gpsTime - dInfo.endTime > 300000){
				dInfo.startTime = gpsTime;
			}
			dInfo.endTime = gpsTime;
		}
		
		//如果已经超出登录时间
		if(!dInfo.isOverLimitDuration()){
			return ;
		}
		
		DestLoginStatusInfoBean pos = new DestLoginStatusInfoBean();
		pos.setUid(taxiNo);
		//判断是否已经登录
		if(RedisConnPoolServer.getSingleInstance().queryTableRecord(new RedisTableBean[]{pos}).size() == 1){
			if(pos.getActionFlag() == 1){
				synchronized(this.destMapping){
					this.destMapping.remove(taxiNo);
				}
			}else{
				if(dInfo.startTime < pos.getDriverLogoutTime().getTime()){
					dInfo.startTime = pos.getDriverLogoutTime().getTime();
				}
				if(dInfo.isOverLimitDuration()){
					//Dispatch Event
					System.out.println("Trigger AccNoLogin Alarm:taxiNo="+taxiNo);
					AccOnWithNoLoginAnalysisServer.this.addExecTask(new SaveTask(taxiNo,dInfo));
				}
			}
		}else{
			//暂时不支持该逻辑判断
		}
	}
	private void updateAccOff(String taxiNo){
		synchronized(destMapping){
			this.destMapping.remove(taxiNo);
		}
	}
	
	
	private class SaveTask extends BasicTask{
		private String taxiNo = null;
		private DestInfo dInfo = null;
		public SaveTask(String taxiNo,DestInfo dInfo){
			this.taxiNo = taxiNo;
			this.dInfo = dInfo;
		}
		
		public boolean execute() throws Exception{
			if(!dInfo.isNeedExecute()){
				return true;
			}
			dInfo.lastExecTime = System.currentTimeMillis();
			Vehicle vehicle = new Vehicle();
			vehicle.setUid(this.taxiNo);
			if(RedisConnPoolServer.getSingleInstance().queryTableRecord(new RedisTableBean[]{vehicle}).size() == 0){
				System.out.println("Dest Not Exist! "+this.taxiNo);
				return true;
			}
			Org org = new Org();
			org.setUid(vehicle.getOid()+"");
			if(RedisConnPoolServer.getSingleInstance().queryTableRecord(new RedisTableBean[]{org}).size() == 0){
				org = null;
			}else{
				org.setUid(org.getFid()+"");
				if(RedisConnPoolServer.getSingleInstance().queryTableRecord(new RedisTableBean[]{org}).size() == 0){
					org = null;
				}
			}
			
			if(notifyInfo != null){
				ByteBuffer buff = ByteBuffer.allocate(1024);
				buff.order(ByteOrder.LITTLE_ENDIAN);
				buff.put((byte)0x06);
				buff.putShort((short)0); //len
				buff.putShort((short)(vehicle.getMdtId()&0xFFFF)); //mdtId
				buff.put(notifyInfo.getBytes("GBK"));
				buff.put((byte)0);
				
				buff.putShort(1, (short)(buff.position()-3));
				buff.flip();
				
				JSONObject tjson = new JSONObject();
				tjson.put("len", new Integer(1));
				JSONArray arr = new JSONArray();
				tjson.put("val", arr);
				JSONObject infoJson = new JSONObject();
				arr.put(infoJson);
				infoJson.put("mdtId", vehicle.getMdtId());
				infoJson.put("bcdStr", Util.byteArr2BcdStr(buff.array(), 0, buff.limit()));
				
				RedisConnPoolServer.getSingleInstance().publish("GATEWAY_BCD_DATA_CHANNEL", tjson.toString());
			}
			DbHandle conn = DbServer.getSingleInstance().getConn();
			try{
				StatementHandle stmt = conn.prepareStatement("insert into T_MESSAGE_SEND_RECORD(id,plate_no,company_id,company_name,mdt_id,msg_content,msg_type,send_time) values(?,?,?,?,?,?,?,?)");
				stmt.setInt(1, (int)DbServer.getSingleInstance().getAvaliableId(conn, "ANA_SINGLE_CAR_DAY_STAT", "id"));
				stmt.setString(2, this.taxiNo);
				stmt.setInt(3, org == null?0:Integer.parseInt(org.getUid()));
				stmt.setString(4, org.getName());
				stmt.setInt(5, vehicle.getMdtId());
				stmt.setString(6, notifyInfo);
				stmt.setInt(7, 3);
				stmt.setTimestamp(8, new java.sql.Timestamp(new Date().getTime()));
				stmt.execute();
			}finally{
				DbServer.getSingleInstance().releaseConn(conn);
			}
			return true;
		}
	}
	
	private class LocationAnalysis extends BasicTask{
		private String jsonLocation = null;
		public LocationAnalysis(String jsonLocation){
			this.jsonLocation = jsonLocation;
		}
		
		public boolean execute() throws Exception{
			JSONObject info = new JSONObject(this.jsonLocation);
			
			String taxiNo = info.getString("uid");
			int acc = info.getInt("acc");
			long gpsTime = info.getLong("dt");
			
			if(acc == 1){
				updateAccON(taxiNo,gpsTime);
			}else{
				updateAccOff(taxiNo);
			}
			
			return true;
		}
		
		public Object getFlag(){
			return "level-11";
		}
	}
	
	private class DestInfo{
		public long lastExecTime = 0;
		public long startTime = 0;
		public long endTime = 0;
		
		public boolean isOverLimitDuration(){
			return this.endTime - this.startTime >= limitDuration;
		}
		
		public boolean isNeedExecute(){
			return System.currentTimeMillis()-this.lastExecTime >= execDuration;
		}
	}
}
