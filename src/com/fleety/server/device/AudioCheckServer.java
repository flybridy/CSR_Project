package com.fleety.server.device;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;

import server.db.DbServer;
import server.threadgroup.ThreadPoolGroupServer;

import com.fleety.base.GeneralConst;
import com.fleety.base.Util;
import com.fleety.common.redis.BusinessLastBusyBean;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.fleety.util.pool.db.redis.BasicRedisObserver;
import com.fleety.util.pool.db.redis.RedisConnPoolServer;
import com.fleety.util.pool.timer.FleetyTimerTask;
import com.fleety.util.pool.timer.TimerPool;

public class AudioCheckServer {

	private int batchSize = 1000;
	private int batchInterval = 5;
	private HashMap destMapping = new HashMap();
	private HashMap searchDestMapping = null;
	private HashMap uploadRequestMapping = new HashMap();
	private List uploadList = null;
	private TimerPool uploadTimer = null;
	private AudioIndexChannel listener = new AudioIndexChannel();
	
	private static AudioCheckServer singleInstance = null;
	public static AudioCheckServer getSingleInstance(){
		if(singleInstance == null){
			synchronized(AudioCheckServer.class){
				if(singleInstance == null){
					singleInstance = new AudioCheckServer();
				}
			}
		}
		return singleInstance;
	}
	
	public void startCheck(int batchSize,int batchInterval) throws Exception{
		this.batchSize = batchSize;
		this.batchInterval = batchInterval;
		this.uploadRequestMapping.clear();
		
		this.uploadTimer = ThreadPoolGroupServer.getSingleInstance().createTimerPool("audio_upload_timer", 1);
		this.uploadList = new LinkedList();
		this.searchDestMapping = this.loadCheckPoint();
		if(this.searchDestMapping == null){
			return ;
		}
		
		uploadTimer.schedule(new FleetyTimerTask(){
			public void run(){
				try{
					RedisConnPoolServer.getSingleInstance().addListener(listener);
					startScan();
				}catch(Exception e){
					e.printStackTrace();
				}
			}
		}, 0);
	}
	
	private HashMap loadCheckPoint() throws Exception{
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try{
			StatementHandle stmt = conn.prepareStatement("select mdt_id,car_id from car where car_id is not null and mdt_id>=10");
			ResultSet sets = stmt.executeQuery();
			while(sets.next()){
				destMapping.put(sets.getString("car_id"), sets.getString("mdt_id").substring(1));
			}
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
		
		BusinessLastBusyBean bean = new BusinessLastBusyBean();
		List dataList = RedisConnPoolServer.getSingleInstance().queryTableRecord(new BusinessLastBusyBean[]{bean});
		HashMap tempMapping = new HashMap();
		int mdtId;
		//过滤数据列表
		String dateFlag = GeneralConst.YYYYMMDD.format(new Date(System.currentTimeMillis()-GeneralConst.ONE_DAY_TIME));
		for(Iterator itr = dataList.iterator();itr.hasNext();){
			bean = (BusinessLastBusyBean)itr.next();
			if(!GeneralConst.YYYYMMDD.format(bean.getTrackDate()).equals(dateFlag)){
				continue;
			}
			if(!destMapping.containsKey(bean.getCarNo())){
				continue;
			}
			mdtId = new Integer(destMapping.get(bean.getCarNo()).toString());
			tempMapping.put(new Integer(mdtId), bean);
		}
		return tempMapping;
	}
	
	private void startScan() throws Exception{
		JSONObject tjson = new JSONObject();
		JSONArray arr = new JSONArray();
		tjson.put("val", arr);
		JSONObject oneCmd = null;

		BusinessLastBusyBean bean;
		ByteBuffer buffer = ByteBuffer.allocate(1024);
		buffer.order(ByteOrder.LITTLE_ENDIAN);
		int count =0;
		for (Iterator itr = this.searchDestMapping.values().iterator(); itr.hasNext();){
			count ++;
			bean = (BusinessLastBusyBean) itr.next();
			buffer.clear();
			oneCmd = this.buildAudioQueryCmd(bean,buffer);
			if (oneCmd != null) {
				arr.put(oneCmd);
			}
			
			if((count % this.batchSize) == 0){
				tjson.put("len", new Integer(arr.length()));

//				System.out.println("Message(QueryAudioIndex):channel=GATEWAY_BCD_DATA_CHANNEL" + " str=" + tjson.toString());
				RedisConnPoolServer.getSingleInstance().publish("GATEWAY_BCD_DATA_CHANNEL", tjson.toString());
				arr = new JSONArray();
				tjson.put("val", arr);
				try{
					Thread.sleep(2 * 60 * 1000);
				}catch(Exception e){}
			}
		}
		
		Thread.sleep(5 * 60 * 1000);
		RedisConnPoolServer.getSingleInstance().removeListener(listener);
		startUpload();
		
	}
	
	private JSONObject buildAudioQueryCmd(BusinessLastBusyBean bean,ByteBuffer buffer) throws Exception{
		String carNo = bean.getCarNo();
		String mdtIdStr = (String)destMapping.get(carNo);
		if(mdtIdStr == null || mdtIdStr.length() == 0){
			return null;
		}
		int mdtId = Integer.parseInt(mdtIdStr,10);
		JSONObject infoJson = new JSONObject();

		long st = bean.getLastBusyTime().getTime()-60000;
		long et = bean.getLastBusyTime().getTime()+60000;
		
		buffer.put((byte)0x1f);
		buffer.put((byte)0x61);
		buffer.putShort((short)0);
		buffer.putShort((short)(mdtId&0xffff));
		buffer.put((byte)0);
		buffer.put(Util.bcdStr2ByteArr(GeneralConst.YYMMDDHHMMSS.format(new Date(st))));
		buffer.put(Util.bcdStr2ByteArr(GeneralConst.YYMMDDHHMMSS.format(new Date(et))));
		buffer.putInt(0);
		buffer.putShort((short)0);
		buffer.put((byte)0);
		
		buffer.putShort(2,(short)(buffer.position()-4));
		
		infoJson.put("mdtId", mdtId);
		infoJson.put("bcdStr", Util.byteArr2BcdStr(buffer.array(), 0, buffer.position()));
		
		return infoJson;
	}
	
	private void startUpload(){
		AudioUploadInfo uInfo;
		int count = 0;
		for(Iterator itr = this.uploadList.iterator();itr.hasNext();){
			count ++;
			uInfo = (AudioUploadInfo)itr.next();
			uInfo.sendUploadCmd();
			
			if((count % this.batchSize) == 0){
				try{
					Thread.sleep(this.batchInterval * 60 * 1000);
				}catch(Exception e){}
			}
		}
		
		//延迟5分钟通知外部扫描数据上传结果
	}
	
	
	private class AudioIndexChannel extends BasicRedisObserver{
		int count =0;
		public AudioIndexChannel(){
			super(new ArrayList(1));
			this.getPatternList().add("MEDIA_DATA_INDEX_CHANNEL");
		}
		public void msgArrived(String pattern,String msg,String content){
			try{
				JSONObject info = new JSONObject(content);
				//1为音频
				if(info.getInt("type") == 1){
					uploadList.add(new AudioUploadInfo(info));
					
					System.out.println(++count+" , "+content);
				}
			}catch(Exception e){
				e.printStackTrace();
			}
		}
	}
	
	private class AudioUploadInfo{
		public int mdtId = 0;
		public JSONObject infoObj;
		
		public AudioUploadInfo(JSONObject infoObj) throws Exception{
			this.infoObj = infoObj;
			this.mdtId = this.infoObj.getInt("mdtId");
		}
		
		public void sendUploadCmd(){
			if(uploadRequestMapping.containsKey(new Integer(mdtId))){
				return ;
			}
			BusinessLastBusyBean bean;
			try{
				
				bean = (BusinessLastBusyBean)searchDestMapping.get(new Integer(mdtId));
				if(bean == null){
					return ;
				}
				
				long t = bean.getLastBusyTime().getTime();
				long st = t - 45000;
				long et = t + 15000;
				
				JSONObject obj;
				JSONArray arr = infoObj.getJSONArray("arr");
				boolean isExist = false;
				ByteBuffer buffer = ByteBuffer.allocate(1024);
				buffer.order(ByteOrder.LITTLE_ENDIAN);
				for(int i=0;i<arr.length();i++){
					obj = (JSONObject)arr.get(i);
					
					t = GeneralConst.YYMMDDHHMMSS.parse(obj.getString("time")).getTime();
					
					if(t>=st && t<=et){
						isExist = true;
						
						buffer.clear();
						buffer.put((byte)0x1f);
						buffer.put((byte)0x62);
						buffer.putShort((short)0);
						buffer.putShort((short)(mdtId&0xffff));
						
						buffer.put(Util.bcdStr2ByteArr(obj.getString("time")));
						buffer.put((byte)0);
						buffer.put((byte)0);
						
						buffer.putShort(2,(short)(buffer.position()-4));
						
						JSONObject tjson = new JSONObject(),infoJson = new JSONObject();
						JSONArray cmdArr = new JSONArray();
						cmdArr.put(infoJson);
						tjson.put("val", cmdArr);
						tjson.put("len", 1);
						
						infoJson.put("mdtId", mdtId);
						infoJson.put("bcdStr", Util.byteArr2BcdStr(buffer.array(), 0, buffer.position()));
						
//						System.out.println("Message(UploadAudio):channel=GATEWAY_BCD_DATA_CHANNEL"
//									+ " str=" + tjson.toString());
						RedisConnPoolServer.getSingleInstance().publish("GATEWAY_BCD_DATA_CHANNEL", tjson.toString());
					}
				}
				
				if(isExist){
					uploadRequestMapping.put(new Integer(mdtId), null);
				}
			}catch(Exception e){
				e.printStackTrace();
			}
		}
	}
}
