package com.fleety.analysis.operation.task;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;

import org.json.JSONArray;
import org.json.JSONObject;

import server.db.DbServer;
import server.var.VarManageServer;

import com.fleety.analysis.AnalysisServer;
import com.fleety.analysis.track.ITrackAnalysis;
import com.fleety.analysis.track.TrackInfo;
import com.fleety.base.InfoContainer;
import com.fleety.base.Util;
import com.fleety.track.TrackIO;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.fleety.util.pool.db.redis.RedisConnPoolServer;
import com.fleety.util.pool.db.redis.RedisConnPoolServer.JedisHandle;

public class ActuralVehicleWorkDurationTimeAnalysis implements ITrackAnalysis {

	private HashMap vehicleMapping = null;
	private HashMap companyMapping = new HashMap<Integer, ComInfo>();
	private int duration = 60 * 60 * 1000;
	private int maxWorkTime;
	private int speed;
	private String msgContent;

	public boolean startAnalysisTrack(AnalysisServer parentServer, InfoContainer statInfo) {
		maxWorkTime = Integer.parseInt(VarManageServer.getSingleInstance().getVarStringValue("max_work_time"));
		speed = Integer.parseInt(VarManageServer.getSingleInstance().getVarStringValue("speed"));
		msgContent = VarManageServer.getSingleInstance().getVarStringValue("over_time_driver_alert");

		if(vehicleMapping == null){
			vehicleMapping = new HashMap<String, ActuralVehicleInfo>();
		}
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			StatementHandle stmt = conn.createStatement();
			StringBuilder sb = new StringBuilder();
			sb.append("select car_id,term_id,term_name from (select car.car_id,car.term_id,term.term_name from car left join term on car.term_id=term.term_id)");
			ResultSet rs = stmt.executeQuery(sb.toString());
			while (rs.next()) {
				ComInfo comInfo = new ComInfo();
				comInfo.companyId = rs.getInt("term_id");
				comInfo.companyName = rs.getString("term_name");
				String palteNo = rs.getString("car_id");
				companyMapping.put(palteNo, comInfo);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}
		return true;
	}

	public void analysisDestTrack(AnalysisServer parentServer, TrackInfo trackInfo) {
		if (trackInfo.trackArr == null || trackInfo.trackArr.length == 0) {
			return;
		}

		Calendar time = Calendar.getInstance();
		Calendar currentTime = Calendar.getInstance();
		String plateNo = trackInfo.dInfo.destNo;
		int mdtId = trackInfo.dInfo.mdtId;
		long startTime = 0, endTime = 0;
		boolean first = true;
		// 从当前时间往前推5个小时，当有一个点的速度小于5，就不是工作状态
		for (int i = 0; i < trackInfo.trackArr.length; i++) {
			time.setTime(trackInfo.trackArr[i].getDate(TrackIO.DEST_TIME_FLAG));
			int currentSpeed = trackInfo.trackArr[i].getInteger(TrackIO.DEST_SPEED_FLAG).intValue();
			long interval = (currentTime.getTimeInMillis() - time.getTimeInMillis()) / duration;
			if (interval < (maxWorkTime + 1)) {
				if (currentSpeed <= speed){
					vehicleMapping.remove(plateNo);
					break;
				}
				else {
					if (first) {
						startTime = time.getTimeInMillis();
						first = false;
					}
					endTime = time.getTimeInMillis();
				}
			} else
				continue;
		}
		if (plateNo != null) {
			float ret = (float) (Math.round(((endTime - startTime) / (float) duration) * 100)) / 100;
			if (ret >= maxWorkTime) {
				ActuralVehicleInfo acInfo = new ActuralVehicleInfo();
				acInfo.startTime = new Date(startTime);
				acInfo.workDuration = ret;
				
				if(vehicleMapping.containsKey(plateNo)){
					acInfo = (ActuralVehicleInfo)vehicleMapping.get(plateNo);
					acInfo.workDuration = acInfo.workDuration  + parentServer.getIntegerPara("circle_time").intValue()/60f;
				}
				
				acInfo.analysisTime = currentTime.getTime();
				
				acInfo.endTime = new Date(endTime);
				
				acInfo.plateNo = plateNo;
				acInfo.mdtId = mdtId;
				if (companyMapping != null) {
					ComInfo comInfo = (ComInfo) (companyMapping.containsKey(plateNo) ? companyMapping.get(plateNo) : null);
					if (comInfo != null) {
						acInfo.companyId = comInfo.companyId;
						acInfo.companyName = comInfo.companyName;

					}
				}
				vehicleMapping.put(plateNo, acInfo);
			}
		}
	}

	public void endAnalysisTrack(AnalysisServer parentServer, InfoContainer statInfo) {
		if (this.vehicleMapping == null) {
			return;
		}

		RedisConnPoolServer server = RedisConnPoolServer.getSingleInstance();
		JedisHandle conn = server.getJedisConnection();
		int num = 0;
		try {
			// delete data from redis
			ActuralWorkDurationTimeBean bean = new ActuralWorkDurationTimeBean();
			if (conn != null) {
				conn.select(0);
				server.clearTableRecord(bean);
			}

			String plateNo = "";
			ActuralVehicleInfo acturalVehicleInfo;
			ActuralWorkDurationTimeBean[] stopTimeBean = new ActuralWorkDurationTimeBean[vehicleMapping.size()];
			for (Iterator itr = this.vehicleMapping.keySet().iterator(); itr.hasNext();) {
				plateNo = (String) itr.next();
				acturalVehicleInfo = (ActuralVehicleInfo) this.vehicleMapping.get(plateNo);
				ActuralWorkDurationTimeBean stopObj = new ActuralWorkDurationTimeBean();
				stopObj.setUid(acturalVehicleInfo.plateNo);
				stopObj.setCompanyId(acturalVehicleInfo.companyId);
				stopObj.setCompanyName(acturalVehicleInfo.companyName);
				stopObj.setWorkDuration(acturalVehicleInfo.workDuration);
				stopObj.setAnalysisTime(acturalVehicleInfo.analysisTime);
				stopObj.setStartTime(acturalVehicleInfo.startTime);
				stopObj.setEndTime(acturalVehicleInfo.endTime);
				stopTimeBean[num] = stopObj;
				num++;
			}

			//System.out.println("Finish vehicle actural work duration time data Analysis:" + this.toString() + " recordNum=" + num);
			if (conn != null) {
				conn.select(0);
				server.saveTableRecord(stopTimeBean);
			}
			SaveSendMsgInfo();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			server.releaseJedisConnection(conn);
		}
	}

	private void SaveSendMsgInfo() {
		if (this.vehicleMapping == null) {
			return;
		}

		String plateNo = "";
		ActuralVehicleInfo acturalVehicleInfo;
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			conn.setAutoCommit(false);
			// inset into databases
			StatementHandle stmt = conn.prepareStatement("insert into T_MESSAGE_SEND_RECORD(id,plate_no,company_id,company_name,mdt_id,msg_content,msg_type,send_time) values(?,?,?,?,?,?,?,?)");
			for (Iterator itr = this.vehicleMapping.keySet().iterator(); itr.hasNext();) {
				plateNo = (String) itr.next();
				acturalVehicleInfo = (ActuralVehicleInfo) this.vehicleMapping.get(plateNo);
				if(System.currentTimeMillis() - acturalVehicleInfo.lastExecTime < 30 * 60 * 1000){
					continue;
				}
				acturalVehicleInfo.lastExecTime = System.currentTimeMillis();
				
				stmt.setInt(1, (int) DbServer.getSingleInstance().getAvaliableId(conn, "ANA_SINGLE_CAR_DAY_STAT", "id"));
				stmt.setString(2, acturalVehicleInfo.plateNo);
				stmt.setInt(3, acturalVehicleInfo.companyId);
				stmt.setString(4, acturalVehicleInfo.companyName);
				stmt.setInt(5, acturalVehicleInfo.mdtId);
				stmt.setString(6, msgContent);
				stmt.setInt(7, 2);
				stmt.setTimestamp(8, new Timestamp(new Date().getTime()));
				stmt.addBatch();
				sendMsgToMdt(plateNo,acturalVehicleInfo.mdtId);
			}
			stmt.executeBatch();
			conn.commit();

		} catch (Exception e) {
			e.printStackTrace();
			if (conn != null) {
				try {
					conn.rollback();
				} catch (Exception ee) {
					ee.printStackTrace();
				}
			}
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}
	}

	private void sendMsgToMdt(String plateNo,int mdtId) { 
		try {
			ByteBuffer buff = ByteBuffer.allocate(1024);
			buff.order(ByteOrder.LITTLE_ENDIAN);
			buff.put((byte) 0x06);
			buff.putShort((short) 0); // len
			buff.putShort((short) 0); // mdtId
			buff.put(msgContent.getBytes("GBK"));
			buff.put((byte) 0);
			buff.putShort(1, (short) (buff.position() - 3));
			buff.flip();
			buff.putShort(4, (short) (mdtId & 0xffff));
			JSONObject tjson = new JSONObject();
			tjson.put("len", new Integer(1));
			JSONArray arr = new JSONArray();
			tjson.put("val", arr);
			JSONObject infoJson = new JSONObject();
			arr.put(infoJson);
			infoJson.put("mdtId", mdtId);
			infoJson.put("bcdStr", Util.byteArr2BcdStr(buff.array(), 0, buff.limit()));
			//System.out.println("MessageDispatch:channel=GATEWAY_BCD_DATA_CHANNEL" + " str=" + tjson.toString());
			RedisConnPoolServer.getSingleInstance().publish("GATEWAY_BCD_DATA_CHANNEL", tjson.toString());

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private class ActuralVehicleInfo {
		public String plateNo;
		public int companyId;
		public int mdtId;
		public String companyName;
		public float workDuration;
		public Date analysisTime;
		public Date startTime;
		public Date endTime;
		public long lastExecTime = 0;
	}

	private class ComInfo {
		public int companyId;
		public String companyName;
	}
}
