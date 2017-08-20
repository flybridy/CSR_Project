package com.fleety.analysis.operation;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;

import org.json.JSONArray;
import org.json.JSONObject;

import server.db.DbServer;

import com.fleety.analysis.AnalysisServer;
import com.fleety.base.GeneralConst;
import com.fleety.base.Util;
import com.fleety.server.device.AudioCheckServer;
import com.fleety.util.ZipTools;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.fleety.util.pool.db.redis.RedisConnPoolServer;
import com.fleety.util.pool.thread.BasicTask;
import com.fleety.util.pool.timer.FleetyTimerTask;

public class ScheduleTaskAnasisServer extends AnalysisServer {
	private TimerTask task = null;

	private int hour = 6;
	private int minute = 30;
	private int batchInterval = 5; // ���μ������λ����
	private int carNum = 500; // ÿ���γ�����;
	private int intervalTime = 24; // ���ڣ���λʱ
	private int delayTime =180; //�Ƴ�ִ����Ƭʱ�䣬��λ����

	private String fileSourcePath = "/home/fleety/iTop"; // "Դ�ļ��洢·��"
	private String fileDestPath = "/home/fleety/iTop/media"; // ѹ���ļ��洢·��
	private String fileCopyPath = "/home/fleety/iTop/media_copy";//�ļ�copy�洢·��
	private String mediaPath = "/media";
	private int fileDays = 7; // ѹ���ļ���������
	private long idFlag = 0;
	private long idFlag1 = 0;

	public boolean startServer() {
		super.startServer();
		if (!this.isRunning()) {
			return this.isRunning();
		}
		String temp = this.getStringPara("hour");
		if (temp != null && !"".equals(temp)) {
			this.hour = Integer.parseInt(temp);
		}

		temp = this.getStringPara("minute");
		if (temp != null && !"".equals(temp)) {
			this.minute = Integer.parseInt(temp);
		}

		temp = this.getStringPara("batch_interval");
		if (temp != null && !"".equals(temp)) {
			this.batchInterval = Integer.parseInt(temp);
		}

		temp = this.getStringPara("car_num");
		if (temp != null && !"".equals(temp)) {
			this.carNum = Integer.parseInt(temp);
		}

		temp = this.getStringPara("interval_time");
		if (temp != null && !"".equals(temp)) {
			this.intervalTime = Integer.parseInt(temp);
		}

		temp = this.getStringPara("file_days");
		if (temp != null && !"".equals(temp)) {
			this.fileDays = Integer.parseInt(temp);
		}
		
		temp = this.getStringPara("delay_time");
		if (temp != null && !"".equals(temp)) {
			this.delayTime = Integer.parseInt(temp);
		}

		temp = this.getStringPara("file_source_path");
		if (temp != null && !"".equals(temp)) {
			this.fileSourcePath = temp;
		}

		temp = this.getStringPara("file_dest_path");
		if (temp != null && !"".equals(temp)) {
			this.fileDestPath = temp;
		}
		
		temp = this.getStringPara("file_copy_path");
		if (temp != null && !"".equals(temp)) {
			this.fileCopyPath = temp;
		}
		
		Calendar cal = this.getNextExecCalendar(hour, minute);
		if (cal.get(Calendar.DAY_OF_MONTH) != Calendar.getInstance().get(Calendar.DAY_OF_MONTH)) {
			this.scheduleTask(new TimerTask(), 500);
		}
		long delay = cal.getTimeInMillis() - System.currentTimeMillis();
		this.isRunning = this.scheduleTask(this.task = new TimerTask(), delay,this.intervalTime * 60 * 60 * 1000);

		return this.isRunning();
	}

	public void stopServer() {
		if (this.task != null) {
			this.task.cancel();
		}
		super.stopServer();
	}

	private void executeTask(Calendar anaDate) throws Exception {

		if(!this.isExecute(anaDate)){
			return;
		}

		try {
			idFlag = this.getFlagId();
			
			AudioCheckServer.getSingleInstance().startCheck(carNum, batchInterval);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		new Timer().schedule(new TimerTask(){
			public void run(){
				idFlag1 = getFlagId();
				List<CarInfo> carList = loadCarInfo();
				photoCmdExecute(carList);
				zipFile();
				zipFilePhotoChoujian();
				removeZipFile();
				saveFailInfo();
			}
		}, this.delayTime * 60 * 1000);
		
	}
	
	//�ж��Ƿ�Ҫִ��
	private boolean isExecute(Calendar anaDate){
		boolean result = true;
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			StatementHandle stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("select * from media_zip_info where to_char(zip_date,'yyyyMMdd')='"+GeneralConst.YYYYMMDD.format(anaDate.getTime())+"'");
			if (rs.next()) {
				result = false;
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}
		return result;
	}
	
	private void photoCmdExecute(List<CarInfo> carList){
		List<CarInfo> failList = new ArrayList<CarInfo>();
		List<CarInfo> tempList = new ArrayList<CarInfo>();
		DbHandle conn = DbServer.getSingleInstance().getConnWithUseTime(0);
		try {
			if (carList != null && carList.size() > 0) {
				ByteBuffer buff = ByteBuffer.allocate(1024);
				buff.order(ByteOrder.LITTLE_ENDIAN);

				buff.put((byte) 0x0f);
				buff.put((byte) 0x1d);
				buff.putShort((short) 0); // len
				buff.putShort((short) 0); // mdtId
				buff.putInt(0); // seq
				buff.put((byte) 0); // camera
				buff.put((byte) 1); // 1��ʼ 0����
				buff.putShort((short) 0); // ������
				buff.putShort((short) 1); // ��������

				buff.putShort(2, (short) (buff.position()-4));
				buff.flip();
				int count = 0;
				for (int i = 0; i < carList.size(); i++) {
					int mdtId = carList.get(i).mdtId;
					tempList.add(carList.get(i));
					buff.putShort(4, (short) (mdtId & 0xffff));

					for(int j=1;j<=2;j++){
						buff.put(10, (byte) j);
						JSONObject tjson = new JSONObject();
						tjson.put("len", new Integer(1));
						JSONArray arr = new JSONArray();
						tjson.put("val", arr);
						JSONObject infoJson = new JSONObject();
						arr.put(infoJson);
						infoJson.put("mdtId", mdtId);
						infoJson.put("bcdStr", Util.byteArr2BcdStr(buff.array(), 0, buff.limit()));
						count++;
						System.out.println(count +","+carList.get(i).carNo+ " , PhotoDispatch:channel=GATEWAY_BCD_DATA_CHANNEL" + " str=" + tjson.toString());
						RedisConnPoolServer.getSingleInstance().publish("GATEWAY_BCD_DATA_CHANNEL", tjson.toString());
					}
					if((i+1) % this.carNum ==0){
						Thread.sleep(this.batchInterval * 60 * 1000);
						failList.addAll(this.checkOperate(tempList, 0));
						tempList.clear();
					}
				}
				
				if(tempList.size()>0){
					Thread.sleep(this.batchInterval * 60 * 1000);
					failList.addAll(this.checkOperate(tempList, 0));
					tempList.clear();
				}
				

				//------------�ٴ��·�û���ϴ��ĳ���-----------------------------
				for (int i = 0; i < failList.size(); i++) {
					int mdtId = failList.get(i).mdtId;
					buff.putShort(4, (short) (mdtId & 0xffff));
					for(int j=1;j<=2;j++){
						buff.put(10, (byte) j);
						JSONObject tjson = new JSONObject();
						tjson.put("len", new Integer(1));
						JSONArray arr = new JSONArray();
						tjson.put("val", arr);
						JSONObject infoJson = new JSONObject();
						arr.put(infoJson);
						infoJson.put("mdtId", mdtId);
						infoJson.put("bcdStr", Util.byteArr2BcdStr(buff.array(), 0, buff.limit()));

						System.out.println(carList.get(i).carNo+ " , PhotoDispatch:channel=GATEWAY_BCD_DATA_CHANNEL" + " str=" + tjson.toString());
						RedisConnPoolServer.getSingleInstance().publish("GATEWAY_BCD_DATA_CHANNEL", tjson.toString());
					}
				}
				
				if(failList.size()>0){
					Thread.sleep(this.batchInterval * 60 * 1000);
					this.checkOperate(failList, 0);
				}

			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}
	}
	
	private void saveFailInfo() {

		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			List<CarInfo> list = new ArrayList<CarInfo>();
			String querySql="select car_id, decode(source_id,0,'���ʱǰ��������ͷ��δ�ϴ�',1,'���ʱ��������ͷδ�ϴ�',2,'���ʱǰ������ͷδ�ϴ�') content from (select t1.car_id,decode(t2.source_id,null,0,t2.source_id) source_id from car t1 left join (select car_no,sum(source_id) source_id from (select car_no,source_id from media where media_type = 0 and id > "+ idFlag1 +" and event_type = 1 group by car_no,source_id ) group by car_no) t2 on t1.car_id = t2.car_no where t1.mdt_id > 10) where source_id<3 ";
			System.out.println(querySql);
			StatementHandle stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(querySql);
			CarInfo info = null;
			while (rs.next()) {
				info = new CarInfo();
				info.carNo = rs.getString("car_id");
				info.mediaType = 0;
				info.content = rs.getString("content");
				list.add(info);
			}
			rs.close();
			
			querySql="select car_id,'���ʱ¼��δ�ϴ�' content from (select t1.car_id,decode(t2.media_num,null,0,t2.media_num) media_num from car t1 left join (select car_no,count(car_no) media_num from media where media_type= 1 and id > "+idFlag+" and id < "+idFlag1+" group by car_no) t2 on t1.car_id = t2.car_no where t1.mdt_id > 10) where media_num=0";
			System.out.println(querySql);
			rs = stmt.executeQuery(querySql);
			while (rs.next()) {
				info = new CarInfo();
				info.carNo = rs.getString("car_id");
				info.mediaType = 1;
				info.content = rs.getString("content");
				list.add(info);
			}
			rs.close();
			
			String sql = "insert into MEDIA_CHECK_EXECUTE_RESULT(id,car_no,media_type,content,check_time) values(?,?,?,?,sysdate)";
			StatementHandle pstmt = conn.prepareStatement(sql);
			int count = 0;
			for(int i=0;i<list.size();i++){
				info = list.get(i);
				pstmt.setLong(1, DbServer.getSingleInstance().getAvaliableId(conn, "MEDIA_CHECK_EXECUTE_RESULT", "id"));
				pstmt.setString(2, info.carNo);
				pstmt.setInt(3, info.mediaType);
				pstmt.setString(4, info.content);
				pstmt.addBatch();
				count ++;
				if(i % 100 == 0){
					pstmt.executeBatch();
					count = 0;
				}
			}
			if(count > 0){
				pstmt.executeBatch();
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}
	}
	
	private void saveMediaZipInfo(int companyId,String path) {

		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			String sql = "insert into media_zip_info(id,company_id,file_path,zip_date) values(?,?,?,sysdate)";
			StatementHandle stmt = conn.prepareStatement(sql.toString());
			stmt.setLong(1, DbServer.getSingleInstance().getAvaliableId(conn, "media_zip_info", "id"));
			stmt.setInt(2, companyId);
			stmt.setString(3, path);
			stmt.execute();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}
	}

	// ���س�����Ϣ
	private List<CarInfo> loadCarInfo() {
		List<CarInfo> list = new ArrayList<CarInfo>();
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			StatementHandle stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("select * from car where car_id is not null and mdt_id >10");
			CarInfo car = null;
			while (rs.next()) {
				car = new CarInfo();
				car.carNo = rs.getString("car_id");
				car.mdtId = Integer.parseInt(rs.getString("mdt_id").substring(1));
				list.add(car);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}
		return list;
	}
	
	// ��ȡ¼���ļ�����ʱ��ý������ID����Զ����load�ļ�ʱʹ��
	private long getFlagId() {
		long tempFlag = 0;
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			StatementHandle stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("select max(id) max_id from media");
			if (rs.next()) {
				tempFlag = rs.getLong("max_id");
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}
		return tempFlag;
	}
	
	//���ز��ɹ������б�
	private List<CarInfo> checkOperate(List<CarInfo> list,int type) {
		List<CarInfo> failList = new ArrayList<CarInfo>();
		Map<String ,String> map = new HashMap<String,String>();
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.MINUTE, -5);

		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			StatementHandle stmt = conn.createStatement();
			String con = "";
			for(int i=0;i<list.size();i++){
				if(i == 0){
					con = "'"+list.get(i).carNo+"'";
					continue;
				}
				con = con + ",'"+list.get(i).carNo+"'";
			}
			StringBuffer sql = new StringBuffer();
			sql.append("select distinct car_no from media where media_type="+type);
			sql.append(" and car_no in ("+con+")");
			if(type == 0){
				sql.append(" and event_type=1");
			}
			sql.append(" and end_time >= to_date('"+GeneralConst.YYYY_MM_DD_HH_MM_SS.format(cal.getTime())+"','yyyy-MM-dd HH24:mi:ss')");
			ResultSet rs = stmt.executeQuery(sql.toString());
			String carNo = null;
			while (rs.next()) {
				carNo = rs.getString("car_no");
				map.put(carNo, carNo);
			}

			for(int i=0;i<list.size();i++){
				if(!map.containsKey(list.get(i).carNo)){
					failList.add(list.get(i));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}
		return failList;
	}
	
	private void zipFile(){
		System.out.println("---------------idFlag="+this.idFlag +" , idFlag1=" + this.idFlag1);
		Calendar cal = Calendar.getInstance();
		
		String rootDir = fileCopyPath+"/"+GeneralConst.YYYYMMDD.format(cal.getTime());
		
		File file = new File(rootDir);
		if(!file.exists() && !file.isDirectory())      
		{       
		    file .mkdir();    
		} 
		
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			StatementHandle stmt = conn.createStatement();
			String sql = "select t1.*,t2.term_id,to_char(t1.the_time,'HH24miss') op_time from media t1 , car t2 where t1.car_no = t2.car_id and media_type=0 and event_type =1 and id > "+ idFlag1 +" order by id asc";
			ResultSet rs = stmt.executeQuery(sql);
			int companyId = 0;
			int sourceId = 0;
			String wholePath = "";
			String filePath = "";
			String carNo = "";
			String opTime = "";
			while (rs.next()) {
				companyId = rs.getInt("term_id");
				sourceId = rs.getInt("source_id");
				wholePath = rs.getString("whole_path");
				carNo = rs.getString("car_no");
				opTime = rs.getString("op_time");
				filePath = rootDir+"/"+companyId;
				file = new File(filePath);
				if(!file.exists() && !file.isDirectory())      
				{       
				    file .mkdir();    
				}
				

				if(sourceId == 1)
					filePath = filePath + "/ǰ����Ƭ";
				else
					filePath = filePath + "/������Ƭ";
					
				
				file = new File(filePath);
				if(!file.exists() && !file.isDirectory())      
				{       
				    file .mkdir();    
				}
				filePath = filePath + "/"+ carNo+"("+opTime+").jpg";
				ZipTools.copyFile(fileSourcePath+"/"+wholePath, filePath);
			}
			
			rs.close();
			
			if(this.idFlag >0 && this.idFlag1>0){
				sql = "select t1.*,t2.term_id,to_char(t1.the_time,'HH24miss') op_time from media t1 , car t2 where t1.car_no = t2.car_id and media_type=1 and id > "+idFlag+" and id < "+idFlag1;
				rs = stmt.executeQuery(sql);
				while (rs.next()) {
					companyId = rs.getInt("term_id");
					wholePath = rs.getString("whole_path");
					carNo = rs.getString("car_no");
					opTime = rs.getString("op_time");
					filePath = rootDir+"/"+companyId;
					file = new File(filePath);
					if(!file.exists() && !file.isDirectory())      
					{       
					    file .mkdir();    
					}
					
					filePath = filePath + "/¼��";

					file = new File(filePath);
					if(!file.exists() && !file.isDirectory())      
					{       
					    file .mkdir();    
					}
					
					filePath = filePath + "/"+ carNo+"("+opTime+").enc";
					
					File sourceFile = new File(fileSourcePath+"/"+wholePath);
					if(sourceFile.exists()){
						ZipTools.copyFile(fileSourcePath+"/"+wholePath, filePath);
					}
				}
			}
			
			String zipRootDir = fileDestPath+"/"+GeneralConst.YYYYMMDD.format(cal.getTime());
			String dbZipPath = mediaPath+"/"+GeneralConst.YYYYMMDD.format(cal.getTime());
			file = new File(zipRootDir);
			if(!file.exists() && !file.isDirectory())      
			{       
			    file .mkdir();    
			} 
			file = new File(rootDir);
			String[] files = file.list();
			if(files != null && files.length>0){
				ZipTools.zipFile(rootDir, zipRootDir+"/all.zip");
				this.saveMediaZipInfo(0, dbZipPath+"/all.zip");//����ѹ���ļ���Ϣ
				
				for(int i=0;i<files.length;i++){
					ZipTools.zipFile(rootDir+"/"+files[i], zipRootDir+"/"+files[i]+".zip");
					this.saveMediaZipInfo(Integer.parseInt(files[i]), dbZipPath+"/"+files[i]+".zip");//����ѹ���ļ���Ϣ
				}
			}
			ZipTools.deleteDirFile(rootDir);//ɾ������Ŀ¼
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}
	}
	
	private Map<String,String> loadHegeCar(){
		Map<String,String> carMap = new HashMap<String,String>();
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			StatementHandle stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("select * from CHOUJIAN_HEGE");
			String flag = "";
			while (rs.next()) {
				flag = rs.getString("car_no")+"-"+rs.getInt("source_id");
				carMap.put(flag, flag);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}
		return carMap;
	}
	
	private void zipFilePhotoChoujian(){
		Map<String,String> carMap = this.loadHegeCar();
		if(carMap == null || carMap.size()==0){
			return;
		}
		
		System.out.println("-------hege="+carMap.size()+"--------idFlag="+this.idFlag +" , idFlag1=" + this.idFlag1);
		Calendar cal = Calendar.getInstance();
		
		String rootDir = fileCopyPath+"/"+GeneralConst.YYYYMMDD.format(cal.getTime())+"_temp";
		
		File file = new File(rootDir);
		if(!file.exists() && !file.isDirectory())      
		{       
		    file .mkdir();    
		} 
		
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			StatementHandle stmt = conn.createStatement();
			String sql = "select t1.*,t2.term_id,to_char(t1.the_time,'HH24miss') op_time from media t1 , car t2 where t1.car_no = t2.car_id and media_type=0 and event_type =1 and id > "+ idFlag1 +" order by id asc";
			ResultSet rs = stmt.executeQuery(sql);
			int companyId = 0;
			int sourceId = 0;
			String wholePath = "";
			String filePath = "";
			String carNo = "";
			String opTime = "";
			while (rs.next()) {
				companyId = rs.getInt("term_id");
				sourceId = rs.getInt("source_id");
				wholePath = rs.getString("whole_path");
				carNo = rs.getString("car_no");
				opTime = rs.getString("op_time");
				if(carMap.containsKey(carNo+"-"+sourceId)){
					continue;
				}
				
				filePath = rootDir+"/"+companyId;
				file = new File(filePath);
				if(!file.exists() && !file.isDirectory())      
				{       
				    file .mkdir();    
				}
				

				if(sourceId == 1)
					filePath = filePath + "/ǰ����Ƭ";
				else
					filePath = filePath + "/������Ƭ";
					
				
				file = new File(filePath);
				if(!file.exists() && !file.isDirectory())      
				{       
				    file .mkdir();    
				}
				filePath = filePath + "/"+ carNo+"("+opTime+").jpg";
				ZipTools.copyFile(fileSourcePath+"/"+wholePath, filePath);
			}
			
			rs.close();
			
			String zipRootDir = fileDestPath+"/"+GeneralConst.YYYYMMDD.format(cal.getTime());
			String dbZipPath = mediaPath+"/"+GeneralConst.YYYYMMDD.format(cal.getTime());
			file = new File(zipRootDir);
			if(!file.exists() && !file.isDirectory())      
			{       
			    file .mkdir();    
			} 
			file = new File(rootDir);
			String[] files = file.list();
			if(files != null && files.length>0){
				ZipTools.zipFile(rootDir, zipRootDir+"/choujian.zip");
				this.saveMediaZipInfo(-1, dbZipPath+"/choujian.zip");//����ѹ���ļ���Ϣ
			}
			ZipTools.deleteDirFile(rootDir);//ɾ������Ŀ¼
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}
	}
	
	private void removeZipFile(){
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.DAY_OF_MONTH, -fileDays);
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		calendar.set(Calendar.MILLISECOND, 0);
		Date pointTime = calendar.getTime();

		File f = new File(fileDestPath);
		if(f.exists()){
			String[] filenames = f.list();
			if(filenames == null){
				return;
			}

			for(int i = 0; i < filenames.length; i++){
				File file = new File(fileDestPath+"/"+filenames[i]);
				if(this.isNeedDelete(file, filenames[i], pointTime)){
					ZipTools.deleteDirFile(fileDestPath+"/"+filenames[i]);
					this.deleteMediaZipInfo(filenames[i]);
				}
			}
		}
	}
	
	private boolean isNeedDelete(File f,String name,Date limitTime){
		if(new Date(f.lastModified()).before(limitTime)){
			return true;
		}
		
		try{
			Date d = GeneralConst.YYYYMMDD.parse(name);
			if(d.before(limitTime)){
				return true;
			}
		}catch(Exception e){}
		
		return false;
	}
	
	private void deleteMediaZipInfo(String date) {

		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			String sql = "delete from  media_zip_info where to_char(zip_date,'yyyyMMdd')=?";
			StatementHandle stmt = conn.prepareStatement(sql.toString());
			stmt.setString(1, date);
			stmt.execute();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}
	}

	private class TimerTask extends FleetyTimerTask {
		public void run() {
			Calendar cal = Calendar.getInstance();
			cal.add(Calendar.DAY_OF_MONTH, 0);
			cal.set(Calendar.HOUR_OF_DAY, 0);
			cal.set(Calendar.MINUTE, 0);
			cal.set(Calendar.SECOND, 0);
			cal.set(Calendar.MILLISECOND, 0);

			System.out.println("Fire ExecTask ScheduleTaskAnasisServer:" + GeneralConst.YYYY_MM_DD_HH.format(cal.getTime()));
			ScheduleTaskAnasisServer.this.addExecTask(new ExecTask(cal));
		}
	}

	private class ExecTask extends BasicTask {
		private Calendar anaDate = null;

		public ExecTask(Calendar anaDate) {
			this.anaDate = anaDate;
		}

		public boolean execute() throws Exception {
			ScheduleTaskAnasisServer.this.executeTask(this.anaDate);
			return true;
		}

		public String getDesc() {
			return "��ʱ���ա����ѹ����Ƭ�Լ�¼������";
		}

		public Object getFlag() {
			return "ScheduleTaskAnasisServer";
		}
	}

	private class CarInfo {
		String carNo;
		int mdtId;
		int mediaType;
		String content;
	}

}
