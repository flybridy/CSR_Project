package com.fleety.analysis.track.task;

import java.net.IDN;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;

import server.db.DbServer;

import com.fleety.analysis.AnalysisServer;
import com.fleety.analysis.track.ITrackAnalysis;
import com.fleety.analysis.track.TrackInfo;
import com.fleety.base.GeneralConst;
import com.fleety.base.InfoContainer;
import com.fleety.track.TrackIO;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;

public class HistoryStatuAnalysisForhour implements ITrackAnalysis{
	private HashMap comMapping = null;
	private int statDuration = 10*60*1000;
	@Override
	public boolean startAnalysisTrack(AnalysisServer parentServer,
			InfoContainer statInfo) {
		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
		Date eTime = statInfo.getDate(STAT_END_TIME_DATE);
		this.comMapping = null;
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try{
			StatementHandle stmt = conn.prepareStatement("select * from company_warning_history where record_time between ? and ?");
			
			stmt.setTimestamp(1, new Timestamp(sTime.getTime()+1000));
			stmt.setTimestamp(2, new Timestamp(eTime.getTime()));
			ResultSet sets = stmt.executeQuery();
			if(!sets.next()){
				this.comMapping = new HashMap();
			}
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
		
		if(this.comMapping == null){
			System.out.println("Not Need Analysis:"+this.toString());
			
		}else{
			System.out.println("Start Analysis:"+this.toString());
		}
		
		return this.comMapping != null;
	}

	@Override
	public void analysisDestTrack(AnalysisServer parentServer,
			TrackInfo trackInfo) {
		if(this.comMapping == null){
			return ;
		}
		if(trackInfo.trackArr == null){
			return ;
		}
		
		int comId = trackInfo.dInfo.companyId;
		ComInfo comInfo = (ComInfo)this.comMapping.get(new Integer(comId));
		if(comInfo == null){
			comInfo = new ComInfo();
			comInfo.companyName = trackInfo.dInfo.companyName;
			
			this.comMapping.put(new Integer(comId), comInfo);
		}
		
		/**
		 * 每辆车按照轨迹时间顺序计算过去，每10分钟采样一次车辆状态并按照机构记录
		 * 记录时每小时取所有10分钟的均值进行记录，如果单个10分钟的采样点样本数据不足总数的一半，那么则放弃该采样点信息。
		 * */
		long limitDuration = 180000;
		long sTime = trackInfo.sDate.getTime();
		int statIndex = 0,curIndex = 0;
		long preTime = -1;
		Calendar time = Calendar.getInstance();
		int status,preStatus = -1;
		DbHandle conn = DbServer.getSingleInstance().getConn();
		int type_id=0;
		
		String car_no = trackInfo.dInfo.destNo;
		StatementHandle stmt = null;
		try {
			stmt = conn.createStatement();
			ResultSet sets = stmt.executeQuery("select type_id from car where car_id = '"+car_no+"'");
			if(sets.next()){
				type_id = sets.getInt("type_id");
			}
			sets.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		DbServer.getSingleInstance().releaseConn(conn);
		for(int i=0;i<trackInfo.trackArr.length;i++){
			time.setTime(trackInfo.trackArr[i].getDate(TrackIO.DEST_TIME_FLAG));
			curIndex = (int)((time.getTimeInMillis() - sTime)/statDuration);
			status = trackInfo.trackArr[i].getInteger(TrackIO.DEST_STATUS_FLAG).intValue()&0x0F;
			if(curIndex > statIndex){
				if(preTime > 0){
					time.setTimeInMillis(sTime + (statIndex+1)*this.statDuration);
					
					if(time.getTimeInMillis() - preTime < limitDuration){
						if(preStatus == 0|| preStatus == 8){//空车数
							if(type_id == 1)
							    comInfo.Empty_num_red[statIndex] ++;
							else if(type_id == 2)
								comInfo.Empty_num_green[statIndex] ++;
							else if(type_id == 3)
								comInfo.Empty_num_electric[statIndex] ++;
							else if(type_id == 4)
								comInfo.Empty_num_accessible[statIndex] ++;
							
						}else if(preStatus == 1 || preStatus == 9){//重车数
							if(type_id == 1)
							    comInfo.Overload_num_red[statIndex]++;
							else if(type_id == 2)
								comInfo.Overload_num_green[statIndex]++;
							else if(type_id == 3)
								comInfo.Overload_num_electric[statIndex] ++;
							else if(type_id == 4)
								comInfo.Overload_num_accessible[statIndex] ++;
						}else if(preStatus == 2){
							if(type_id == 1)
							    comInfo.Task_num_red[statIndex]++;
							else if(type_id == 2)
								comInfo.Task_num_green[statIndex]++;
							else if(type_id == 3)
								comInfo.Task_num_electric[statIndex] ++;
							else if(type_id == 4)
								comInfo.Task_num_accessible[statIndex] ++;
						}else{
							if(type_id == 1)
							    comInfo.Other_num_red[statIndex]++;
							else if(type_id == 2)
								comInfo.Other_num_green[statIndex]++;
							else if(type_id == 3)
								comInfo.Other_num_electric[statIndex] ++;
							else if(type_id == 4)
								comInfo.Other_num_accessible[statIndex] ++;
						}
					}
				}
				
				statIndex = curIndex;
			}
			
			preTime = time.getTimeInMillis();
			preStatus = status;
		}
		
		if(preTime > 0){
			time.setTimeInMillis(sTime+(curIndex+1)*this.statDuration);
			if(time.getTimeInMillis() - preTime < limitDuration){
				if(preStatus == 0){
					if(type_id == 1)
					    comInfo.Empty_num_red[curIndex] ++;
					else if(type_id == 2)
						comInfo.Empty_num_green[curIndex] ++;
					else if(type_id == 3)
						comInfo.Empty_num_electric[curIndex] ++;
					else if(type_id == 4)
						comInfo.Empty_num_accessible[curIndex] ++;
				}else if(preStatus == 1){
					if(type_id == 1)
					    comInfo.Overload_num_red[curIndex]++;
					else if(type_id == 2)
						comInfo.Overload_num_green[curIndex]++;
					else if(type_id == 3)
						comInfo.Overload_num_electric[curIndex] ++;
					else if(type_id == 4)
						comInfo.Overload_num_accessible[curIndex] ++;
				}else if(preStatus == 2){
					if(type_id == 1)
					    comInfo.Task_num_red[curIndex]++;
					else if(type_id == 2)
						comInfo.Task_num_green[curIndex]++;
					else if(type_id == 3)
						comInfo.Task_num_electric[curIndex] ++;
					else if(type_id == 4)
						comInfo.Task_num_accessible[curIndex] ++;
				}else {
					if(type_id == 1)
					    comInfo.Other_num_red[curIndex]++;
					else if(type_id == 2)
						comInfo.Other_num_green[curIndex]++;
					else if(type_id == 3)
						comInfo.Other_num_electric[curIndex] ++;
					else if(type_id == 4)
						comInfo.Other_num_accessible[curIndex] ++;
				}
			}
		}
	}

	@Override
	public void endAnalysisTrack(AnalysisServer parentServer,
			InfoContainer statInfo) {
		if(this.comMapping == null){
			return ;
		}
		int recordNum = 0;
		Date sDate = statInfo.getDate(STAT_START_TIME_DATE);
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try{
			conn.setAutoCommit(false);
			Calendar cal = Calendar.getInstance();
			Integer comId;
			int totalNum,count,Overload_num_red ,Empty_num_red ,Task_num_red ,Other_num_red ,Overload_num_green ,Empty_num_green ,Task_num_green ,Other_num_green ,Overload_num_electric ,Empty_num_electric ,Task_num_electric ,Other_num_electric ,Overload_num_accessible ,Empty_num_accessible ,Task_num_accessible ,Other_num_accessible  ;
			
			ComInfo comInfo;
			int step = (int)(GeneralConst.ONE_HOUR_TIME/this.statDuration);
			
			StatementHandle stmt = conn.prepareStatement("insert into company_warning_history(id ,Company_id ,Company_name ,record_time ,Overload_num_red ,Empty_num_red ,Task_num_red ,Other_num_red ,Overload_num_green ,Empty_num_green ,Task_num_green ,Other_num_green ,Overload_num_electric ,Empty_num_electric ,Task_num_electric ,Other_num_electric ,Overload_num_accessible ,Empty_num_accessible ,Task_num_accessible ,Other_num_accessible ) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
			
			for(Iterator itr = this.comMapping.keySet().iterator();itr.hasNext();){
				comId = (Integer)itr.next();
				comInfo = (ComInfo)this.comMapping.get(comId);
				cal.setTime(sDate);
				for(int i=0;i<24;i++){
					cal.set(Calendar.HOUR_OF_DAY, i);
					Overload_num_red =Empty_num_red =Task_num_red =Other_num_red =Overload_num_green =Empty_num_green =Task_num_green =Other_num_green =Overload_num_electric =Empty_num_electric =Task_num_electric =Other_num_electric =Overload_num_accessible =Empty_num_accessible =Task_num_accessible =Other_num_accessible=count=0;
					for(int x=0,index=i*step;x<step;x++,index++){
						totalNum = comInfo.Overload_num_red[index]+comInfo.Overload_num_green[index]+comInfo.Overload_num_electric[index]+comInfo.Overload_num_accessible[index]
								+comInfo.Empty_num_red[index]+comInfo.Empty_num_green[index]+comInfo.Empty_num_electric[index]+comInfo.Empty_num_accessible[index]
								+comInfo.Task_num_red[index]+comInfo.Task_num_green[index]+comInfo.Task_num_electric[index]+comInfo.Task_num_accessible[index]
								+comInfo.Other_num_red[index]+comInfo.Other_num_green[index]+comInfo.Other_num_electric[index]+comInfo.Other_num_accessible[index];
						if(totalNum > 0){
							Overload_num_red +=comInfo.Overload_num_red[index];
							Empty_num_red +=comInfo.Empty_num_red[index];
							Task_num_red +=comInfo.Task_num_red[index];
							Other_num_red +=comInfo.Other_num_red[index];
							
							Overload_num_green +=comInfo.Overload_num_green[index];
							Empty_num_green +=comInfo.Empty_num_green[index];
							Task_num_green +=comInfo.Task_num_green[index];
							Other_num_green +=comInfo.Other_num_green[index];
							
							Overload_num_electric +=comInfo.Overload_num_electric[index];
							Empty_num_electric +=comInfo.Empty_num_electric[index];
							Task_num_electric +=comInfo.Task_num_electric[index];
							Other_num_electric +=comInfo.Other_num_electric[index];
							
							Overload_num_accessible +=comInfo.Overload_num_accessible[index];
							Empty_num_accessible +=comInfo.Empty_num_accessible[index];
							Task_num_accessible +=comInfo.Task_num_accessible[index];
							Other_num_accessible +=comInfo.Other_num_accessible[index];
							count ++;
						}
					}
					if(count > 0){
						stmt.setInt(1, (int)DbServer.getSingleInstance().getAvaliableId(conn, "company_warning_history", "id"));
						stmt.setInt(2, comId);
						stmt.setString(3, comInfo.companyName);
						stmt.setTimestamp(4, new Timestamp(cal.getTimeInMillis()));
						stmt.setInt(5, Overload_num_red/count);
						stmt.setInt(6, Empty_num_red/count);
						stmt.setInt(7, Task_num_red/count);
						stmt.setInt(8, Other_num_red/count);
						stmt.setInt(9, Overload_num_green/count);
						stmt.setInt(10, Empty_num_green/count);
						stmt.setInt(11, Task_num_green/count);
						stmt.setInt(12, Other_num_green/count);
						stmt.setInt(13, Overload_num_electric/count);
						stmt.setInt(14, Empty_num_electric/count);
						stmt.setInt(15, Task_num_electric/count);
						stmt.setInt(16, Other_num_electric/count);
						stmt.setInt(17, Overload_num_accessible/count);
						stmt.setInt(18, Empty_num_accessible/count);
						stmt.setInt(19, Task_num_accessible/count);
						stmt.setInt(20, Other_num_accessible/count);
						
						stmt.addBatch();
						
						recordNum++;
					}
				}
				stmt.executeBatch();
			}
			
			conn.commit();
		}catch(Exception e){
			e.printStackTrace();
			if(conn != null){
				try{
					conn.rollback();
				}catch(Exception ee){
					ee.printStackTrace();
				}
			}
			recordNum = 0;
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
		System.out.println("Finish CompanyHistoryAnalysis!!"+" recordNum="+recordNum);
//		try {
//			StatementHandle stmt11 = conn.prepareStatement("select * from all_warning_parameter where to_char(record_time,'yyyy-mm-dd')=to_char(sysdate-1,'yyyy-mm-dd')");
//			ResultSet set = stmt11.executeQuery();
//			if(!set.next()){
//				System.out.println("开始整体指标分析");
//					DbHandle con= DbServer.getSingleInstance().getConn();
////				AllDataAnalysis(con);
//			}else {
//				System.out.println("总体性指标不需要分析");
//			}
//		} catch (SQLException e) {
//			e.printStackTrace();
//		}
	}
	public void AllDataAnalysis(DbHandle conn) {
		try {
			StatementHandle allstmt = conn.prepareStatement("insert into all_warning_parameter(id ,record_time,Overload_num_red ,Empty_num_red ,Task_num_red ,Other_num_red ,Overload_num_green ,Empty_num_green ,Task_num_green ,Other_num_green ,Overload_num_electric ,Empty_num_electric ,Task_num_electric ,Other_num_electric ,Overload_num_accessible ,Empty_num_accessible ,Task_num_accessible ,Other_num_accessible ) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
			StatementHandle querystmt = conn.createStatement();
			ResultSet res=querystmt.executeQuery("select to_char(record_time,'yyyy-mm-dd hh24:mi:ss')record_time,sum(overload_num_red)overload_num_red,sum(empty_num_red)empty_num_red,sum(task_num_red)task_num_red,sum(other_num_red)other_num_red,sum(overload_num_green)overload_num_green,sum(empty_num_green)empty_num_green,sum(task_num_green)task_num_green,sum(other_num_green)other_num_green,sum(overload_num_electric)overload_num_electric,sum(empty_num_electric)empty_num_electric,sum(task_num_electric)task_num_electric,sum(other_num_electric)other_num_electric,sum(overload_num_accessible)overload_num_accessible,sum(empty_num_accessible)empty_num_accessible,sum(task_num_accessible)task_num_accessible,sum(other_num_accessible)other_num_accessible from company_warning_history where to_char(record_time,'yyyy-mm-dd')=to_char(sysdate-1,'yyyy-mm-dd') group by record_time"); 
		   while(res.next()){
			   allstmt.setInt(1, (int)DbServer.getSingleInstance().getAvaliableId(conn, "all_warning_parameter", "id"));
			   System.out.println("循环的时间"+res.getString("record_time"));
			   SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			   java.util.Date d1 = sdf.parse(res.getString("record_time")); //先把字符串转为util.Date对象
			   Timestamp d2 = new Timestamp(d1.getTime());
			   allstmt.setTimestamp(2, d2);
			   allstmt.setInt(3, res.getInt("Overload_num_red"));
			   allstmt.setInt(4, res.getInt("Empty_num_red"));
			   allstmt.setInt(5, res.getInt("Task_num_red"));
			   allstmt.setInt(6, res.getInt("Other_num_red"));
			   allstmt.setInt(7, res.getInt("Overload_num_green"));
			   allstmt.setInt(8, res.getInt("Empty_num_green"));
			   allstmt.setInt(9, res.getInt("Task_num_green"));
			   allstmt.setInt(10, res.getInt("Other_num_green"));
			   allstmt.setInt(11, res.getInt("Overload_num_electric"));
			   allstmt.setInt(12, res.getInt("Empty_num_electric"));
			   allstmt.setInt(13, res.getInt("Task_num_electric"));
			   allstmt.setInt(14, res.getInt("Other_num_electric"));
			   allstmt.setInt(15, res.getInt("Overload_num_accessible"));
			   allstmt.setInt(16, res.getInt("Empty_num_accessible"));
			   allstmt.setInt(17, res.getInt("Task_num_accessible"));
			   allstmt.setInt(18, res.getInt("Other_num_accessible"));
			   allstmt.addBatch();
			   
		   }
		   System.out.println("分析完成！！！！");
		   allstmt.executeBatch();
		   
		   
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public String toString(){
		return "CompanyHistoryStatuAnalysisForhour";
	}
	private class ComInfo{
		public String companyName = null;
		
		public int[] Overload_num_red = new int[(int)(GeneralConst.ONE_DAY_TIME/statDuration)];
		public int[] Empty_num_red = new int[(int)(GeneralConst.ONE_DAY_TIME/statDuration)];
		public int[] Task_num_red = new int[(int)(GeneralConst.ONE_DAY_TIME/statDuration)];
		public int[] Other_num_red = new int[(int)(GeneralConst.ONE_DAY_TIME/statDuration)];
		public int[] Overload_num_green = new int[(int)(GeneralConst.ONE_DAY_TIME/statDuration)];
		public int[] Empty_num_green = new int[(int)(GeneralConst.ONE_DAY_TIME/statDuration)];
		public int[] Task_num_green = new int[(int)(GeneralConst.ONE_DAY_TIME/statDuration)];
		public int[] Other_num_green = new int[(int)(GeneralConst.ONE_DAY_TIME/statDuration)];
		public int[] Overload_num_electric = new int[(int)(GeneralConst.ONE_DAY_TIME/statDuration)];
		public int[] Empty_num_electric  = new int[(int)(GeneralConst.ONE_DAY_TIME/statDuration)];
		public int[] Task_num_electric = new int[(int)(GeneralConst.ONE_DAY_TIME/statDuration)];
		public int[] Other_num_electric = new int[(int)(GeneralConst.ONE_DAY_TIME/statDuration)];
		public int[] Overload_num_accessible = new int[(int)(GeneralConst.ONE_DAY_TIME/statDuration)];
		public int[] Empty_num_accessible = new int[(int)(GeneralConst.ONE_DAY_TIME/statDuration)];
		public int[] Task_num_accessible = new int[(int)(GeneralConst.ONE_DAY_TIME/statDuration)];
		public int[] Other_num_accessible = new int[(int)(GeneralConst.ONE_DAY_TIME/statDuration)];
		
	}

}
