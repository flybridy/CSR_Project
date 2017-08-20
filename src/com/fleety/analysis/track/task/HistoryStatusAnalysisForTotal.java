package com.fleety.analysis.track.task;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

import server.db.DbServer;

import com.fleety.analysis.AnalysisServer;
import com.fleety.analysis.track.ITrackAnalysis;
import com.fleety.analysis.track.TrackInfo;
import com.fleety.base.GeneralConst;
import com.fleety.base.InfoContainer;
import com.fleety.track.TrackIO;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;

public class HistoryStatusAnalysisForTotal implements ITrackAnalysis{
	private HashMap comMapping = null;
	private int statDuration = 10*60*1000;
	@Override
	public boolean startAnalysisTrack(AnalysisServer parentServer,
			InfoContainer statInfo) {
		System.out.println("yyyyyyy");
		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
		Date eTime = statInfo.getDate(STAT_END_TIME_DATE);
		this.comMapping = null;
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try{
			StatementHandle stmt = conn.prepareStatement("select * from all_warning_parameter where record_time between ? and ?");
			
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
		
		TotalInfo totalinfo;
		if(this.comMapping.containsKey("today"))
			totalinfo = (TotalInfo)this.comMapping.get("today");
		else
			{
			totalinfo = new TotalInfo();
			this.comMapping.put("today", totalinfo);
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
		int type_id=-1;
		type_id = trackInfo.dInfo.carType;
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
								totalinfo.Empty_num_red[statIndex] ++;
							else if(type_id == 2)
								totalinfo.Empty_num_green[statIndex] ++;
							else if(type_id == 3)
								totalinfo.Empty_num_electric[statIndex] ++;
							else if(type_id == 4)
								totalinfo.Empty_num_accessible[statIndex] ++;
							
						}else if(preStatus == 1 || preStatus == 9){//重车数
							if(type_id == 1)
								totalinfo.Overload_num_red[statIndex]++;
							else if(type_id == 2)
								totalinfo.Overload_num_green[statIndex]++;
							else if(type_id == 3)
								totalinfo.Overload_num_electric[statIndex] ++;
							else if(type_id == 4)
								totalinfo.Overload_num_accessible[statIndex] ++;
						}else if(preStatus == 2){
							if(type_id == 1)
								totalinfo.Task_num_red[statIndex]++;
							else if(type_id == 2)
								totalinfo.Task_num_green[statIndex]++;
							else if(type_id == 3)
								totalinfo.Task_num_electric[statIndex] ++;
							else if(type_id == 4)
								totalinfo.Task_num_accessible[statIndex] ++;
						}else{
							if(type_id == 1)
								totalinfo.Other_num_red[statIndex]++;
							else if(type_id == 2)
								totalinfo.Other_num_green[statIndex]++;
							else if(type_id == 3)
								totalinfo.Other_num_electric[statIndex] ++;
							else if(type_id == 4)
								totalinfo.Other_num_accessible[statIndex] ++;
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
						totalinfo.Empty_num_red[curIndex] ++;
					else if(type_id == 2)
						totalinfo.Empty_num_green[curIndex] ++;
					else if(type_id == 3)
						totalinfo.Empty_num_electric[curIndex] ++;
					else if(type_id == 4)
						totalinfo.Empty_num_accessible[curIndex] ++;
				}else if(preStatus == 1){
					if(type_id == 1)
						totalinfo.Overload_num_red[curIndex]++;
					else if(type_id == 2)
						totalinfo.Overload_num_green[curIndex]++;
					else if(type_id == 3)
						totalinfo.Overload_num_electric[curIndex] ++;
					else if(type_id == 4)
						totalinfo.Overload_num_accessible[curIndex] ++;
				}else if(preStatus == 2){
					if(type_id == 1)
						totalinfo.Task_num_red[curIndex]++;
					else if(type_id == 2)
						totalinfo.Task_num_green[curIndex]++;
					else if(type_id == 3)
						totalinfo.Task_num_electric[curIndex] ++;
					else if(type_id == 4)
						totalinfo.Task_num_accessible[curIndex] ++;
				}else {
					if(type_id == 1)
						totalinfo.Other_num_red[curIndex]++;
					else if(type_id == 2)
						totalinfo.Other_num_green[curIndex]++;
					else if(type_id == 3)
						totalinfo.Other_num_electric[curIndex] ++;
					else if(type_id == 4)
						totalinfo.Other_num_accessible[curIndex] ++;
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
			int totalNum,count,Overload_num_red ,Empty_num_red ,Task_num_red ,Other_num_red ,Overload_num_green ,Empty_num_green ,Task_num_green ,Other_num_green ,Overload_num_electric ,Empty_num_electric ,Task_num_electric ,Other_num_electric ,Overload_num_accessible ,Empty_num_accessible ,Task_num_accessible ,Other_num_accessible  ;
			
			TotalInfo totalInfo = (TotalInfo)this.comMapping.get("today");
			int step = (int)(GeneralConst.ONE_HOUR_TIME/this.statDuration);
			
			StatementHandle stmt = conn.prepareStatement("insert into all_warning_parameter(id ,record_time,Overload_num_red ,Empty_num_red ,Task_num_red ,Other_num_red ,Overload_num_green ,Empty_num_green ,Task_num_green ,Other_num_green ,Overload_num_electric ,Empty_num_electric ,Task_num_electric ,Other_num_electric ,Overload_num_accessible ,Empty_num_accessible ,Task_num_accessible ,Other_num_accessible ) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
			
				cal.setTime(sDate);
				for(int i=0;i<24;i++){
					cal.set(Calendar.HOUR_OF_DAY, i);
					Overload_num_red =Empty_num_red =Task_num_red =Other_num_red =Overload_num_green =Empty_num_green =Task_num_green =Other_num_green =Overload_num_electric =Empty_num_electric =Task_num_electric =Other_num_electric =Overload_num_accessible =Empty_num_accessible =Task_num_accessible =Other_num_accessible=count=0;
					for(int x=0,index=i*step;x<step;x++,index++){
						totalNum = totalInfo.Overload_num_red[index]+totalInfo.Overload_num_green[index]+totalInfo.Overload_num_electric[index]+totalInfo.Overload_num_accessible[index]
								+totalInfo.Empty_num_red[index]+totalInfo.Empty_num_green[index]+totalInfo.Empty_num_electric[index]+totalInfo.Empty_num_accessible[index]
								+totalInfo.Task_num_red[index]+totalInfo.Task_num_green[index]+totalInfo.Task_num_electric[index]+totalInfo.Task_num_accessible[index]
								+totalInfo.Other_num_red[index]+totalInfo.Other_num_green[index]+totalInfo.Other_num_electric[index]+totalInfo.Other_num_accessible[index];
						if(totalNum > 0){
							Overload_num_red +=totalInfo.Overload_num_red[index];
							Empty_num_red +=totalInfo.Empty_num_red[index];
							Task_num_red +=totalInfo.Task_num_red[index];
							Other_num_red +=totalInfo.Other_num_red[index];
							
							Overload_num_green +=totalInfo.Overload_num_green[index];
							Empty_num_green +=totalInfo.Empty_num_green[index];
							Task_num_green +=totalInfo.Task_num_green[index];
							Other_num_green +=totalInfo.Other_num_green[index];
							
							Overload_num_electric +=totalInfo.Overload_num_electric[index];
							Empty_num_electric +=totalInfo.Empty_num_electric[index];
							Task_num_electric +=totalInfo.Task_num_electric[index];
							Other_num_electric +=totalInfo.Other_num_electric[index];
							
							Overload_num_accessible +=totalInfo.Overload_num_accessible[index];
							Empty_num_accessible +=totalInfo.Empty_num_accessible[index];
							Task_num_accessible +=totalInfo.Task_num_accessible[index];
							Other_num_accessible +=totalInfo.Other_num_accessible[index];
							count ++;
						}
					}
					if(count > 0){
						stmt.setInt(1, (int)DbServer.getSingleInstance().getAvaliableId(conn, "all_warning_parameter", "id"));
						stmt.setTimestamp(2, new Timestamp(cal.getTimeInMillis()));
						stmt.setInt(3, Overload_num_red/count);
						stmt.setInt(4, Empty_num_red/count);
						stmt.setInt(5, Task_num_red/count);
						stmt.setInt(6, Other_num_red/count);
						stmt.setInt(7, Overload_num_green/count);
						stmt.setInt(8, Empty_num_green/count);
						stmt.setInt(9, Task_num_green/count);
						stmt.setInt(10, Other_num_green/count);
						stmt.setInt(11, Overload_num_electric/count);
						stmt.setInt(12, Empty_num_electric/count);
						stmt.setInt(13, Task_num_electric/count);
						stmt.setInt(14, Other_num_electric/count);
						stmt.setInt(15, Overload_num_accessible/count);
						stmt.setInt(16, Empty_num_accessible/count);
						stmt.setInt(17, Task_num_accessible/count);
						stmt.setInt(18, Other_num_accessible/count);
						stmt.addBatch();
						recordNum++;
					}
				}
				stmt.executeBatch();
			
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
		System.out.println("Finish AllHistoryAnalysis!!"+" recordNum="+recordNum);
	}
	
	public String toString(){
		return "AllHistoryStatuAnalysisForhour";
	}
	private class TotalInfo{
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
