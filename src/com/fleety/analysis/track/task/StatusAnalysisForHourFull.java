package com.fleety.analysis.track.task;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
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

public class StatusAnalysisForHourFull implements ITrackAnalysis{
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
			StatementHandle stmt = conn.prepareStatement("select * from ana_taxi_status_hour_type where stat_time between ? and ?");
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
		int type_id = 0;
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
						if(preStatus == 0|| preStatus == 8){
							if(type_id == 1)
							    comInfo.vacantTaxiNum_red[statIndex] ++;
							else if(type_id == 2)
								comInfo.vacantTaxiNum_green[statIndex] ++;
							else if(type_id == 3)
								comInfo.vacant_num_electric[statIndex] ++;
							else if(type_id == 4)
								comInfo.Vacant_num_NoObstacle[statIndex] ++;
							else if(type_id==7)
								comInfo.Vacant_num_ElectricGreen[statIndex]++;
							else 
								comInfo.vacantTaxiNum_other[statIndex] ++;
						}else if(preStatus == 1 || preStatus == 9){
							if(type_id == 1)
							    comInfo.occupyTaxiNum_red[statIndex]++;
							else if(type_id == 2)
								comInfo.occupyTaxiNum_green[statIndex]++;
							else if(type_id == 3)
								comInfo.occupy_num_electric[statIndex] ++;
							else if(type_id == 4)
								comInfo.Occupy_num_NoObstacle[statIndex] ++;
							else if(type_id==7)
								comInfo.Occupy_num_ElectricGreen[statIndex]++;
							else
								comInfo.occupyTaxiNum_other[statIndex]++;
						}else if(preStatus == 2 ){
							if(type_id == 1)
							    comInfo.TaskTaxiNum_red[statIndex]++;
							else if(type_id == 2)
								comInfo.TaskTaxiNum_green[statIndex]++;
							else if(type_id == 3)
								comInfo.task_num_electric[statIndex] ++;
							else if(type_id == 4)
								comInfo.Task_num_NoObstacle[statIndex] ++;
							else if(type_id==7)
								comInfo.Task_num_ElectricGreen[statIndex]++;
							else
								comInfo.TaskTaxiNum_other[statIndex]++;
						}else{
							if(type_id == 1)
							    comInfo.otherTaxiNum_red[statIndex]++;
							else if(type_id == 2)
								comInfo.otherTaxiNum_green[statIndex]++;
							else if(type_id == 3)
								comInfo.Other_num_electric[statIndex] ++;
							else if(type_id == 4)
								comInfo.Other_num_NoObstacle[statIndex] ++;
							else if(type_id==7)
								comInfo.Other_num_ElectricGreen[statIndex]++;
							else
								comInfo.otherTaxiNum_other[statIndex]++;
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
					    comInfo.vacantTaxiNum_red[curIndex] ++;
					else if(type_id == 2)
						comInfo.vacantTaxiNum_green[curIndex] ++;
					else if(type_id == 3)
						comInfo.vacant_num_electric[curIndex] ++;
					else if(type_id == 4)
						comInfo.Vacant_num_NoObstacle[curIndex] ++;
					else if(type_id==7)
						comInfo.Vacant_num_ElectricGreen[curIndex]++;
					else
						comInfo.vacantTaxiNum_other[curIndex] ++;
				}else if(preStatus == 1){
					if(type_id == 1)
					    comInfo.occupyTaxiNum_red[curIndex]++;
					else if(type_id == 2)
						comInfo.occupyTaxiNum_green[curIndex]++;
					else if(type_id == 3)
						comInfo.occupy_num_electric[curIndex] ++;
					else if(type_id == 4)
						comInfo.Occupy_num_NoObstacle[curIndex] ++;
					else if(type_id==7)
						comInfo.Occupy_num_ElectricGreen[curIndex]++;
					else
						comInfo.occupyTaxiNum_other[curIndex]++;
				}else if(preStatus == 2){
					if(type_id == 1)
					    comInfo.TaskTaxiNum_red[curIndex]++;
					else if(type_id == 2)
						comInfo.TaskTaxiNum_green[curIndex]++;
					else if(type_id == 3)
						comInfo.task_num_electric[curIndex] ++;
					else if(type_id == 4)
						comInfo.Task_num_NoObstacle[curIndex] ++;
					else if(type_id==7)
						comInfo.Task_num_ElectricGreen[curIndex]++;
					else
						comInfo.TaskTaxiNum_other[curIndex]++;
				}else{
					if(type_id == 1)
					    comInfo.otherTaxiNum_red[curIndex]++;
					else if(type_id == 2)
						comInfo.otherTaxiNum_green[curIndex]++;
					else if(type_id == 3)
						comInfo.Other_num_electric[curIndex] ++;
					else if(type_id == 4)
						comInfo.Other_num_NoObstacle[curIndex] ++;
					else if(type_id==7)
						comInfo.Other_num_ElectricGreen[curIndex]++;
					else
						comInfo.otherTaxiNum_other[curIndex]++;
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
			int totalNum,occupyNum_green,vacantNum_green,taskNum_green,otherNum_green,occupyNum_red,vacantNum_red,taskNum_red,otherNum_red,occupyNum_other,vacantNum_other,taskNum_other,otherNum_other,count,vacant_num_electric,occupy_num_electric,task_num_electric,Other_num_electric,Vacant_num_NoObstacle,Occupy_num_NoObstacle,Task_num_NoObstacle,Other_num_NoObstacle,Vacant_num_ElectricGreen,Occupy_num_ElectricGreen,Task_num_ElectricGreen,Other_num_ElectricGreen;
			ComInfo comInfo;
			int step = (int)(GeneralConst.ONE_HOUR_TIME/this.statDuration);
			StatementHandle stmt = conn.prepareStatement("insert into ana_taxi_status_hour_type(id,stat_time,company_id,company_name,vacant_num_green,occupy_num_green,task_num_green,unknown_num_green,vacant_num_red,occupy_num_red,task_num_red,unknown_num_red,vacant_num_other,occupy_num_other,task_num_other,unknown_num_other,vacant_num_electric,occupy_num_electric,task_num_electric,unknown_num_electric,Vacant_num_NoObstacle,Occupy_num_NoObstacle,Task_num_NoObstacle,Unknown_num_NoObstacle,Vacant_num_ElectricGreen,Occupy_num_ElectricGreen,Task_num_ElectricGreen,Unknown_num_ElectricGreen) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
			for(Iterator itr = this.comMapping.keySet().iterator();itr.hasNext();){
				comId = (Integer)itr.next();
				comInfo = (ComInfo)this.comMapping.get(comId);
				
				cal.setTime(sDate);
				for(int i=0;i<24;i++){
					cal.set(Calendar.HOUR_OF_DAY, i);
					
					occupyNum_green = vacantNum_green = taskNum_green = otherNum_green =occupyNum_red = vacantNum_red = taskNum_red = otherNum_red =occupyNum_other = vacantNum_other = taskNum_other = otherNum_other = vacant_num_electric=occupy_num_electric=task_num_electric=Other_num_electric=Vacant_num_NoObstacle=Occupy_num_NoObstacle=Task_num_NoObstacle=Other_num_NoObstacle=Vacant_num_ElectricGreen=Occupy_num_ElectricGreen=Task_num_ElectricGreen=Other_num_ElectricGreen=count = 0;	
					for(int x=0,index=i*step;x<step;x++,index++){
						totalNum = comInfo.occupyTaxiNum_red[index]+comInfo.occupyTaxiNum_green[index]+comInfo.occupyTaxiNum_other[index]+comInfo.Occupy_num_NoObstacle[index]+comInfo.occupy_num_electric[index]+comInfo.Occupy_num_ElectricGreen[index]
								+comInfo.vacantTaxiNum_red[index]+comInfo.vacantTaxiNum_green[index]+comInfo.vacantTaxiNum_other[index]+comInfo.Vacant_num_NoObstacle[index]+comInfo.vacant_num_electric[index]+comInfo.Vacant_num_ElectricGreen[index]
								+comInfo.TaskTaxiNum_red[index]+comInfo.TaskTaxiNum_green[index]+comInfo.TaskTaxiNum_other[index]+comInfo.Task_num_NoObstacle[index]+comInfo.task_num_electric[index]+comInfo.Task_num_ElectricGreen[index]
								+comInfo.otherTaxiNum_red[index]+comInfo.otherTaxiNum_green[index]+comInfo.otherTaxiNum_other[index]+comInfo.Other_num_electric[index]+comInfo.Other_num_NoObstacle[index]+comInfo.Other_num_ElectricGreen[index];
						        
						
						if(totalNum > 0){
							occupyNum_green += comInfo.occupyTaxiNum_green[index];
							vacantNum_green += comInfo.vacantTaxiNum_green[index];
							taskNum_green += comInfo.TaskTaxiNum_green[index];
							otherNum_green += comInfo.otherTaxiNum_green[index];
							occupyNum_red += comInfo.occupyTaxiNum_red[index];
							vacantNum_red += comInfo.vacantTaxiNum_red[index];
							taskNum_red += comInfo.TaskTaxiNum_red[index];
							otherNum_red += comInfo.otherTaxiNum_red[index];
							occupyNum_other += comInfo.occupyTaxiNum_other[index];
							vacantNum_other += comInfo.vacantTaxiNum_other[index];
							taskNum_other += comInfo.TaskTaxiNum_other[index];
							otherNum_other += comInfo.otherTaxiNum_other[index];
							occupy_num_electric +=comInfo.occupy_num_electric[index];
							vacant_num_electric +=comInfo.vacant_num_electric[index];
							task_num_electric +=comInfo.task_num_electric[index];
							Other_num_electric +=comInfo.Other_num_electric[index];
							Occupy_num_NoObstacle +=comInfo.Occupy_num_NoObstacle[index];
							Vacant_num_NoObstacle +=comInfo.Vacant_num_NoObstacle[index];
							Task_num_NoObstacle +=comInfo.Task_num_NoObstacle[index];
							Other_num_NoObstacle +=comInfo.Other_num_NoObstacle[index];
							Vacant_num_ElectricGreen +=comInfo.Vacant_num_ElectricGreen[index];
							Occupy_num_ElectricGreen +=comInfo.Occupy_num_ElectricGreen[index];
							Task_num_ElectricGreen +=comInfo.Task_num_ElectricGreen[index];
							Other_num_ElectricGreen +=comInfo.Other_num_ElectricGreen[index];
							count ++;
						}
					}
					
					if(count > 0){
						stmt.setInt(1, (int)DbServer.getSingleInstance().getAvaliableId(conn, "ana_taxi_status_hour_type", "id"));
						stmt.setTimestamp(2, new Timestamp(cal.getTimeInMillis()));
						stmt.setInt(3, comId);
						stmt.setString(4, comInfo.companyName);
						stmt.setInt(5, vacantNum_green/count);
						stmt.setInt(6, occupyNum_green/count);
						stmt.setInt(7, taskNum_green/count);
						stmt.setInt(8, otherNum_green/count);
						stmt.setInt(9, vacantNum_red/count);
						stmt.setInt(10, occupyNum_red/count);
						stmt.setInt(11, taskNum_red/count);
						stmt.setInt(12, otherNum_red/count);
						stmt.setInt(13, vacantNum_other/count);
						stmt.setInt(14, occupyNum_other/count);
						stmt.setInt(15, taskNum_other/count);
						stmt.setInt(16, otherNum_other/count);
						
						stmt.setInt(17, vacant_num_electric/count);
						stmt.setInt(18, occupy_num_electric/count);
						stmt.setInt(19, task_num_electric/count);
						stmt.setInt(20, Other_num_electric/count);
						stmt.setInt(21, Vacant_num_NoObstacle/count);
						stmt.setInt(22, Occupy_num_NoObstacle/count);
						stmt.setInt(23, Task_num_NoObstacle/count);
						stmt.setInt(24, Other_num_NoObstacle/count);
						stmt.setInt(25, Vacant_num_ElectricGreen/count);
						stmt.setInt(26, Occupy_num_ElectricGreen/count);
						stmt.setInt(27, Task_num_ElectricGreen/count);
						stmt.setInt(28, Other_num_ElectricGreen/count);
						
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
		System.out.println("Finish Analysis:"+this.toString()+" recordNum="+recordNum);
	}
	
	public String toString(){
		return "StatusAnalysisForHourFull";
	}
	
	private class ComInfo{
		public String companyName = null;
		
		public int[] occupyTaxiNum_green = new int[(int)(GeneralConst.ONE_DAY_TIME/statDuration)];
		public int[] vacantTaxiNum_green = new int[(int)(GeneralConst.ONE_DAY_TIME/statDuration)];
		public int[] TaskTaxiNum_green = new int[(int)(GeneralConst.ONE_DAY_TIME/statDuration)];
		public int[] otherTaxiNum_green = new int[(int)(GeneralConst.ONE_DAY_TIME/statDuration)];
		public int[] occupyTaxiNum_red = new int[(int)(GeneralConst.ONE_DAY_TIME/statDuration)];
		public int[] vacantTaxiNum_red = new int[(int)(GeneralConst.ONE_DAY_TIME/statDuration)];
		public int[] TaskTaxiNum_red = new int[(int)(GeneralConst.ONE_DAY_TIME/statDuration)];
		public int[] otherTaxiNum_red = new int[(int)(GeneralConst.ONE_DAY_TIME/statDuration)];
		public int[] occupyTaxiNum_other = new int[(int)(GeneralConst.ONE_DAY_TIME/statDuration)];
		public int[] vacantTaxiNum_other = new int[(int)(GeneralConst.ONE_DAY_TIME/statDuration)];
		public int[] TaskTaxiNum_other = new int[(int)(GeneralConst.ONE_DAY_TIME/statDuration)];
		public int[] otherTaxiNum_other = new int[(int)(GeneralConst.ONE_DAY_TIME/statDuration)];
		public int[] Vacant_num_NoObstacle = new int[(int)(GeneralConst.ONE_DAY_TIME/statDuration)];
		public int[] Occupy_num_NoObstacle = new int[(int)(GeneralConst.ONE_DAY_TIME/statDuration)];
		public int[] Task_num_NoObstacle = new int[(int)(GeneralConst.ONE_DAY_TIME/statDuration)];
		public int[] Other_num_NoObstacle = new int[(int)(GeneralConst.ONE_DAY_TIME/statDuration)];
		public int[] Other_num_electric = new int[(int)(GeneralConst.ONE_DAY_TIME/statDuration)];
		public int[] task_num_electric = new int[(int)(GeneralConst.ONE_DAY_TIME/statDuration)];
		public int[] occupy_num_electric = new int[(int)(GeneralConst.ONE_DAY_TIME/statDuration)];
		public int[] vacant_num_electric = new int[(int)(GeneralConst.ONE_DAY_TIME/statDuration)];
		public int[] Vacant_num_ElectricGreen = new int[(int)(GeneralConst.ONE_DAY_TIME/statDuration)];
		public int[] Occupy_num_ElectricGreen = new int[(int)(GeneralConst.ONE_DAY_TIME/statDuration)];
		public int[] Task_num_ElectricGreen = new int[(int)(GeneralConst.ONE_DAY_TIME/statDuration)];
		public int[] Other_num_ElectricGreen = new int[(int)(GeneralConst.ONE_DAY_TIME/statDuration)];
		
	}

}
