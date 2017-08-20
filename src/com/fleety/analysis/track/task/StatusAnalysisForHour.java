package com.fleety.analysis.track.task;

import java.sql.ResultSet;
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

public class StatusAnalysisForHour implements ITrackAnalysis {
	private HashMap comMapping = null;
	private int statDuration = 10*60*1000;
	
	@Override
	public boolean startAnalysisTrack(AnalysisServer parentServer,InfoContainer statInfo) {
		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
		Date eTime = statInfo.getDate(STAT_END_TIME_DATE);
		
		this.comMapping = null;
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try{
			StatementHandle stmt = conn.prepareStatement("select * from ana_taxi_status_hour_stat where stat_time between ? and ?");
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
	public void analysisDestTrack(AnalysisServer parentServer, TrackInfo trackInfo) {
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
		for(int i=0;i<trackInfo.trackArr.length;i++){
			time.setTime(trackInfo.trackArr[i].getDate(TrackIO.DEST_TIME_FLAG));
			curIndex = (int)((time.getTimeInMillis() - sTime)/statDuration);
			status = trackInfo.trackArr[i].getInteger(TrackIO.DEST_STATUS_FLAG).intValue()&0x0F;
			
			if(curIndex > statIndex){
				if(preTime > 0){
					time.setTimeInMillis(sTime + (statIndex+1)*this.statDuration);
					
					if(time.getTimeInMillis() - preTime < limitDuration){
						if(preStatus == 0|| preStatus == 8){
							comInfo.vacantTaxiNum[statIndex] ++;
						}else if(preStatus == 1 || preStatus == 9){
							comInfo.occupyTaxiNum[statIndex]++;
						}else if(preStatus == 2 ){
							comInfo.TaskTaxiNum[statIndex]++;
						}else{
							comInfo.otherTaxiNum[statIndex]++;
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
					comInfo.vacantTaxiNum[curIndex] ++;
				}else if(preStatus == 1){
					comInfo.occupyTaxiNum[curIndex]++;
				}else if(preStatus == 2){
					comInfo.TaskTaxiNum[curIndex]++;
				}else{
					comInfo.otherTaxiNum[curIndex]++;
				}
			}
		}
	}

	@Override
	public void endAnalysisTrack(AnalysisServer parentServer,InfoContainer statInfo) {
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
			int totalNum,occupyNum,vacantNum,taskNum,otherNum,count;
			ComInfo comInfo;
			int step = (int)(GeneralConst.ONE_HOUR_TIME/this.statDuration);
			StatementHandle stmt = conn.prepareStatement("insert into ana_taxi_status_hour_stat(id,stat_time,company_id,company_name,vacant_num,occupy_num,task_num,unknown_num) values(?,?,?,?,?,?,?,?)");
			for(Iterator itr = this.comMapping.keySet().iterator();itr.hasNext();){
				comId = (Integer)itr.next();
				comInfo = (ComInfo)this.comMapping.get(comId);
				
				cal.setTime(sDate);
				for(int i=0;i<24;i++){
					cal.set(Calendar.HOUR_OF_DAY, i+1);
					
					occupyNum = vacantNum = taskNum = otherNum = count = 0;	
					for(int x=0,index=i*step;x<step;x++,index++){
						totalNum = comInfo.occupyTaxiNum[index]+comInfo.vacantTaxiNum[index]+comInfo.TaskTaxiNum[index]+comInfo.otherTaxiNum[index];
						
						if(totalNum > 0){
							occupyNum += comInfo.occupyTaxiNum[index];
							vacantNum += comInfo.vacantTaxiNum[index];
							taskNum += comInfo.TaskTaxiNum[index];
							otherNum += comInfo.otherTaxiNum[index];
							count ++;
						}
					}
					
					if(count > 0){
						stmt.setInt(1, (int)DbServer.getSingleInstance().getAvaliableId(conn, "ana_taxi_status_hour_stat", "id"));
						stmt.setTimestamp(2, new Timestamp(cal.getTimeInMillis()));
						stmt.setInt(3, comId);
						stmt.setString(4, comInfo.companyName);
						stmt.setInt(5, vacantNum/count);
						stmt.setInt(6, occupyNum/count);
						stmt.setInt(7, taskNum/count);
						stmt.setInt(8, otherNum/count);
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
		return "StatusAnalysisForHour";
	}

	private class ComInfo{
		public String companyName = null;
		
		public int[] occupyTaxiNum = new int[(int)(GeneralConst.ONE_DAY_TIME/statDuration)];
		public int[] vacantTaxiNum = new int[(int)(GeneralConst.ONE_DAY_TIME/statDuration)];
		public int[] TaskTaxiNum = new int[(int)(GeneralConst.ONE_DAY_TIME/statDuration)];
		public int[] otherTaxiNum = new int[(int)(GeneralConst.ONE_DAY_TIME/statDuration)];
	}
}
