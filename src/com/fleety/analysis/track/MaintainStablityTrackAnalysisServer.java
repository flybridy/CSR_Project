package com.fleety.analysis.track;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import server.db.DbServer;
import server.track.TrackServer;

import com.fleety.analysis.AnalysisServer;
import com.fleety.base.GeneralConst;
import com.fleety.base.InfoContainer;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.fleety.util.pool.thread.BasicTask;
import com.fleety.util.pool.timer.FleetyTimerTask;

public class MaintainStablityTrackAnalysisServer extends AnalysisServer {
	private TimerTask task = null;
	private ArrayList taskList = new ArrayList(4);
	public int preTime = 5;
	public int preDays = 5;
	private static MaintainStablityTrackAnalysisServer instance = null;
	
	private MaintainStablityTrackAnalysisServer (){}
	public static MaintainStablityTrackAnalysisServer getSingleInstance(){
		if(instance == null){
			instance = new MaintainStablityTrackAnalysisServer();
		}
		return instance;
	}
	
	@Override
	public boolean startServer() {
		this.isRunning = super.startServer();
		
		int hour = this.getIntegerPara("hour").intValue();
		int minute = this.getIntegerPara("minute").intValue();
		preTime = this.getIntegerPara("key_area_stat_pre_time").intValue();
		preDays = this.getIntegerPara("preDays").intValue();
		
		Object obj = this.getPara("task_class");
		if(obj == null){
			this.isRunning = true;
			return this.isRunning();
		}
		
		try{
			if(obj instanceof List){
				for(Iterator itr = ((List)obj).iterator();itr.hasNext();){
					taskList.add((ITrackAnalysis)Class.forName(itr.next().toString()).newInstance());
				}
			}else{
				taskList.add((ITrackAnalysis)Class.forName(obj.toString()).newInstance());
			}
		}catch(Exception e){
			e.printStackTrace();
			this.isRunning = false;
			return false;
		}
		Calendar cal = this.getNextExecCalendar(hour, minute);
		
		if(cal.get(Calendar.DAY_OF_MONTH) != Calendar.getInstance().get(Calendar.DAY_OF_MONTH)){
			this.scheduleTask(new TimerTask(), 500);
		}
		
		long delay = cal.getTimeInMillis() - System.currentTimeMillis();
		this.isRunning = this.scheduleTask(this.task = new TimerTask(), delay, GeneralConst.ONE_DAY_TIME);
		
		return this.isRunning();
	}

	public void stopServer(){
		super.stopServer();
	}
	private class TimerTask extends FleetyTimerTask{
		public void run(){
			Calendar cal = Calendar.getInstance();
			cal.add(Calendar.DAY_OF_MONTH, -1);
			cal.set(Calendar.HOUR_OF_DAY, 0);
			cal.set(Calendar.MINUTE, 0);
			cal.set(Calendar.SECOND, 0);
			cal.set(Calendar.MILLISECOND, 0);
			
			Calendar cal1 = null;
			for (int j = 0; j < preDays; j++) {
				cal1 = Calendar.getInstance();
				cal1.setTimeInMillis(cal.getTimeInMillis());
				if(cal.getTimeInMillis()>=new Date().getTime()){
					break;
				}
				System.out.println("Fire ExecTask MaintainStablityTrackAnalysisServer "+j+":"+GeneralConst.YYYY_MM_DD_HH.format(cal1.getTime())+" "+GeneralConst.YYYY_MM_DD_HH.format(cal.getTime()));
				MaintainStablityTrackAnalysisServer.this.addExecTask(new ExecTask(cal1));
				cal.add(Calendar.DAY_OF_MONTH, -1);
			}
		}
	}
	
	private void executeTask(Calendar anaDate) throws Exception{
		ArrayList destList = new ArrayList(1024);
		DbHandle conn = DbServer.getSingleInstance().getConn();
		DestInfo dInfo;
		try{
			StatementHandle stmt = conn.prepareStatement("select mdt_id,dest_no,company_id,company_name,type_id,gps_run_com_id,gps_run_com_name from v_ana_dest_info");
			ResultSet sets = stmt.executeQuery();
			while(sets.next()){
				dInfo = new DestInfo();
				dInfo.mdtId = sets.getInt("mdt_id");
				dInfo.destNo = sets.getString("dest_no");
				dInfo.companyId = sets.getInt("company_id");
				dInfo.companyName = sets.getString("company_name");
				dInfo.gpsRunComId = sets.getInt("gps_run_com_id");
				dInfo.gpsRunComName = sets.getString("gps_run_com_name");
				dInfo.carType=sets.getInt("type_id");
				destList.add(dInfo);
			}
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
		
		Date sDate = anaDate.getTime();
		anaDate.set(Calendar.HOUR_OF_DAY, 0);
		anaDate.setTimeInMillis(anaDate.getTimeInMillis()+GeneralConst.ONE_DAY_TIME-1000);
		Date eDate = anaDate.getTime();
		
		InfoContainer statInfo = new InfoContainer();
		statInfo.setInfo(ITrackAnalysis.STAT_START_TIME_DATE, sDate);
		statInfo.setInfo(ITrackAnalysis.STAT_END_TIME_DATE, eDate);
		statInfo.setInfo(ITrackAnalysis.STAT_DEST_NUM_INTEGER, new Integer(destList.size()));

		System.out.println("Start Exec:"+GeneralConst.YYYY_MM_DD_HH.format(sDate)+" "+GeneralConst.YYYY_MM_DD_HH.format(eDate)+" "+destList.size());
		
		ArrayList execList = new ArrayList(this.taskList.size());
		ITrackAnalysis analysis;
		for(int i=0;i<this.taskList.size();i++){
			analysis = (ITrackAnalysis)this.taskList.get(i);
			if(analysis.startAnalysisTrack(this,statInfo)){
				execList.add(analysis);
			}
		}
		StringBuffer strBuff =  new StringBuffer(256);
		strBuff.append("Exec Task Num:"+execList.size());
		for(Iterator itr = execList.iterator();itr.hasNext();){
			strBuff.append("\n"+itr.next().getClass().getName());
		}
		System.out.println(strBuff);
		
		if(execList.size() > 0){
			InfoContainer queryInfo = new InfoContainer();
			queryInfo.setInfo(TrackServer.START_DATE_FLAG,sDate);
			queryInfo.setInfo(TrackServer.END_DATE_FLAG, eDate);
			TrackInfo trackInfo;
			int totalCarNum=destList.size();
			int finishNum = 0;
			for(Iterator itr = destList.iterator();itr.hasNext();){
				dInfo = (DestInfo)itr.next();
				dInfo.totalCarNum=totalCarNum;
				queryInfo.setInfo(TrackServer.DEST_NO_FLAG, dInfo.destNo);

				trackInfo = new TrackInfo();
				trackInfo.dInfo = dInfo;
				trackInfo.sDate = sDate;
				trackInfo.eDate = eDate;
				trackInfo.trackArr = TrackServer.getSingleInstance().getTrackInfo(queryInfo);
	
				for(int i=0;i<execList.size();i++){
					analysis = (ITrackAnalysis)execList.get(i);
					try{
						analysis.analysisDestTrack(this, trackInfo);
					}catch(Exception e){
						e.printStackTrace();
						System.out.println("Analysis Failure:"+analysis.toString());
					}
				}
				
				finishNum ++;
				
				if((finishNum%100) == 0){
					System.out.println("AnalysisProcess:"+finishNum+"/"+destList.size());
				}
			}
			
	
			for(int i=0;i<execList.size();i++){
				analysis = (ITrackAnalysis)execList.get(i);
				analysis.endAnalysisTrack(this,statInfo);
			}
		}
		
		System.out.println("Exec Finished");
	}

	private class ExecTask extends BasicTask{
		private Calendar anaDate = null;
		public ExecTask(Calendar anaDate){
			this.anaDate = anaDate;
		}
		
		public boolean execute() throws Exception{
			MaintainStablityTrackAnalysisServer.this.executeTask(this.anaDate);
			return true;
		}
		
		public String getDesc(){
			return "维稳轨迹轨迹数据分析(多天分析)";
		}
		public Object getFlag(){
			return "MaintainStablityTrackAnalysisServer";
		}
	}
}
