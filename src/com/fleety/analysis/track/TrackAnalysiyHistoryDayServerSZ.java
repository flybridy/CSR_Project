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
import com.fleety.base.StrFilter;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.fleety.util.pool.thread.BasicTask;
import com.fleety.util.pool.timer.FleetyTimerTask;

public class TrackAnalysiyHistoryDayServerSZ extends AnalysisServer {
	private TimerTask task = null;
	private ArrayList taskList = new ArrayList(4);
	public String time = null;
	public Date stime = null;
	public Date etime = null;
	public int preDays = 5;
	private static TrackAnalysiyHistoryDayServerSZ instance = null;
	
	private TrackAnalysiyHistoryDayServerSZ (){}
	public static TrackAnalysiyHistoryDayServerSZ getSingleInstance(){
		if(instance == null){
			instance = new TrackAnalysiyHistoryDayServerSZ();
		}
		return instance;
	}
	
	@Override
	public boolean startServer() {
		this.isRunning = super.startServer();
		
		int hour = this.getIntegerPara("hour").intValue();
		int minute = this.getIntegerPara("minute").intValue();
		try{
			time = this.getStringPara("start_time");
			
			if(StrFilter.hasValue(time)){
						stime = GeneralConst.YYYY_MM_DD.parse(time);
						System.out.println("ss:"+time);
					}
			time=this.getStringPara("end_time");
			if(StrFilter.hasValue(time)){
				etime = GeneralConst.YYYY_MM_DD.parse(time);
				System.out.println("ee"+time);
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		
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
		
		boolean runnext = true;
		Calendar cal1 = Calendar.getInstance();
		cal1.setTime(stime);
		
		while(runnext){
			if(stime==null || etime==null || cal1.getTimeInMillis()>etime.getTime()){
				runnext = false;
				this.task=null;
				System.out.println("多日分析退出");
				break ;
			}
			System.out.println("Fire ExecTask TrackAnalysiyHistoryDayServerSZ:"+GeneralConst.YYYY_MM_DD.format(cal1.getTime()));
			this.addExecTask(new ExecTask(cal1));
			System.out.println("添加分析任务："+stime);
			cal1.add(Calendar.DAY_OF_YEAR, 1);
			stime = cal1.getTime();	
		}	
		return this.isRunning();
	}

	public void stopServer(){
		super.stopServer();
	}
	private class TimerTask extends FleetyTimerTask{
		
		

		
		private Calendar cal1 = null ;
		
		public TimerTask( Calendar anaday){
			this.cal1 = anaday;
			
		}
		
		
		public void run(){
			System.out.println("add exectask:"+GeneralConst.YYYY_MM_DD.format(cal1.getTime()));
			TrackAnalysiyHistoryDayServerSZ.this.addExecTask(new ExecTask(cal1));
		}
	}
	
	private void executeTask(Calendar anadate) throws Exception{
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
		
		Date sDate = anadate.getTime();
		anadate.set(Calendar.HOUR_OF_DAY, 0);
		anadate.setTimeInMillis(anadate.getTimeInMillis()+GeneralConst.ONE_DAY_TIME-1000);
		Date eDate = anadate.getTime();
		
		InfoContainer statInfo = new InfoContainer();
		statInfo.setInfo(ITrackAnalysis.STAT_START_TIME_DATE, sDate);
		statInfo.setInfo(ITrackAnalysis.STAT_END_TIME_DATE, eDate);
		statInfo.setInfo(ITrackAnalysis.STAT_DEST_NUM_INTEGER, new Integer(destList.size()));

		System.out.println("Start Exec:深圳历史速度分析服务开始"+GeneralConst.YYYY_MM_DD_HH_MM_SS.format(sDate)+" 到 "+GeneralConst.YYYY_MM_DD_HH_MM_SS.format(eDate)+" 车辆数"+destList.size());
		
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
				
				if((finishNum%400) == 0){
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
		private Calendar sdate = null;
		private Calendar edate = null;
		public ExecTask(Calendar asdate){
			this.sdate = asdate;
		}
		
		public boolean execute() throws Exception{
			TrackAnalysiyHistoryDayServerSZ.this.executeTask(this.sdate);
			return true;
		}
		
		public String getDesc(){
			return "深圳历史速度分析";
		}
		public Object getFlag(){
			return "TrackAnalysiyHistoryDayServerSZ";
		}
	}
}
