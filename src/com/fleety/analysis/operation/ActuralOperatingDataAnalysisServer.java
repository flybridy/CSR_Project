package com.fleety.analysis.operation;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import server.db.DbServer;
import server.track.TrackServer;

import com.fleety.analysis.AnalysisServer;
import com.fleety.analysis.track.DestInfo;
import com.fleety.analysis.track.ITrackAnalysis;
import com.fleety.analysis.track.TrackInfo;
import com.fleety.base.GeneralConst;
import com.fleety.base.InfoContainer;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.fleety.util.pool.thread.BasicTask;
import com.fleety.util.pool.timer.FleetyTimerTask;

public class ActuralOperatingDataAnalysisServer extends AnalysisServer{

	private TimerTask task = null;
	private ArrayList taskList = new ArrayList(4);
	public int circleTime = 10;
	private static ActuralOperatingDataAnalysisServer instance = null;
	
	private ActuralOperatingDataAnalysisServer (){}
	public static ActuralOperatingDataAnalysisServer getSingleInstance(){
		if(instance == null){
			instance = new ActuralOperatingDataAnalysisServer();
		}
		return instance;
	}
	
	public boolean startServer() {
		this.isRunning = super.startServer();
		
		circleTime = this.getIntegerPara("circle_time").intValue();
		
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

		long period = circleTime * 60 * 1000;
		this.isRunning = this.scheduleTask(this.task = new TimerTask(), 5000, period);
		
		
		return this.isRunning();
	}

	public void stopServer(){
		if(this.task != null){
			this.task.cancel();
		}
		super.stopServer();
	}
	
	private void executeTask(Calendar anaDate) throws Exception{
		ArrayList destList = new ArrayList(1024);
		DbHandle conn = DbServer.getSingleInstance().getConn();
		DestInfo dInfo;
		try{
			StatementHandle stmt = conn.prepareStatement("select mdt_id,dest_no,company_id,company_name from v_ana_dest_info");
			ResultSet sets = stmt.executeQuery();
			while(sets.next()){
				dInfo = new DestInfo();
				dInfo.mdtId = sets.getInt("mdt_id");
				dInfo.destNo = sets.getString("dest_no");
				dInfo.companyId = sets.getInt("company_id");
				dInfo.companyName = sets.getString("company_name");
				destList.add(dInfo);
			}
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
		
		Date eDate = anaDate.getTime();
		anaDate.setTimeInMillis(anaDate.getTimeInMillis()-GeneralConst.ONE_DAY_TIME/2);
		Date sDate = anaDate.getTime();
		
		InfoContainer statInfo = new InfoContainer();
		statInfo.setInfo(ITrackAnalysis.STAT_START_TIME_DATE, sDate);
		statInfo.setInfo(ITrackAnalysis.STAT_END_TIME_DATE, eDate);
		statInfo.setInfo(ITrackAnalysis.STAT_DEST_NUM_INTEGER, new Integer(destList.size()));

		System.out.println("Start current Exec:"+GeneralConst.YYYY_MM_DD.format(sDate)+" "+GeneralConst.YYYY_MM_DD.format(eDate)+" "+destList.size());
		
		ArrayList execList = new ArrayList(this.taskList.size());
		ITrackAnalysis analysis;
		for(int i=0;i<this.taskList.size();i++){
			analysis = (ITrackAnalysis)this.taskList.get(i);
			if(analysis.startAnalysisTrack(this,statInfo)){
				execList.add(analysis);
			}
		}
		System.out.println("Exec Task Num:"+execList.size());
		if(execList.size() > 0){
			InfoContainer queryInfo = new InfoContainer();
			queryInfo.setInfo(TrackServer.START_DATE_FLAG,sDate);
			queryInfo.setInfo(TrackServer.END_DATE_FLAG, eDate);
			TrackInfo trackInfo;
			for(Iterator itr = destList.iterator();itr.hasNext();){
				dInfo = (DestInfo)itr.next();
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
			}
			
	
			for(int i=0;i<execList.size();i++){
				analysis = (ITrackAnalysis)execList.get(i);
				analysis.endAnalysisTrack(this,statInfo);
			}
		}
		
		System.out.println("Exec Finished");
	}

	private class TimerTask extends FleetyTimerTask{
		public void run(){
			Calendar cal = Calendar.getInstance();			
			System.out.println("Fire ExecTask ActuralOperatingDataAnalysisServer:"+GeneralConst.YYYY_MM_DD_HH.format(cal.getTime()));
			ActuralOperatingDataAnalysisServer.this.addExecTask(new ExecTask(cal));
		}
	}
	private class ExecTask extends BasicTask{
		private Calendar anaDate = null;
		public ExecTask(Calendar anaDate){
			this.anaDate = anaDate;
		}
		
		public boolean execute() throws Exception{
			ActuralOperatingDataAnalysisServer.this.executeTask(this.anaDate);
			return true;
		}
		
		public String getDesc(){
			return "实时数据分析";
		}
		public Object getFlag(){
			return "ActuralOperatingDataAnalysisServer";
		}
	}
}
