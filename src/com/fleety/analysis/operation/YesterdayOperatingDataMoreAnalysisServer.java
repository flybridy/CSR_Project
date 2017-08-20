package com.fleety.analysis.operation;

import java.sql.ResultSet;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import server.track.TrackServer;

import com.fleety.analysis.AnalysisServer;
import com.fleety.analysis.operation.task.VehicleOperateDataAnalysisForDaySZ_waittime;
import com.fleety.base.GeneralConst;
import com.fleety.base.InfoContainer;
import com.fleety.base.StrFilter;
import com.fleety.util.pool.thread.BasicTask;
import com.fleety.util.pool.timer.FleetyTimerTask;

public class YesterdayOperatingDataMoreAnalysisServer extends AnalysisServer {
	private TimerTask task = null;
	private ArrayList taskList = new ArrayList(4);
	private static YesterdayOperatingDataMoreAnalysisServer instance = null;
	private Date start_day = null;
	private Date end_day = null;
	
	private YesterdayOperatingDataMoreAnalysisServer (){}
	public static YesterdayOperatingDataMoreAnalysisServer getSingleInstance(){
		if(instance == null){
			instance = new YesterdayOperatingDataMoreAnalysisServer();
		}
		return instance;
	}
	
	@Override
	public boolean startServer() {
		this.isRunning = super.startServer();
		
		System.out.println("分析等候时长数据");
		int hour = this.getIntegerPara("hour").intValue();
		int minute = this.getIntegerPara("minute").intValue();
		
		String temp  = this.getStringPara("start_day");
		
		try{

			if(StrFilter.hasValue(temp)){
				start_day = GeneralConst.YYYY_MM_DD.parse(temp);
			}
			
			temp  = this.getStringPara("end_day");
			if(StrFilter.hasValue(temp)){
				end_day = GeneralConst.YYYY_MM_DD.parse(temp);
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
//					taskList.add((IOperationAnalysis)Class.forName(itr.next().toString()).newInstance());
					taskList.add(itr.next().toString());
				}
			}else{
				taskList.add(obj.toString());
			}
		}catch(Exception e){
			e.printStackTrace();
			this.isRunning = false;
			return false;
		}
		
		
		boolean runnext = true;
		Calendar cal1 = Calendar.getInstance();
		cal1.setTime(start_day);
		
		long delay = 100;
		while(runnext){
			if(start_day==null || end_day==null || cal1.getTimeInMillis()>end_day.getTime()){
				runnext = false;
				this.task=null;
				System.out.println("多日分析退出");
				break ;
			}
			
			
			System.out.println("Fire ExecTask YesterdayOperatingDataMoreAnalysisServer:"+GeneralConst.YYYY_MM_DD.format(cal1.getTime()));
			
			Calendar cal2 = Calendar.getInstance();//局部变量
			cal2.setTime(cal1.getTime());
			this.addExecTask(new ExecTask(cal2));
			cal1.add(Calendar.DATE, 1);
//			start_day = cal1.getTime();
			
			
			
		}
		return this.isRunning();
	}

	public void stopServer(){
		if(this.task != null){
			this.task.cancel();
		}
		super.stopServer();
	}
	
	private void executeTask(Calendar anaDate) throws Exception{
		
		Date sDate = anaDate.getTime();
		
		System.out.println("more Exec Task day:"+GeneralConst.YYYY_MM_DD_HH_MM_SS.format(sDate));
		anaDate.setTimeInMillis(sDate.getTime()+GeneralConst.ONE_DAY_TIME-1000);
		Date eDate = anaDate.getTime();
		System.out.println("more Exec Task day:"+GeneralConst.YYYY_MM_DD_HH_MM_SS.format(sDate)+",endtime:"+GeneralConst.YYYY_MM_DD_HH_MM_SS.format(eDate));
		
		InfoContainer statInfo = new InfoContainer();
		statInfo.setInfo(IOperationAnalysis.STAT_START_TIME_DATE, sDate);
		statInfo.setInfo(IOperationAnalysis.STAT_END_TIME_DATE, eDate);

		ArrayList execList = new ArrayList(this.taskList.size());
		IOperationAnalysis analysis;
		for(int i=0;i<this.taskList.size();i++){
			String classname = (String) this.taskList.get(i);
			analysis = (IOperationAnalysis) Class.forName(classname).newInstance();
			if(analysis.startAnalysisOperation(this,statInfo)){
				execList.add(analysis);
			}
		}
		
//		analysis = new VehicleOperateDataAnalysisForDaySZ_waittime();
//		if(analysis.startAnalysisOperation(this,statInfo)){
//			execList.add(analysis);
//			}
		
		
		System.out.println("YesterdayOperatingDataMoreAnalysisServer Exec Task Num:"+execList.size());

		
		if(execList.size() > 0){
			for(int i=0;i<execList.size();i++){
				analysis = (IOperationAnalysis)execList.get(i);
				try{
					analysis.analysisDestOperation(this, statInfo);
				}catch(Exception e){
					e.printStackTrace();
					System.out.println("Analysis Failure:"+analysis.toString());
				}
			}
			
	
			for(int i=0;i<execList.size();i++){
				analysis = (IOperationAnalysis)execList.get(i);
				analysis.endAnalysisOperation(this,statInfo);
			}
		}
		
		System.out.println("Exec Finished for day ");
		
	}

	private class TimerTask extends FleetyTimerTask{
		
		private Calendar cal1 = null ;
		
		public TimerTask( Calendar anaday){
			this.cal1 = anaday;
			
		}
		
		
		public void run(){
			System.out.println("add exectask:"+GeneralConst.YYYY_MM_DD.format(cal1.getTime()));
			YesterdayOperatingDataMoreAnalysisServer.this.addExecTask(new ExecTask(cal1));
		}
	}
	
	private class ExecTask extends BasicTask{
		private Calendar anaDate = null;
		public ExecTask(Calendar anaDate){
			this.anaDate = anaDate;
			System.out.println("YesterdayOperatingDataMoreAnalysisServer ExecTask anaDate :"+GeneralConst.YYYY_MM_DD_HH_MM_SS.format(this.anaDate.getTime())+",参数值:"+GeneralConst.YYYY_MM_DD_HH_MM_SS.format(anaDate.getTime()));
		}
		
		public boolean execute() throws Exception{
			
			System.out.println("YesterdayOperatingDataMoreAnalysisServer execute ExecTask:"+GeneralConst.YYYY_MM_DD.format(anaDate.getTime()));
			YesterdayOperatingDataMoreAnalysisServer.this.executeTask(this.anaDate);
//			System.out.println("YesterdayOperatingDataMoreAnalysisServer execute ExecTask:"+GeneralConst.YYYY_MM_DD.format(anaDate.getTime()));
			return true;
		}
		
		public String getDesc(){
			return "昨日营运数据数据分析多天";
		}
		public Object getFlag(){
			int val =(int) (Math.random()*50+1);
			return "YesterdayOperatingDataMoreAnalysisServer"+val;
		}
	}
}
