package com.fleety.analysis.operation;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import com.fleety.analysis.AnalysisServer;
import com.fleety.base.GeneralConst;
import com.fleety.base.InfoContainer;
import com.fleety.util.pool.thread.BasicTask;
import com.fleety.util.pool.timer.FleetyTimerTask;

public class YesterdayOperatingDataAnalysisServer extends AnalysisServer {
	private TimerTask task = null;
	private ArrayList taskList = new ArrayList(4);
	public int preTime = 5;
	private static YesterdayOperatingDataAnalysisServer instance = null;
	
	private YesterdayOperatingDataAnalysisServer (){}
	public static YesterdayOperatingDataAnalysisServer getSingleInstance(){
		if(instance == null){
			instance = new YesterdayOperatingDataAnalysisServer();
		}
		return instance;
	}
	
	@Override
	public boolean startServer() {
		this.isRunning = super.startServer();
		
		int hour = this.getIntegerPara("hour").intValue();
		int minute = this.getIntegerPara("minute").intValue();
		preTime = this.getIntegerPara("key_area_stat_pre_time").intValue();
		int preDays=this.getIntegerPara("preDays").intValue();
		Object obj = this.getPara("task_class");
		if(obj == null){
			this.isRunning = true;
			return this.isRunning();
		}
		
		try{
			if(obj instanceof List){
				for(Iterator itr = ((List)obj).iterator();itr.hasNext();){
					taskList.add((IOperationAnalysis)Class.forName(itr.next().toString()).newInstance());
				}
			}else{
				taskList.add((IOperationAnalysis)Class.forName(obj.toString()).newInstance());
			}
		}catch(Exception e){
			e.printStackTrace();
			this.isRunning = false;
			return false;
		}
		Calendar cal2 = Calendar.getInstance();
		cal2.add(Calendar.DAY_OF_MONTH, -1);
		cal2.set(Calendar.HOUR_OF_DAY, 0);
		cal2.set(Calendar.MINUTE, 0);
		cal2.set(Calendar.SECOND, 0);
		cal2.set(Calendar.MILLISECOND, 0);
		for (int j = 0; j < preDays; j++) {	
			Calendar cal1 = Calendar.getInstance();
			cal1 = Calendar.getInstance();
			cal1.setTimeInMillis(cal2.getTimeInMillis());
			if(cal2.getTimeInMillis()>=new Date().getTime()){
				break;
			}
			System.out.println("营运数据分析 添加任务:Fire ExecTask YesterdayOperatingDataAnalysisServer "+j+":"+GeneralConst.YYYY_MM_DD_HH.format(cal1.getTime())+" "+GeneralConst.YYYY_MM_DD_HH.format(cal2.getTime()));
			this.addExecTask(new ExecTask(cal1));
			cal2.add(Calendar.DAY_OF_MONTH, -1);
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
		if(this.task != null){
			this.task.cancel();
		}
		super.stopServer();
	}
	
	private void executeTask(Calendar anaDate) throws Exception{
		
		Date sDate = anaDate.getTime();
		
//		anaDate.setTimeInMillis(anaDate.getTimeInMillis()+GeneralConst.ONE_DAY_TIME-1000);
		anaDate.add(Calendar.DAY_OF_MONTH, 1);
		Date eDate = anaDate.getTime();
		
		InfoContainer statInfo = new InfoContainer();
		statInfo.setInfo(IOperationAnalysis.STAT_START_TIME_DATE, sDate);
		statInfo.setInfo(IOperationAnalysis.STAT_END_TIME_DATE, eDate);

		System.out.println("昨日营运数据分析日期："+GeneralConst.YYYY_MM_DD_HH_MM_SS.format(sDate)+","+GeneralConst.YYYY_MM_DD_HH_MM_SS.format(eDate));
		ArrayList execList = new ArrayList(this.taskList.size());
		IOperationAnalysis analysis;
		for(int i=0;i<this.taskList.size();i++){
			analysis = (IOperationAnalysis)this.taskList.get(i);
			if(analysis.startAnalysisOperation(this,statInfo)){
				execList.add(analysis);
			}
		}
		System.out.println("Exec Task Num:"+execList.size());

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
		
		System.out.println("Exec Finished :"+GeneralConst.YYYYMMDD.format(sDate));
	}

	private class TimerTask extends FleetyTimerTask{
		public void run(){
			Calendar cal = Calendar.getInstance();
			cal.add(Calendar.DAY_OF_MONTH, -1);
			cal.set(Calendar.HOUR_OF_DAY, 0);
			cal.set(Calendar.MINUTE, 0);
			cal.set(Calendar.SECOND, 0);
			cal.set(Calendar.MILLISECOND, 0);
			
			System.out.println("Fire ExecTask YesterdayTrackAnalysisServer:"+GeneralConst.YYYY_MM_DD_HH.format(cal.getTime()));
			YesterdayOperatingDataAnalysisServer.this.addExecTask(new ExecTask(cal));
		}
	}
	private class ExecTask extends BasicTask{
		private Calendar anaDate = null;
		public ExecTask(Calendar anaDate){
			this.anaDate = anaDate;
		}
		
		public boolean execute() throws Exception{
			System.out.println("昨日营运数据分析ExecTask:"+GeneralConst.YYYY_MM_DD_HH_MM_SS.format(anaDate.getTime()));
			YesterdayOperatingDataAnalysisServer.this.executeTask(this.anaDate);
			return true;
		}
		
		public String getDesc(){
			return "昨日营运数据数据分析";
		}
		public Object getFlag(){
			return "YesterdayOperatingDataAnalysisServer";
		}
	}
}
