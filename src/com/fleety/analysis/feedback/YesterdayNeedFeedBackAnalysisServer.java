package com.fleety.analysis.feedback;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import com.fleety.analysis.AnalysisServer;
import com.fleety.base.GeneralConst;
import com.fleety.util.pool.thread.BasicTask;
import com.fleety.util.pool.timer.FleetyTimerTask;

public class YesterdayNeedFeedBackAnalysisServer extends AnalysisServer {
	private TimerTask task = null;
	private ArrayList taskList = new ArrayList(4);
	private static YesterdayNeedFeedBackAnalysisServer instance = null;
	private YesterdayNeedFeedBackAnalysisServer(){}
	
	public static YesterdayNeedFeedBackAnalysisServer getSingleInstance(){
		if(instance==null){
			instance = new YesterdayNeedFeedBackAnalysisServer();
		}
		return instance;
	}
	
	public boolean startServer() {
		this.isRunning = super.startServer();
		int hour = this.getIntegerPara("hour").intValue();
		int minute = this.getIntegerPara("minute").intValue();
		Object obj = this.getPara("task_class");
		if(obj == null){
			this.isRunning = true;
			return this.isRunning();
		}
		
		try{
			if(obj instanceof List){
				for(Iterator itr = ((List)obj).iterator();itr.hasNext();){
					taskList.add((FeedBackAnalysis)Class.forName(itr.next().toString()).newInstance());
				}
			}else{
				taskList.add((FeedBackAnalysis)Class.forName(obj.toString()).newInstance());
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
		
		return this.isRunning;
	}

	public void stopServer() {
		if(this.task != null){
			this.task.cancel();
		}
		super.stopServer();
	}
	private void executeTask(Calendar anaDate) throws Exception{
		Date sDate = anaDate.getTime();
		anaDate.setTimeInMillis(anaDate.getTimeInMillis()+GeneralConst.ONE_DAY_TIME-1000);
		Date eDate = anaDate.getTime();
		System.out.println("Start Exec:"+GeneralConst.YYYY_MM_DD.format(sDate)+" "+GeneralConst.YYYY_MM_DD.format(eDate));
		
		ArrayList execList = new ArrayList(this.taskList.size());
		FeedBackAnalysis analysis;
		for(int i=0;i<this.taskList.size();i++){
			analysis = (FeedBackAnalysis)this.taskList.get(i);
			if(analysis.startAnalysisFeedBack(this,sDate,eDate)){
				execList.add(analysis);
			}
		}
		for(int i=0;i<execList.size();i++){
			analysis = (FeedBackAnalysis)execList.get(i);
			analysis.analysisDestFeedBack(this,sDate,eDate);
		}
		System.out.println("Exec Task Num:"+execList.size());
		if(execList.size() > 0){
			
			for(int i=0;i<execList.size();i++){
				analysis = (FeedBackAnalysis)execList.get(i);
				analysis.endAnalysisFeedBack(this,sDate,eDate);
			}
		}
		
		System.out.println("Exec Finished");
	}
	private class TimerTask extends FleetyTimerTask{
		public void run(){
			Calendar cal = Calendar.getInstance();
			cal.add(Calendar.DAY_OF_MONTH, -1);
			cal.set(Calendar.HOUR_OF_DAY, 0);
			cal.set(Calendar.MINUTE, 0);
			cal.set(Calendar.SECOND, 0);
			cal.set(Calendar.MILLISECOND, 0);
			
			System.out.println("Fire ExecTask YesterdayNeedFeedBackAnalysisServer:"+GeneralConst.YYYY_MM_DD_HH.format(cal.getTime()));
			YesterdayNeedFeedBackAnalysisServer.this.addExecTask(new ExecTask(cal));
		}
	}
	private class ExecTask extends BasicTask{
		private Calendar anaDate = null;
		public ExecTask(Calendar anaDate){
			this.anaDate = anaDate;
		}
		
		public boolean execute() throws Exception{
			YesterdayNeedFeedBackAnalysisServer.this.executeTask(this.anaDate);
			return true;
		}
		
		public String getDesc(){
			return "昨日需反馈数据分析";
		}
		public Object getFlag(){
			return "YesterdayNeedFeedBackAnalysisServer";
		}
	}
	protected Calendar getNextExecCalendar(int hour,int minute){
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.HOUR_OF_DAY, hour);
		cal.set(Calendar.MINUTE, minute);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		
		if(cal.getTimeInMillis() < System.currentTimeMillis()){
			cal.add(Calendar.DAY_OF_MONTH, 1);
		}
		
		return cal;
	}
}
