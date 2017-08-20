package com.fleety.analysis.operation;

import java.sql.ResultSet;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import server.track.TrackServer;

import com.fleety.analysis.AnalysisServer;
import com.fleety.base.GeneralConst;
import com.fleety.base.InfoContainer;
import com.fleety.util.pool.thread.BasicTask;
import com.fleety.util.pool.timer.FleetyTimerTask;

public class YesterdayOperatingDataPrepareAnalysisServer extends AnalysisServer {
	private TimerTask task = null;
	private ArrayList taskList = new ArrayList(4);
	private static YesterdayOperatingDataPrepareAnalysisServer instance = null;
	private int num = 5;
	
	private YesterdayOperatingDataPrepareAnalysisServer (){}
	public static YesterdayOperatingDataPrepareAnalysisServer getSingleInstance(){
		if(instance == null){
			instance = new YesterdayOperatingDataPrepareAnalysisServer();
		}
		return instance;
	}
	
	@Override
	public boolean startServer() {
		this.isRunning = super.startServer();
		
		int hour = this.getIntegerPara("hour").intValue();
		int minute = this.getIntegerPara("minute").intValue();
		num = this.getIntegerPara("num").intValue();
		
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
		anaDate.setTimeInMillis(anaDate.getTimeInMillis()+GeneralConst.ONE_DAY_TIME-1000);
		Date eDate = anaDate.getTime();
		
		InfoContainer statInfo = new InfoContainer();
		statInfo.setInfo(IOperationAnalysis.STAT_START_TIME_DATE, sDate);
		statInfo.setInfo(IOperationAnalysis.STAT_END_TIME_DATE, eDate);

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
			boolean temp = true;
			int i = 0;
			Calendar cal1 = null;
			while(temp){
				cal1 = Calendar.getInstance();
				cal1.setTimeInMillis(cal.getTimeInMillis());
				cal1.add(Calendar.DAY_OF_MONTH, -i);
				System.out.println("Fire ExecTask YesterdayOperatingDataPrepareAnalysisServer:"+GeneralConst.YYYY_MM_DD_HH.format(cal1.getTime()));
				YesterdayOperatingDataPrepareAnalysisServer.this.addExecTask(new ExecTask(cal1));
				i++;
				if(i == num){
					temp = false;
				}
			}
		}
	}
	private class ExecTask extends BasicTask{
		private Calendar anaDate = null;
		public ExecTask(Calendar anaDate){
			this.anaDate = anaDate;
		}
		
		public boolean execute() throws Exception{
			YesterdayOperatingDataPrepareAnalysisServer.this.executeTask(this.anaDate);
			return true;
		}
		
		public String getDesc(){
			return "昨日营运数据数据多天准备";
		}
		public Object getFlag(){
			return "YesterdayOperatingDataPrepareAnalysisServer";
		}
	}
}
