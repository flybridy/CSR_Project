package com.fleety.analysis;

import java.util.Calendar;

import server.threadgroup.PoolInfo;
import server.threadgroup.ThreadPoolGroupServer;
import server.var.VarManageServer;

import com.fleety.server.BasicServer;
import com.fleety.util.pool.thread.BasicTask;
import com.fleety.util.pool.thread.ThreadPool;
import com.fleety.util.pool.timer.FleetyTimerTask;
import com.fleety.util.pool.timer.TimerPool;

public abstract class AnalysisServer extends BasicServer {
	protected String ANALYSIS_TIMER_POOL = "analysis-timer-pool";
	protected String ANALYSIS_THREAD_POOL = "analysis-thread-pool";
	private TimerPool timer = null;
	private ThreadPool pool = null;
	
	@Override
	public boolean startServer() {
		String tempStr = this.getStringPara("analysis_timer_pool");
		if(tempStr != null && tempStr.trim().length() > 0){
			this.ANALYSIS_TIMER_POOL = tempStr.trim();
		}
		tempStr = this.getStringPara("analysis_thread_pool");
		if(tempStr != null && tempStr.trim().length() > 0){
			this.ANALYSIS_THREAD_POOL = tempStr.trim();
		}
		
		Integer timerThreadNum = VarManageServer.getSingleInstance().getIntegerPara("timer_thread_num");
		if(timerThreadNum == null){
			timerThreadNum = new Integer(4);
		}
		
		this.timer = ThreadPoolGroupServer.getSingleInstance().getTimerPool(ANALYSIS_TIMER_POOL);
		if(this.timer == null){
			this.timer = ThreadPoolGroupServer.getSingleInstance().createTimerPool(ANALYSIS_TIMER_POOL, timerThreadNum.intValue(), false);
		}
		
		Integer threadNum = VarManageServer.getSingleInstance().getIntegerPara("pool_thread_num");
		if(threadNum == null){
			threadNum = new Integer(10);
		}
		
		this.pool = ThreadPoolGroupServer.getSingleInstance().getThreadPool(ANALYSIS_THREAD_POOL);
		if(this.pool == null){
			PoolInfo pInfo = new PoolInfo();
			pInfo.taskCapacity = 10000;
			pInfo.workersNumber = threadNum;
			pInfo.poolType = ThreadPool.MULTIPLE_TASK_LIST_POOL;
			try{
				this.pool = ThreadPoolGroupServer.getSingleInstance().createThreadPool(ANALYSIS_THREAD_POOL, pInfo);
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		if(this.timer != null && this.pool != null){
			this.isRunning = true;
		}
		return this.isRunning();
	}
	
	public void stopServer(){
		this.timer = null;
		this.pool = null;
		super.stopServer();
	}

	/**
	 * 把某个任务放入定时执行中，如果任务执行时间较长，应该产生新的任务放置到执行任务池中进行执行
	 * @param timerTask 待周期性执行的任务
	 * @param delay     延迟执行时长，单位毫秒
	 * @param period    执行周期，单位毫秒
	 * @return
	 */
	public boolean scheduleTask(FleetyTimerTask timerTask,long delay,long period){
		if(!this.isRunning()){
			return false;
		}
		
		this.timer.schedule(timerTask, delay, period);
		return true;
	}
	
	public boolean scheduleTask(FleetyTimerTask timerTask,long delay){
		if(!this.isRunning()){
			return false;
		}
		
		this.timer.schedule(timerTask, delay);
		return true;
	}
	
	/**
	 * 把执行任务放入执行线程池中进行执行
	 * @param task 待执行的任务
	 * @return true代表放入成功 false代表失败
	 */
	public boolean addExecTask(BasicTask task){
		if(!this.isRunning()){
			return false;
		}
		
		return this.pool.addTaskWithReturn(task, false);
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
