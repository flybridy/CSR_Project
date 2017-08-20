package com.fleety.analysis;

import server.threadgroup.PoolInfo;
import server.threadgroup.ThreadPoolGroupServer;
import server.var.VarManageServer;

import com.fleety.server.BasicServer;
import com.fleety.util.pool.thread.BasicTask;
import com.fleety.util.pool.thread.ThreadPool;

public class RealTimeAnalysisServer extends BasicServer {
	private String ANALYSIS_THREAD_POOL = "realtime-analysis-thread-pool";
	private ThreadPool pool = null;
	
	@Override
	public boolean startServer() {
		if(this.isRunning){
			return this.isRunning;
		}
		String tempStr = this.getStringPara("realtime_analysis_thread_pool");
		if(tempStr != null && tempStr.trim().length() > 0){
			this.ANALYSIS_THREAD_POOL = tempStr.trim();
		}
		
		Integer threadNum = VarManageServer.getSingleInstance().getIntegerPara("realtime_pool_thread_num");
		if(threadNum == null){
			threadNum = new Integer(10);
		}
		PoolInfo pInfo = new PoolInfo();
		pInfo.taskCapacity = 10000;
		pInfo.workersNumber = threadNum;
		pInfo.poolType = ThreadPool.MULTIPLE_TASK_LIST_POOL;
		try{
			this.pool = ThreadPoolGroupServer.getSingleInstance().createThreadPool(ANALYSIS_THREAD_POOL, pInfo);
		}catch(Exception e){
			e.printStackTrace();
			return false;
		}

		if(this.pool != null){
			this.isRunning = true;
		}
		
		return this.isRunning();
	}
	
	public void stopServer(){
		this.pool = null;
		super.stopServer();
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
		
		return this.pool.addTaskWithReturn(task, true);
	}
}
