package com.fleety.server;

import org.quartz.SchedulerException;

import server.threadgroup.PoolInfo;
import server.threadgroup.ThreadPoolGroupServer;

import com.fleety.util.pool.thread.ThreadPool;

public class JobLoadServer extends BasicServer {
	private static JobLoadServer singleInstance = null;
	private ThreadPool pool = null;

	public static JobLoadServer getSingleInstance() {
		if (singleInstance == null) {
			synchronized (JobLoadServer.class) {
				if (singleInstance == null) {
					singleInstance = new JobLoadServer();
				}
			}
		}
		return singleInstance;
	}

	public boolean startServer() {
		try {
			PoolInfo pInfo = new PoolInfo();
			pInfo.taskCapacity = 10000;
			pInfo.workersNumber = 20;
			pInfo.poolType = ThreadPool.MULTIPLE_TASK_LIST_POOL;
			this.pool = ThreadPoolGroupServer.getSingleInstance()
					.createThreadPool("job-thread-pool", pInfo);

			String cfgPath = this.getStringPara("cfg_path");
			JobLoader job = new JobLoader(cfgPath);
			job.loadAllJobs();
			this.isRunning = true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return this.isRunning();
	}

	public void stopServer() {
		super.stopServer();
		try {
			JobLoader.getScheduler().shutdown();
		} catch (SchedulerException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public ThreadPool getThreadPool() {
		return this.pool;
	}

}
