package com.fleety.job;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import com.fleety.job.task.RealOfflineAnasisTask;
import com.fleety.server.JobLoadServer;

public class RealOfflineAnasisJob implements Job {

	@Override
	public void execute(JobExecutionContext arg0) throws JobExecutionException {
		// TODO Auto-generated method stub
		JobLoadServer.getSingleInstance().getThreadPool().addTask(new RealOfflineAnasisTask());
	}

}
