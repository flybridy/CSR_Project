package com.fleety.job;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.fleety.job.task.FaultCarAnasisTask;
import com.fleety.server.JobLoadServer;

public class FaultCarCountAnasisJob implements Job {

	@Override
	public void execute(JobExecutionContext arg0) throws JobExecutionException {
		// TODO Auto-generated method stub
		JobLoadServer.getSingleInstance().getThreadPool().addTask(new FaultCarAnasisTask());
	}

}
