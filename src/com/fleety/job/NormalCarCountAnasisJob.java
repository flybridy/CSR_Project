package com.fleety.job;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.fleety.job.task.SaveNormalCarAnasisTask;
import com.fleety.server.JobLoadServer;

public class NormalCarCountAnasisJob implements Job {

	@Override
	public void execute(JobExecutionContext arg0) throws JobExecutionException {
		// TODO Auto-generated method stub
		JobLoadServer.getSingleInstance().getThreadPool().addTask(new SaveNormalCarAnasisTask());
	}

}
