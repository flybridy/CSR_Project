package com.fleety.job;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.fleety.job.task.MaintainRealTimeForCompanyAnalysisTask;

import com.fleety.server.JobLoadServer;

public class MaintainStabilityRealTimeForCompanyAnasisJob implements Job{

	@Override
	public void execute(JobExecutionContext arg0) throws JobExecutionException {
    	JobLoadServer.getSingleInstance().getThreadPool().addTask(new MaintainRealTimeForCompanyAnalysisTask());

	}

}
