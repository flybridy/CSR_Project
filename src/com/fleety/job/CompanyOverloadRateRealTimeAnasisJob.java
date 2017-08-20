package com.fleety.job;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.fleety.job.task.CompanyOverloadRateRealTimeAnalysisTask;
import com.fleety.server.JobLoadServer;

public class CompanyOverloadRateRealTimeAnasisJob implements Job{

	@Override
	public void execute(JobExecutionContext arg0) throws JobExecutionException {
		JobLoadServer.getSingleInstance().getThreadPool().addTask(new CompanyOverloadRateRealTimeAnalysisTask());//企业实时分析
	}

}
