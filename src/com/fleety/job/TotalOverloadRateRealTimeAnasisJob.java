package com.fleety.job;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.fleety.job.task.TotalOverloadRateRealTimeAnalysisTask;
import com.fleety.server.JobLoadServer;

public class TotalOverloadRateRealTimeAnasisJob implements Job{

	@Override
	public void execute(JobExecutionContext arg0) throws JobExecutionException {
		JobLoadServer.getSingleInstance().getThreadPool().addTask(new TotalOverloadRateRealTimeAnalysisTask());//整体企业实时分析
	}

}
