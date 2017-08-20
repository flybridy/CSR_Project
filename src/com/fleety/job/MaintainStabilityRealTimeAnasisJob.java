package com.fleety.job;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.fleety.job.task.CurrentMaintainStabilitydataRealTimeAnalysisTask;
import com.fleety.server.JobLoadServer;

public class MaintainStabilityRealTimeAnasisJob implements Job{

	@Override
	public void execute(JobExecutionContext arg0) throws JobExecutionException {
    	JobLoadServer.getSingleInstance().getThreadPool().addTask(new CurrentMaintainStabilitydataRealTimeAnalysisTask());//当前维稳数据实时分析
	}

}
