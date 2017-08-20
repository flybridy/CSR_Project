package com.fleety.job;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.fleety.job.task.GradeRealTimeAnasisTask;
import com.fleety.server.JobLoadServer;

public class GradeRealTimeAnasisJob implements Job
{

    public void execute(JobExecutionContext arg0) throws JobExecutionException
    {
    	JobLoadServer.getSingleInstance().getThreadPool().addTask(new GradeRealTimeAnasisTask());
    }

}
