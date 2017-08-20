package com.fleety.job;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.fleety.job.task.DriverBusinessRealTimeAnasisTask;
import com.fleety.server.JobLoadServer;

public class DriverBusinessRealTimeAnasisJob implements Job
{

    public void execute(JobExecutionContext arg0) throws JobExecutionException
    {
    	JobLoadServer.getSingleInstance().getThreadPool().addTask(new DriverBusinessRealTimeAnasisTask());
    }

}