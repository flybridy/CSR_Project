package com.fleety.job;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.fleety.job.task.OrderCompanyRealTimeAnasisTask;
import com.fleety.server.JobLoadServer;

public class OrderCompanyRealTimeAnasisJob implements Job
{

    public void execute(JobExecutionContext arg0) throws JobExecutionException
    {
    	JobLoadServer.getSingleInstance().getThreadPool().addTask(new OrderCompanyRealTimeAnasisTask());
    }

}