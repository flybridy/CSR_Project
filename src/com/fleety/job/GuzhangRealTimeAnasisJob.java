package com.fleety.job;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.fleety.job.task.GuzhangRealTimeAnasisTask;
import com.fleety.server.JobLoadServer;

public class GuzhangRealTimeAnasisJob implements Job
{

    public void execute(JobExecutionContext arg0) throws JobExecutionException
    {
    	JobLoadServer.getSingleInstance().getThreadPool().addTask(new GuzhangRealTimeAnasisTask());
    }

}