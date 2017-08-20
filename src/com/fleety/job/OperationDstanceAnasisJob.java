package com.fleety.job;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.fleety.job.task.OperationDstanceAnasisTask;
import com.fleety.server.JobLoadServer;
import com.fleety.util.pool.thread.ITask;

public class OperationDstanceAnasisJob implements Job {
	
	public void execute(JobExecutionContext arg0) throws JobExecutionException
    {
    	JobLoadServer.getSingleInstance().getThreadPool().addTask((ITask) new OperationDstanceAnasisTask());
    }

}
