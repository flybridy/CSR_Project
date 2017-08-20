package com.fleety.job;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.fleety.server.JobLoadServer;
import com.fleety.server.emp_enti_change.Emp_Enti_Change;
import com.fleety.util.pool.thread.ITask;

public class EmpEntiChangeAnasisJob implements Job {
	
	public void execute(JobExecutionContext arg0) throws JobExecutionException
    {
    	JobLoadServer.getSingleInstance().getThreadPool().addTask((ITask) new Emp_Enti_Change());
    }

}
