package com.fleety.job.task;

import com.fleety.util.pool.thread.BasicTask;

public class TestTask extends BasicTask {

	@Override
	public boolean execute() throws Exception {
		System.out.println("Job TestTask");
		return false;
	}		
}
