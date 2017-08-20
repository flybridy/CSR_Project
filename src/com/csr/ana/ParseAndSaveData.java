package com.csr.ana;

import com.fleety.util.pool.thread.ThreadPool;

import server.threadgroup.PoolInfo;
import server.threadgroup.ThreadPoolGroupServer;

public class ParseAndSaveData {
		
	public static void parse(String messageInfo){
		try {
			String poolName = "data_save_pool";
			ThreadPool pool = ThreadPoolGroupServer
					.getSingleInstance().getThreadPool(poolName);
			if (pool == null) {
				PoolInfo pInfo = new PoolInfo();
				pInfo.workersNumber = 1;
				pInfo.taskCapacity =50;
				pool = ThreadPoolGroupServer.getSingleInstance()
						.createThreadPool(poolName, pInfo);
			}
			pool.addTask(new SaveTask(messageInfo));		
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
	
}
