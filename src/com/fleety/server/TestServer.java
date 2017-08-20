package com.fleety.server;

import java.util.Calendar;

import com.fleety.analysis.operation.task.DriverOperateDataAnalysisForDay;
import com.fleety.analysis.order.DriverOrdersDayAnasisServer;
import com.fleety.base.GeneralConst;

public class TestServer extends BasicServer {

	@Override
	public boolean startServer() {
		// TODO Auto-generated method stub
		
		DriverOrdersDayAnasisServer test=new DriverOrdersDayAnasisServer();
		Calendar calendar=Calendar.getInstance();
		calendar.setTimeInMillis(System.currentTimeMillis()-30*GeneralConst.ONE_DAY_TIME);
		while(calendar.getTimeInMillis()<System.currentTimeMillis()){
			try {
//				test.executeTask(calendar);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			calendar.setTimeInMillis(calendar.getTimeInMillis()+GeneralConst.ONE_DAY_TIME);
		}
		
		return true;
	}

}
