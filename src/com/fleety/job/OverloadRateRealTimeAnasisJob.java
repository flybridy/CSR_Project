package com.fleety.job;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import server.db.DbServer;

import com.fleety.job.task.OverloadRateRealTimeAnalysisTask;
import com.fleety.server.JobLoadServer;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;

public class OverloadRateRealTimeAnasisJob implements Job
{

	@Override
	 public void execute(JobExecutionContext arg0) throws JobExecutionException
	    {
		    List<Integer> list = new ArrayList<Integer>();
		    DbHandle conn = DbServer.getSingleInstance().getConnWithUseTime(0);
		    StatementHandle stmt;
			try {
				stmt = conn
						.prepareStatement("select distinct company_id  from v_ana_dest_info order by to_number(company_id)");
				ResultSet rs = stmt.executeQuery();
			    while(rs.next())
			    	list.add(rs.getInt("company_id"));
			} catch (SQLException e) {
				e.printStackTrace();
			}finally{
				DbServer.getSingleInstance().releaseConn(conn);
			}
		    for(int i=0;i<list.size();i++){
	    	    JobLoadServer.getSingleInstance().getThreadPool().addTask(new OverloadRateRealTimeAnalysisTask(list.get(i),i));//区域企业的实时分析
		    }
	    }

}
