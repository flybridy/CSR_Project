package com.fleety.job.task;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import server.db.DbServer;

import com.fleety.analysis.order.redis.OrderRealTimeBean;
import com.fleety.analysis.realtime.BusinessRealTimeBean;
import com.fleety.analysis.realtime.GradeRealTimeBean;
import com.fleety.base.GeneralConst;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.fleety.util.pool.db.redis.RedisConnPoolServer;
import com.fleety.util.pool.db.redis.RedisTableBean;
import com.fleety.util.pool.thread.BasicTask;

public class GradeRealTimeAnasisTask  extends BasicTask {

	public boolean execute() throws Exception {

		DbHandle conn = DbServer.getSingleInstance().getConnWithUseTime(0);
		try{
			Calendar now = Calendar.getInstance();
			
			now.set(Calendar.HOUR_OF_DAY, 0);
			now.set(Calendar.MINUTE, 0);
			now.set(Calendar.SECOND, 0);
			now.set(Calendar.MILLISECOND, 0);
			Timestamp startTime = new Timestamp(now.getTimeInMillis());
			
			now.set(Calendar.HOUR_OF_DAY, 23);
			now.set(Calendar.MINUTE, 59);
			now.set(Calendar.SECOND, 59);
			now.set(Calendar.MILLISECOND, 0);
			
			Timestamp endTime = new Timestamp(now.getTimeInMillis());
			
			StringBuffer sql = new StringBuffer();
			sql.append("select c.term_id, sum(case grade_type when 0 then 1 else 0 end) goodNum,sum(case grade_type when 1 then 1 else 0 end) badNum,sum(case grade_type when 2 then 1 else 0 end) verygoodNum,sum(case grade_type when 3 then 1 else 0 end) unknowNum from grade t, car c where t.car_no = c.car_id and create_time >= ? and create_time <= ? group by term_id");

			StatementHandle stmt = conn.prepareStatement(sql.toString());
			stmt.setTimestamp(1, startTime);
			stmt.setTimestamp(2, endTime);
			
			ResultSet sets = stmt.executeQuery();
			List<GradeRealTimeBean> list = new ArrayList<GradeRealTimeBean>();
			GradeRealTimeBean bean =null;
			while(sets.next()){
				bean = new GradeRealTimeBean();
				bean.setUid(sets.getInt("term_id")+"");
				bean.setComId(sets.getInt("term_id"));
				bean.setGoodNum(sets.getInt("goodNum"));
				bean.setBadNum(sets.getInt("badNum"));
				bean.setVeryGoodNum(sets.getInt("verygoodNum"));
				bean.setUnknownNum(sets.getInt("unknowNum"));
				list.add(bean);
			}
			conn.closeStatement(stmt);
			
			RedisTableBean beans[] = new RedisTableBean[list.size()];
			list.toArray(beans);
			RedisConnPoolServer.getSingleInstance().clearTableRecord(new GradeRealTimeBean());
			RedisConnPoolServer.getSingleInstance().saveTableRecord(beans);

		}catch(Exception e){
			throw e;
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
		return false;
	}

	public String getDesc() {
		return "服务评价实时数据分析";
	}

	public Object getFlag() {
		return "GradeRealTimeAnasisTask";
	}
}
