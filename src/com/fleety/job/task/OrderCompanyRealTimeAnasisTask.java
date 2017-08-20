package com.fleety.job.task;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import server.db.DbServer;

import com.fleety.analysis.realtime.OrderCompanyRealTimeBean;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.fleety.util.pool.db.redis.RedisConnPoolServer;
import com.fleety.util.pool.db.redis.RedisTableBean;
import com.fleety.util.pool.thread.BasicTask;

public class OrderCompanyRealTimeAnasisTask  extends BasicTask {

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
			sql.append("select t1.term_id,t1.term_name,decode(t2.qiantian,null,0,t2.qiantian) qiantian,decode(t2.zuotian,null,0,t2.zuotian) zuotian,decode(t3.total,null,0,t3.total) jintian from term t1 left join");
			sql.append(" (select company_id,sum(case to_char(stat_time,'yyyy-MM-dd') when to_char(sysdate-2,'yyyy-MM-dd') then order_total else 0 end) qiantian,sum(case to_char(stat_time,'yyyy-MM-dd') when to_char(sysdate-1,'yyyy-MM-dd') then order_total else 0 end)zuotian from ana_order_company_stat  where stat_time > sysdate-3 group by company_id)t2 on t1.term_id = t2.company_id");
			sql.append(" left join (select car_company,count(order_id)total from taxi_order_list where car_company is not null and car_wanted_time >= ? and car_wanted_time<=? group by car_company)t3 on t1.term_id = t3.car_company");
			sql.append(" where t1.term_id in (select distinct term_id from car where mdt_id>-1)");

			StatementHandle stmt = conn.prepareStatement(sql.toString());
			stmt.setTimestamp(1, startTime);
			stmt.setTimestamp(2, endTime);
			
			ResultSet sets = stmt.executeQuery();
			List<OrderCompanyRealTimeBean> list = new ArrayList<OrderCompanyRealTimeBean>();
			OrderCompanyRealTimeBean bean = null;
			while(sets.next()){
				bean = new OrderCompanyRealTimeBean();
				bean.setUid(sets.getInt("term_id")+"");
				bean.setCompanyId(sets.getInt("term_id"));
				bean.setCompanyName(sets.getString("term_name"));
				bean.setQtTotal(sets.getInt("qiantian"));
				bean.setZtTotal(sets.getInt("zuotian"));
				bean.setJtTotal(sets.getInt("jintian"));
				list.add(bean);
			}
			conn.closeStatement(stmt);
			
			RedisTableBean beans[] = new RedisTableBean[list.size()];
			list.toArray(beans);
			
			RedisConnPoolServer.getSingleInstance().saveTableRecord(beans);
//			bean = new OrderCompanyRealTimeBean();
//			list = RedisConnPoolServer.getSingleInstance().queryTableRecord(new RedisTableBean[]{bean});
//			for(int i=0;i<list.size();i++){
//				bean = (OrderCompanyRealTimeBean)list.get(i);
//				System.out.println("companyId="+bean.getCompanyId()+" ,companyName="+bean.getCompanyName()+", qt="+bean.getQtTotal()+" ,zt="+bean.getZtTotal()+" ,jt="+bean.getJtTotal());
//			}

		}catch(Exception e){
			throw e;
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
		return false;
	}

	public String getDesc() {
		return "企业电召业务实时分析";
	}

	public Object getFlag() {
		return "OrderCompanyRealTimeAnasisTask";
	}
}
