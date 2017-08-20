package com.fleety.job.task;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import server.db.DbServer;

import com.fleety.analysis.realtime.GuzhangRealTimeBean;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.fleety.util.pool.db.redis.RedisConnPoolServer;
import com.fleety.util.pool.db.redis.RedisTableBean;
import com.fleety.util.pool.thread.BasicTask;

public class GuzhangRealTimeAnasisTask  extends BasicTask {

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
			sql.append("select a.id, a.name,a.alias_name, decode(b.alarm_num,null,0,b.alarm_num) alarm_num from v_ana_company_info a left join (select term_id, sum(alarm_num) as alarm_num from (select car_no, count(car_no) as alarm_num from vehicle_alarm_log_new where alarm_type = 2 and alarm_time>=? and alarm_time <=? group by car_no) a inner join car c on a.car_no = c.car_id where term_id is not null group by term_id) b on a.id = b.term_id where a.id in (select term_id from car where mdt_id>-1)");

			StatementHandle stmt = conn.prepareStatement(sql.toString());
			stmt.setTimestamp(1, startTime);
			stmt.setTimestamp(2, endTime);
			
			ResultSet sets = stmt.executeQuery();
			Map<String,GuzhangRealTimeBean> map = new HashMap<String,GuzhangRealTimeBean>();
			GuzhangRealTimeBean bean = null;
			while(sets.next()){
				bean = new GuzhangRealTimeBean();
				bean.setUid(sets.getInt("id")+"");
				bean.setCompanyId(sets.getInt("id"));
				bean.setAliasName(sets.getString("alias_name"));
				bean.setCompanyName(sets.getString("name"));
				bean.setOverSpeedTotal(sets.getInt("alarm_num"));
				map.put(bean.getUid(), bean);
			}
			sets.close();
			
			sql = new StringBuffer();
			sql.append(" select t2.term_id,count(t2.term_id) total,sum(decode(status_end_time,null,0,1)) hf from vehicle_status_alarm_log t1 left join car t2 on t1.mdt_id = t2.mdt_id where t1.status_type=11 and status_start_time >=? and status_start_time <=? and t2.term_id in (select term_id from car where mdt_id>-1) group by t2.term_id");
			stmt = conn.prepareStatement(sql.toString());
			stmt.setTimestamp(1, startTime);
			stmt.setTimestamp(2, endTime);
			sets = stmt.executeQuery();
			while(sets.next()){
				if(map.containsKey(sets.getInt("term_id")+"")){
					bean = map.get(sets.getInt("term_id")+"");
					bean.setSxtTotal(sets.getInt("total"));
					bean.setSxtHfTotal(sets.getInt("hf"));
				}
			}
			sets.close();
			
			sql = new StringBuffer();
			sql.append(" select t2.term_id,count(t2.term_id) total,sum(decode(status_end_time,null,0,1)) hf from vehicle_status_alarm_log t1 left join car t2 on t1.mdt_id = t2.mdt_id where t1.status_type=12 and status_start_time >=? and status_start_time <=? and t2.term_id in (select term_id from car where mdt_id>-1) group by t2.term_id");
			stmt = conn.prepareStatement(sql.toString());
			stmt.setTimestamp(1, startTime);
			stmt.setTimestamp(2, endTime);
			sets = stmt.executeQuery();
			while(sets.next()){
				if(map.containsKey(sets.getInt("term_id")+"")){
					bean = map.get(sets.getInt("term_id")+"");
					bean.setJjqTotal(sets.getInt("total"));
					bean.setJjqHfTotal(sets.getInt("hf"));
				}
			}
			sets.close();
			
			sql = new StringBuffer();
			sql.append(" select t2.term_id,count(t2.term_id) total,sum(decode(status_end_time,null,0,1)) hf from vehicle_status_alarm_log t1 left join car t2 on t1.mdt_id = t2.mdt_id where t1.status_type=9 and status_start_time >=? and status_start_time <=? and t2.term_id in (select term_id from car where mdt_id>-1) group by t2.term_id");
			stmt = conn.prepareStatement(sql.toString());
			stmt.setTimestamp(1, startTime);
			stmt.setTimestamp(2, endTime);
			sets = stmt.executeQuery();
			while(sets.next()){
				if(map.containsKey(sets.getInt("term_id")+"")){
					bean = map.get(sets.getInt("term_id")+"");
					bean.setScreenTotal(sets.getInt("total"));
					bean.setScreenHfTotal(sets.getInt("hf"));
				}
			}
			sets.close();
			
			conn.closeStatement(stmt);
			
			RedisTableBean beans[] = new RedisTableBean[map.size()];
			Iterator<String> it = map.keySet().iterator();
			List<GuzhangRealTimeBean> list = new ArrayList<GuzhangRealTimeBean>();
			while(it.hasNext()){
				list.add(map.get(it.next()));
			}
			list.toArray(beans);
			
			RedisConnPoolServer.getSingleInstance().saveTableRecord(beans);

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
