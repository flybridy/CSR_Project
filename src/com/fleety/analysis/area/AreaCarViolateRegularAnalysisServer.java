package com.fleety.analysis.area;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;

import server.db.DbServer;
import server.mail.MailServer;

import com.fleety.analysis.AnalysisServer;
import com.fleety.base.GeneralConst;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.fleety.util.pool.timer.FleetyTimerTask;

public class AreaCarViolateRegularAnalysisServer extends AnalysisServer{
	private int duraStandard = 1*60;
	private double mileStandard = 20;
	private String mail = "guang.zhou@fleety.com";
	private TimerTask task = null;
	private int type0 = 0;//行驶里程
	private int type1 = 1;//停留时长
	public boolean startServer() {
		super.startServer();
		if(!this.isRunning){
			return this.isRunning;
		}
		int hour = this.getIntegerPara("hour").intValue();
		int minute = this.getIntegerPara("minute").intValue();
		String temp = this.getStringPara("duraStandard");
		if(temp!=null&&!temp.equals("")){
			duraStandard = Integer.valueOf(temp)*60;
		}
		temp = this.getStringPara("mileStandard");
		if(temp!=null&&!temp.equals("")){
			mileStandard = Double.valueOf(temp);
		}
		temp = this.getStringPara("mail");
		if(temp!=null&&!temp.equals("")){
			mail = temp;
		}
		Calendar cal = this.getNextExecCalendar(hour, minute);
		if(cal.get(Calendar.DAY_OF_MONTH) != Calendar.getInstance().get(Calendar.DAY_OF_MONTH)){
			this.scheduleTask(new TimerTask(), 500);
		}
		long delay = cal.getTimeInMillis() - System.currentTimeMillis();
		this.isRunning = this.scheduleTask(this.task = new TimerTask(), delay, GeneralConst.ONE_DAY_TIME);
		
		return this.isRunning;
	}
	private class TimerTask extends FleetyTimerTask
	{
		public void run() 
		{
			if(isDataYesterday()){
				return;
			}
			List<JSONObject> areaCarAlarmlist = new ArrayList<JSONObject>();
			try {
				analysisDataByMile(areaCarAlarmlist);
				analysisDataByDura(areaCarAlarmlist);
			} catch (Exception e) {
				e.printStackTrace();
				return;
			}
			saveAnalysisData(areaCarAlarmlist);
			sendSMS(areaCarAlarmlist);
		}
		
	}
	private boolean isDataYesterday(){
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try 
		{
			String sql = "select * from residence_area_car_alarm_day where statistics_date<? and statistics_date>=?";
			Calendar cal = Calendar.getInstance();
			cal.set(Calendar.HOUR_OF_DAY, 0);
			cal.set(Calendar.MINUTE, 0);
			cal.set(Calendar.SECOND, 0);
			StatementHandle stmt = conn.prepareStatement(sql);
			stmt.setTimestamp(1, new Timestamp(cal.getTimeInMillis()));
			cal.set(Calendar.DAY_OF_MONTH, cal.get(Calendar.DAY_OF_MONTH)-1);
			stmt.setTimestamp(2, new Timestamp(cal.getTimeInMillis()));
			ResultSet sets = stmt.executeQuery();
			if(sets.next()){
				return true;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
		return false;
	}
	private void analysisDataByMile(List<JSONObject> areaCarAlarmlist){
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try 
		{
			String sql = "select * from (select r.dest_no,r.area_id,a.cname,r.statistics_date,r.total_mile_n,r.total_dura_n,r.plan_id,r.plan_name,r.dura,r.mile from " +
					"(select * from (select rb.plan_id,p.plan_name,p.start_time,p.end_time,p.start_time1,p.end_time1," +
					"p.dura,p.mile,rb.statistics_date,rb.dest_no,rb.area_id,rb.com_id,rb.com_name,rb.server_id,rb.server_name," +
					"rb.total_dura_n,rb.total_dura_w,rb.run_dura_n,rb.run_dura_w,rb.total_mile_n,rb.total_mile_w,rb.nul_mile_n,rb.nul_mile_w," +
					"rb.run_num_n,rb.run_num_w,rb.run_money_n,rb.run_money_w,rb.total_num_j,rb.total_num_l,rb.kong_num_j,rb.kong_num_l,rb.zhong_num_j,rb.zhong_num_l from residence_area_business_day rb inner join car_area_plan_info p " +
					"on rb.plan_id = p.id ) where statistics_date<? and statistics_date>=? and total_mile_n<mile ) r " +
					"left join (select distinct area_id,cname from alarm_area) a on a.area_id=r.area_id) order by plan_id desc";
			Calendar cal = Calendar.getInstance();
			cal.set(Calendar.HOUR_OF_DAY, 0);
			cal.set(Calendar.MINUTE, 0);
			cal.set(Calendar.SECOND, 0);
			StatementHandle stmt = conn.prepareStatement(sql);
			stmt.setTimestamp(1, new Timestamp(cal.getTimeInMillis()));
			cal.set(Calendar.DAY_OF_MONTH, cal.get(Calendar.DAY_OF_MONTH)-1);
			stmt.setTimestamp(2, new Timestamp(cal.getTimeInMillis()));
			//stmt.setDouble(3, mileStandard);
			JSONObject areaCarAlarmJson = null;
			ResultSet sets = stmt.executeQuery();
			while (sets.next()) {
				areaCarAlarmJson = new JSONObject();
				try {
					areaCarAlarmJson.put("dest_no", sets.getString("dest_no"));
					areaCarAlarmJson.put("area_id", sets.getInt("area_id"));
					areaCarAlarmJson.put("plan_id", sets.getInt("plan_id"));
					areaCarAlarmJson.put("plan_name", sets.getString("plan_name"));
					areaCarAlarmJson.put("dura", sets.getInt("dura"));
					areaCarAlarmJson.put("mile", sets.getDouble("mile"));
					areaCarAlarmJson.put("statistics_date", sets.getString("statistics_date"));
					areaCarAlarmJson.put("total_mile_n", sets.getInt("total_mile_n"));
					areaCarAlarmJson.put("total_dura_n", sets.getInt("total_dura_n"));
					areaCarAlarmJson.put("type", this.type0);
					areaCarAlarmJson.put("area_name", sets.getString("cname"));
				} catch (JSONException e) {
					e.printStackTrace();
				}
				areaCarAlarmlist.add(areaCarAlarmJson);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
	}
	private void analysisDataByDura(List<JSONObject> areaCarAlarmlist){
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try 
		{
			String sql = "select * from (select r.dest_no,r.area_id,a.cname,to_char(r.statistics_date,'yyyy-MM-dd hh24:mi:ss') statistics_date,r.total_mile_n,r.total_dura_n,r.plan_id,r.plan_name,r.dura,r.mile from " +
					"(select * from (select rb.plan_id,p.plan_name,p.start_time,p.end_time,p.start_time1,p.end_time1," +
					"p.dura,p.mile,rb.statistics_date,rb.dest_no,rb.area_id,rb.com_id,rb.com_name,rb.server_id,rb.server_name," +
					"rb.total_dura_n,rb.total_dura_w,rb.run_dura_n,rb.run_dura_w,rb.total_mile_n,rb.total_mile_w,rb.nul_mile_n,rb.nul_mile_w," +
					"rb.run_num_n,rb.run_num_w,rb.run_money_n,rb.run_money_w,rb.total_num_j,rb.total_num_l,rb.kong_num_j,rb.kong_num_l,rb.zhong_num_j,rb.zhong_num_l from residence_area_business_day rb inner join car_area_plan_info p " +
					"on rb.plan_id = p.id ) where statistics_date<? and statistics_date>=? and total_dura_n<dura) r " +
					"left join (select distinct area_id,cname from alarm_area) a on a.area_id=r.area_id) order by plan_id desc";
			Calendar cal = Calendar.getInstance();
			cal.set(Calendar.HOUR_OF_DAY, 0);
			cal.set(Calendar.MINUTE, 0);
			cal.set(Calendar.SECOND, 0);
			StatementHandle stmt = conn.prepareStatement(sql);
			stmt.setTimestamp(1, new Timestamp(cal.getTimeInMillis()));
			cal.set(Calendar.DAY_OF_MONTH, cal.get(Calendar.DAY_OF_MONTH)-1);
			stmt.setTimestamp(2, new Timestamp(cal.getTimeInMillis()));
			//stmt.setDouble(3, duraStandard);
			JSONObject areaCarAlarmJson = null;
			ResultSet sets = stmt.executeQuery();
			while (sets.next()) {
				areaCarAlarmJson = new JSONObject();
				try {
					areaCarAlarmJson.put("dest_no", sets.getString("dest_no"));
					areaCarAlarmJson.put("area_id", sets.getInt("area_id"));
					areaCarAlarmJson.put("plan_id", sets.getInt("plan_id"));
					areaCarAlarmJson.put("plan_name", sets.getString("plan_name"));
					areaCarAlarmJson.put("dura", sets.getInt("dura"));
					areaCarAlarmJson.put("mile", sets.getDouble("mile"));
					areaCarAlarmJson.put("statistics_date", sets.getString("statistics_date"));
					areaCarAlarmJson.put("total_mile_n", sets.getInt("total_mile_n"));
					areaCarAlarmJson.put("total_dura_n", sets.getInt("total_dura_n"));
					areaCarAlarmJson.put("type", this.type1);
					areaCarAlarmJson.put("area_name", sets.getString("cname"));
				} catch (JSONException e) {
					e.printStackTrace();
				}
				areaCarAlarmlist.add(areaCarAlarmJson);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
	}
	private void saveAnalysisData(List<JSONObject> areaCarAlarmlist){
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try 
		{
			String sql = "insert into residence_area_car_alarm_day(id,dest_no,area_id,statistics_date,mile,mile_standard,dura,dura_standard,type,plan_id) values(?,?,?,?,?,?,?,?,?,?)";
			StatementHandle stmt = conn.prepareStatement(sql);
			int i=1;
			for (JSONObject areaCarAlarmJson : areaCarAlarmlist) {
				stmt.setInt(1, (int)DbServer.getSingleInstance().getAvaliableId(conn, "residence_area_car_alarm_day", "id"));
				stmt.setString(2, areaCarAlarmJson.getString("dest_no"));
				stmt.setInt(3, areaCarAlarmJson.getInt("area_id"));
				stmt.setTimestamp(4, new Timestamp(GeneralConst.YYYY_MM_DD_HH_MM_SS.parse(areaCarAlarmJson.getString("statistics_date")).getTime()));
				stmt.setDouble(5, areaCarAlarmJson.getInt("total_mile_n"));
				stmt.setDouble(6, areaCarAlarmJson.getDouble("mile"));
				stmt.setInt(7, areaCarAlarmJson.getInt("total_dura_n"));
				stmt.setInt(8, areaCarAlarmJson.getInt("dura"));
				stmt.setInt(9, areaCarAlarmJson.getInt("type"));
				stmt.setInt(10, areaCarAlarmJson.getInt("plan_id"));
				stmt.addBatch();
				if(i%200==0){
					stmt.executeBatch();
				}
				i++;
			}
			stmt.executeBatch();
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
	}
	private void sendSMS(List<JSONObject> areaCarAlarmlist){
		if(areaCarAlarmlist==null||areaCarAlarmlist.size()<=0){
			System.out.println("AreaCarViolateRegularAnalysisServer[没有驻点区域违规]");
			return;
		}
		 try {
			StringBuffer content = new StringBuffer();
			content.append("<html><body style='font-family:Arial'>");
			content.append("<table border='1px solid black' >");
			content.append("<tr bgColor='green' style='color:white'>");
			content.append("<td>序号</td>");
			content.append("<td>车牌号</td>");
			content.append("<td>计划名称</td>");
			content.append("<td>区域</td>");
			content.append("<td>行驶里程</td>");
			content.append("<td>行驶里程标准</td>");
			content.append("<td>停留时长</td>");
			content.append("<td>停留时长标准</td>");
			content.append("<td>违规类型</td>");
			content.append("</tr>");
			int i = 1;
			StringBuffer content1 = new StringBuffer();
			String tempStr = "";
			HashMap<String, String> tempMap = new HashMap<String, String>();
			for (JSONObject areaCarAlarmJson : areaCarAlarmlist) {
				content1.append("<tr>");
				content1.append("<td>"+i+"</td>");
				content1.append("<td>"+areaCarAlarmJson.getString("dest_no")+"</td>");
				content1.append("<td>"+areaCarAlarmJson.getString("plan_name")+"</td>");
				content1.append("<td>"+areaCarAlarmJson.getString("area_name")+"</td>");
				content1.append("<td>"+areaCarAlarmJson.getInt("total_mile_n")+"</td>");
				content1.append("<td>"+areaCarAlarmJson.getDouble("mile")+"</td>");
				content1.append("<td>"+areaCarAlarmJson.getInt("total_dura_n")+"</td>");
				content1.append("<td>"+areaCarAlarmJson.getInt("dura")+"</td>");
				int type = areaCarAlarmJson.getInt("type");
				if(type==0){
					content1.append("<td>行驶里程</td>");
				}else {
					content1.append("<td>停留时长</td>");
				}
				content1.append("</tr>");
				if(tempMap.containsKey(areaCarAlarmJson.getInt("plan_id")+"")){
					tempStr = tempMap.get(areaCarAlarmJson.getInt("plan_id")+"")+content1.toString();
				}else {
					tempStr = "<tr><td colspan='9'>"+areaCarAlarmJson.getString("plan_name")+"车辆违规情况</td></tr>"+content1.toString();
				}
				tempMap.put(areaCarAlarmJson.getInt("plan_id")+"",tempStr);
				i++;
			}
			for (Iterator iterator = tempMap.values().iterator(); iterator
					.hasNext();) {
				String temp = (String) iterator.next();
				content.append(temp);
			}
			content.append("</table></body></html>");
	        Calendar calStat = Calendar.getInstance();
	        calStat.add(Calendar.DAY_OF_MONTH, - 1);
	        mail.replaceAll("，", ",");
			String[] mailArray = mail.split(",");
			if(MailServer.getSingleInstance().isRunning()){
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy年MM月dd日");
				String title=sdf.format(calStat.getTime()) + "驻点区域车辆违规统计";
				MailServer.getSingleInstance().sendMail(mailArray,title,content.toString());
				System.out.println("AreaCarViolateRegularAnalysisServer[驻点区域违规详细发送成功]");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public void stopServer()
	{
		if(this.task != null)
		{
			this.task.cancel();
		}
		this.isRunning = false;
	}
	protected Calendar getNextExecCalendar(int hour,int minute){
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.HOUR_OF_DAY, hour);
		cal.set(Calendar.MINUTE, minute);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		
		if(cal.getTimeInMillis() < System.currentTimeMillis()){
			cal.add(Calendar.DAY_OF_MONTH, 1);
		}
		
		return cal;
	}
}
