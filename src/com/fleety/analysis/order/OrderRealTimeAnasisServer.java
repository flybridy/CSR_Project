package com.fleety.analysis.order;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import server.db.DbServer;

import com.fleety.analysis.AnalysisServer;
import com.fleety.analysis.order.redis.OrderRealTimeBean;
import com.fleety.analysis.order.redis.OrderRealTimeStatBean;
import com.fleety.base.GeneralConst;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.fleety.util.pool.db.redis.RedisConnPoolServer;
import com.fleety.util.pool.db.redis.RedisTableBean;
import com.fleety.util.pool.thread.BasicTask;
import com.fleety.util.pool.timer.FleetyTimerTask;

public class OrderRealTimeAnasisServer extends AnalysisServer {
	private TimerTask task = null;
	private int mobileWeixinOperator[] = {0};
	private int netOperator[] = {0};
	private int intervalTime = 60;
	
	private static final int PHONE_TYPE = 0;
	private static final int MOBILE_APP_TYPE = 1;
	private static final int QQ_TYPE =2;
	private static final int WEIXIN_TYPE = 3;
	private static final int NET_TYPE = 4;

	public boolean startServer() {
		super.startServer();
		if(!this.isRunning()){
			return this.isRunning();
		}		
		
		intervalTime = this.getIntegerPara("interval_time").intValue();
		
		String temp = this.getStringPara("mobile_and_weixin_operator");
		if(temp != null && !"".equals(temp)){
			String []arr = temp.split(",");
			mobileWeixinOperator = new int [arr.length];
			for(int i = 0;i<arr.length;i++){
				mobileWeixinOperator[i] = Integer.parseInt(arr[i]);
			}
		}
		
		temp = this.getStringPara("net_operator");
		if(temp != null && !"".equals(temp)){
			String []arr = temp.split(",");
			netOperator = new int [arr.length];
			for(int i = 0;i<arr.length;i++){
				netOperator[i] = Integer.parseInt(arr[i]);
			}
		}

		this.isRunning = this.scheduleTask(this.task = new TimerTask(), 1000, intervalTime * 1000);
		
		return this.isRunning();
	}
	
	public void stopServer(){
		if(this.task != null){
			this.task.cancel();
		}
		super.stopServer();
	}

	private void executeTask() throws Exception{
		//每天凌晨或中午12点清理两天前数据
		this.deleteInfo();
		
		DbHandle conn = DbServer.getSingleInstance().getConnWithUseTime(0);
		try{
			Calendar now = Calendar.getInstance();
			Timestamp endTime = new Timestamp(now.getTimeInMillis());
			String statTime = GeneralConst.YYYY_MM_DD_HH_MM.format(now.getTime());
			
			now.set(Calendar.HOUR_OF_DAY, 0);
			now.set(Calendar.MINUTE, 0);
			now.set(Calendar.SECOND, 0);
			now.set(Calendar.MILLISECOND, 0);
			
			Timestamp startTime = new Timestamp(now.getTimeInMillis());
			
			String sql ="select * from TAXI_ORDER_LIST where created_time>=? and created_time<?";
			
			StatementHandle stmt = conn.prepareStatement(sql);
			stmt.setTimestamp(1, startTime);
			stmt.setTimestamp(2, endTime);
			
			RecordInfo[] rInfoArr = new RecordInfo[5];
			for(int i=0;i<rInfoArr.length;i++){
				rInfoArr[i] = new RecordInfo();
				rInfoArr[i].orderType=i;
			}
			
			RecordInfo rInfo;
			int status,operoterId,isImmediate=0;
			String specialRemark="",customerName="";
			ResultSet sets = stmt.executeQuery();
			
			while(sets.next()){
				operoterId = sets.getInt("user_id");
				status = sets.getInt("status");
				specialRemark = sets.getString("special_remark");	
				customerName = sets.getString("customer_name");
				isImmediate = sets.getInt("is_immediate");
				if("QQ约车订单".equals(specialRemark)){
					rInfo = rInfoArr[QQ_TYPE];
				}else if("微信约车订单".equals(specialRemark)){
					rInfo = rInfoArr[WEIXIN_TYPE];
				}else if(isPhoneOrder(operoterId)){
					if(customerName != null && customerName.indexOf("微信用户")>=0){
						rInfo = rInfoArr[WEIXIN_TYPE];
					}else{
						rInfo = rInfoArr[MOBILE_APP_TYPE];
					}
				}else if(isNetOrder(operoterId)){
					rInfo = rInfoArr[NET_TYPE];
				}else{
					rInfo = rInfoArr[PHONE_TYPE];
				}
				if(isImmediate == 0){
					rInfo.yuyueNum++;
				}
				
				rInfo.totalNum++;
				switch(status){
				case 3:
					rInfo.successNum++;
					break;
				case 4:
					rInfo.wugongNum++;
					break;
				case 5:
					rInfo.cancelNum++;
					break;
				case 7:
					rInfo.fangkongNum++;
					break;
				}
			}
			conn.closeStatement(stmt);
			
			OrderRealTimeBean bean = new OrderRealTimeBean();
			bean.setUid(statTime);
			for(int i=0;i<rInfoArr.length;i++){
				if(rInfoArr[i].orderType == PHONE_TYPE){
					bean.setIvrTotalNum(rInfoArr[i].totalNum);
					bean.setIvrSuccessNum(rInfoArr[i].successNum);
					bean.setIvrCancelNum(rInfoArr[i].cancelNum);
					bean.setIvrFangkongNum(rInfoArr[i].fangkongNum);
					bean.setIvrWugongNum(rInfoArr[i].wugongNum);
					bean.setIvrYuyueNum(rInfoArr[i].yuyueNum);
				}
				
				if(rInfoArr[i].orderType == MOBILE_APP_TYPE){
					bean.setMobileTotalNum(rInfoArr[i].totalNum);
					bean.setMobileSuccessNum(rInfoArr[i].successNum);
					bean.setMobileCancelNum(rInfoArr[i].cancelNum);
					bean.setMobileFangkongNum(rInfoArr[i].fangkongNum);
					bean.setMobileWugongNum(rInfoArr[i].wugongNum);
					bean.setMobileYuyueNum(rInfoArr[i].yuyueNum);
				}
				
				if(rInfoArr[i].orderType == QQ_TYPE){
					bean.setQqTotalNum(rInfoArr[i].totalNum);
					bean.setQqSuccessNum(rInfoArr[i].successNum);
					bean.setQqCancelNum(rInfoArr[i].cancelNum);
					bean.setQqFangkongNum(rInfoArr[i].fangkongNum);
					bean.setQqWugongNum(rInfoArr[i].wugongNum);
					bean.setQqYuyueNum(rInfoArr[i].yuyueNum);
				}
				
				if(rInfoArr[i].orderType == WEIXIN_TYPE){
					bean.setWxTotalNum(rInfoArr[i].totalNum);
					bean.setWxSuccessNum(rInfoArr[i].successNum);
					bean.setWxCancelNum(rInfoArr[i].cancelNum);
					bean.setWxFangkongNum(rInfoArr[i].fangkongNum);
					bean.setWxWugongNum(rInfoArr[i].wugongNum);
					bean.setWxYuyueNum(rInfoArr[i].yuyueNum);
				}
				
				if(rInfoArr[i].orderType == NET_TYPE){
					bean.setNetTotalNum(rInfoArr[i].totalNum);
					bean.setNetSuccessNum(rInfoArr[i].successNum);
					bean.setNetCancelNum(rInfoArr[i].cancelNum);
					bean.setNetFangkongNum(rInfoArr[i].fangkongNum);
					bean.setNetWugongNum(rInfoArr[i].wugongNum);
					bean.setNetYuyueNum(rInfoArr[i].yuyueNum);
				}
			}
			RedisConnPoolServer.getSingleInstance().saveTableRecord(new RedisTableBean[]{bean});

			this.stat();
		}catch(Exception e){
			throw e;
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
	}
	
	private void stat(){
		List list  = new ArrayList();
		DbHandle conn = DbServer.getSingleInstance().getConnWithUseTime(0);
		try{
			Calendar now = Calendar.getInstance();
			Timestamp endTime = new Timestamp(now.getTimeInMillis());
			now.set(Calendar.HOUR_OF_DAY, 0);
			now.set(Calendar.MINUTE, 0);
			now.set(Calendar.SECOND, 0);
			now.set(Calendar.MILLISECOND, 0);
			
			Timestamp startTime = new Timestamp(now.getTimeInMillis());
			
			String sql ="select decode(car_company,null,0,car_company) car_company,count(*) order_total,sum(decode(car_no, null, 0, 1)) yougong_total,sum(case when (status = 2 or status = 3) then 1 else 0 end) as FINISH_TOTAL,sum(case when status = 5 then 1 else 0 end) as cancel_total,sum(case when status = 7 then 1 else 0end) as fangkong_total from TAXI_ORDER_LIST where created_time>=? and created_time<? group by car_company";
			
			StatementHandle stmt = conn.prepareStatement(sql);
			stmt.setTimestamp(1, startTime);
			stmt.setTimestamp(2, endTime);

			ResultSet sets = stmt.executeQuery();
			OrderRealTimeStatBean bean = null;
			while(sets.next()){
				bean = new OrderRealTimeStatBean();
				bean.setCompanyId(sets.getInt("car_company"));
				bean.setUid(sets.getInt("car_company")+"");
				bean.setOrderTotal(sets.getInt("order_total"));
				bean.setYougongTotal(sets.getInt("yougong_total"));
				bean.setFinishTotal(sets.getInt("finish_total"));
				bean.setCancelTotal(sets.getInt("cancel_total"));
				bean.setFangkongTotal(sets.getInt("fangkong_total"));
				list.add(bean);
			}
			conn.closeStatement(stmt);
			
			bean = new OrderRealTimeStatBean();
			RedisConnPoolServer.getSingleInstance().clearTableRecord(bean);
			
			if(list.size()>0){
				OrderRealTimeStatBean []beans = new OrderRealTimeStatBean[list.size()];
				list.toArray(beans);
				RedisConnPoolServer.getSingleInstance().saveTableRecord(beans);
			}

		}catch(Exception e){
			e.printStackTrace();
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
	}
	
	private void deleteInfo() throws Exception{
		Calendar now = Calendar.getInstance();
		String time = GeneralConst.HHMM.format(now.getTime());
		if(!time.equals("0000") && !time.equals("1200")){
			return;
		}

		String today = GeneralConst.YYYY_MM_DD.format(now.getTime());
		now.add(Calendar.DAY_OF_MONTH, -1);
		String yesterday = GeneralConst.YYYY_MM_DD.format(now.getTime());
		
		now.add(Calendar.DAY_OF_MONTH, -1);
		String last = GeneralConst.YYYY_MM_DD.format(now.getTime());
		
		OrderRealTimeBean bean = new OrderRealTimeBean();
		Set<String> keySet = RedisConnPoolServer.getSingleInstance().getAllIdsForTable(bean);
		Iterator<String> it = keySet.iterator();
		String uid = "";
		List<OrderRealTimeBean> list = new ArrayList<OrderRealTimeBean>();
		while(it.hasNext()){
			bean = new OrderRealTimeBean();
			uid = it.next();
			if(uid.indexOf(today)>-1 || uid.indexOf(yesterday)>-1 || uid.indexOf(last)>-1){
				continue;
			}
			bean.setUid(uid);
			list.add(bean);
		}
		
		if(list.size()>0){
			RedisTableBean[] beanArr= new OrderRealTimeBean[list.size()];
			list.toArray(beanArr);
			RedisConnPoolServer.getSingleInstance().deleteTableRecord(beanArr);
		}
	}
	
	private boolean isPhoneOrder(int operatorId){
		boolean result = false;
		for(int i = 0;i<mobileWeixinOperator.length;i++){
			if(mobileWeixinOperator[i] == operatorId){
				result =  true;
				break;
			}
		}
		return result;
	}
	
	private boolean isNetOrder(int operatorId){
		boolean result = false;
		for(int i = 0;i<netOperator.length;i++){
			if(netOperator[i] == operatorId){
				result =  true;
				break;
			}
		}
		return result;
	}
	

	private class RecordInfo{
		public int orderType = 0;
		public int totalNum = 0;
		public int successNum = 0;
		public int wugongNum = 0;
		public int cancelNum = 0;
		public int fangkongNum = 0;
		public int yuyueNum = 0;
		
		public RecordInfo(){
			
		}
	}
	
	private class TimerTask extends FleetyTimerTask{
		public void run(){
			OrderRealTimeAnasisServer.this.addExecTask(new ExecTask());
		}
	}
	
	private class ExecTask extends BasicTask{
		public ExecTask(){}
		
		public boolean execute() throws Exception{
			OrderRealTimeAnasisServer.this.executeTask();
			return true;
		}
		
		public String getDesc(){
			return "订单实时数据分析";
		}
		public Object getFlag(){
			return "OrderRealTimeAnasisServer";
		}
	}
}
