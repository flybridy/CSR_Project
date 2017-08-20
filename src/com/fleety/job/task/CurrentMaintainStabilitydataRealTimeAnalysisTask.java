package com.fleety.job.task;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import server.db.DbServer;
import server.track.TrackServer;

import com.fleety.analysis.realtime.MaintainStabilityRealTimeBean;
import com.fleety.analysis.track.DestInfo;
import com.fleety.analysis.track.TrackInfo;
import com.fleety.base.InfoContainer;
import com.fleety.track.TrackIO;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.fleety.util.pool.db.redis.RedisConnPoolServer;
import com.fleety.util.pool.db.redis.RedisTableBean;
import com.fleety.util.pool.thread.BasicTask;

public class CurrentMaintainStabilitydataRealTimeAnalysisTask extends BasicTask{
	
	@Override
	public boolean execute() throws Exception {
		System.out.println("XXXXXXXXXXXXXXXXXXXXXXXXX!!!!!!!!!");
		    HashMap<String, Car_speed> map = new HashMap<String, Car_speed>();
		//获取当前车辆总数
			int total_car = 0,car_red=0,car_green=0,car_electric=0,car_accessible=0,car_other=0;
			DbHandle conn = DbServer.getSingleInstance().getConn();
			try {
				StatementHandle stmt = conn.createStatement();
				String sql = "select count(*) total_car,type_id from v_ana_dest_info group by type_id";
				ResultSet sets = stmt.executeQuery(sql);
				while(sets.next()){
					if(sets.getInt("type_id")==1)
						car_red = sets.getInt("total_car");
					else if(sets.getInt("type_id")==2)
						car_green = sets.getInt("total_car");
					else if(sets.getInt("type_id")==3)
						car_electric = sets.getInt("total_car");
					else if(sets.getInt("type_id")==4)
						car_accessible = sets.getInt("total_car");
					else
						car_other = sets.getInt("total_car");
				}
				total_car = car_red+car_green+car_electric+car_accessible+car_other;
			}catch (SQLException e) {
				e.printStackTrace();
				System.out.println("数据库操作失败！");
			}
		
		//统计当前5分钟营运数据信息
		float business_num = 0,distance = 0,sum = 0,business_num_red = 0,distance_red = 0,sum_red = 0,business_num_green = 0,distance_green = 0,sum_green = 0,business_num_electric = 0,distance_electric = 0,sum_electric = 0,business_num_accessible = 0,distance_accessible = 0,sum_accessible = 0;
		StatementHandle stmt = conn
				.prepareStatement("select count(*) business_num,sum(distance) distance,sum(sum) sum,type_id from single_business_data_bs sbd left join v_ana_dest_info va on sbd.car_no = va.dest_no  where date_down >= sysdate-5/(60*24) and date_down <= sysdate  group by type_id");
		ResultSet rs = stmt.executeQuery();
		while(rs.next()){
			if(rs.getInt("type_id")==1)
			{
				business_num_red = rs.getFloat("business_num");
				distance_red = rs.getFloat("distance");
				sum_red = rs.getFloat("sum");
			}
			else if(rs.getInt("type_id")==2)
			{
				business_num_green = rs.getFloat("business_num");
				distance_green = rs.getFloat("distance");
				sum_green = rs.getFloat("sum");
			}
			else if(rs.getInt("type_id")==3)
			{
				business_num_electric = rs.getFloat("business_num");
				distance_electric = rs.getFloat("distance");
				sum_electric = rs.getFloat("sum");
			}
			else if(rs.getInt("type_id")==4)
			{
				business_num_accessible = rs.getFloat("business_num");
				distance_accessible = rs.getFloat("distance");
				sum_accessible = rs.getFloat("sum");
			}else{
				business_num = rs.getFloat("business_num");
				distance = rs.getFloat("distance");
				sum = rs.getFloat("sum");
			}
		}
		business_num = business_num+business_num_red+business_num_green+business_num_electric+business_num_accessible;
		distance = distance + distance_red + distance_green + distance_electric + distance_accessible;
		sum = sum + sum_red + sum_green + sum_electric + sum_accessible;
		//统计当前整分时间5分钟车辆速度一直为0的车辆数
		
		Calendar now = Calendar.getInstance();// 当前时间
		Calendar start = Calendar.getInstance();
		
		now.set(Calendar.SECOND, 0);
		now.set(Calendar.MILLISECOND, 0);
		
		start.add(Calendar.MINUTE,-5);
		start.set(Calendar.SECOND, 0);
		start.set(Calendar.MILLISECOND, 0);
		
		DestInfo dInfo;
		ArrayList destList = new ArrayList(1024);
		try {
			stmt = conn
					.prepareStatement("select mdt_id,dest_no,company_id,company_name,type_id,gps_run_com_id,gps_run_com_name from v_ana_dest_info");
			ResultSet sets = stmt.executeQuery();
			while (sets.next()) {
				dInfo = new DestInfo();
				dInfo.mdtId = sets.getInt("mdt_id");
				dInfo.destNo = sets.getString("dest_no");
				dInfo.companyId = sets.getInt("company_id");
				dInfo.companyName = sets.getString("company_name");
				dInfo.gpsRunComId = sets.getInt("gps_run_com_id");
				dInfo.gpsRunComName = sets.getString("gps_run_com_name");
				dInfo.carType = sets.getInt("type_id");
				destList.add(dInfo);
			}
			sets.close();
			stmt.close();
		}catch (Exception e) {
				throw e;
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
		InfoContainer queryInfo = new InfoContainer();
		
		Date sDate = start.getTime();
		Date eDate = now.getTime();
		queryInfo.setInfo(TrackServer.START_DATE_FLAG, sDate);
		queryInfo.setInfo(TrackServer.END_DATE_FLAG, eDate);
		TrackInfo trackInfo;
		int stopcarnum = 0,stopcarnum_red = 0,stopcarnum_green=0,stopcarnum_electric=0,stopcarnum_accessible=0;//各类型车辆停车数
		
		for (Iterator itr = destList.iterator(); itr.hasNext();) {
			dInfo = (DestInfo) itr.next();
			queryInfo.setInfo(TrackServer.DEST_NO_FLAG, dInfo.destNo);

			trackInfo = new TrackInfo();
			trackInfo.dInfo = dInfo;
			trackInfo.sDate = sDate;
			trackInfo.eDate = eDate;
			trackInfo.trackArr = TrackServer.getSingleInstance()
					.getTrackInfo(queryInfo);
			if(trackInfo.trackArr.length==0)
				continue;
			for(int i=0;i<trackInfo.trackArr.length;i++){
				if(trackInfo.trackArr[i].getInteger(TrackIO.DEST_SPEED_FLAG)!=0)
					break;
				if(i==trackInfo.trackArr.length-1)
				{
					stopcarnum ++;
					if(trackInfo.dInfo.carType == 1)
						stopcarnum_red++;
					if(trackInfo.dInfo.carType == 2)
						stopcarnum_green++;
					if(trackInfo.dInfo.carType == 3)
						stopcarnum_electric++;
					if(trackInfo.dInfo.carType == 4)
						stopcarnum_accessible++;
				}
			}
			float temp = 0;
			int j;
			for(j=0;j<trackInfo.trackArr.length;j++){
				temp = temp + trackInfo.trackArr[j].getInteger(TrackIO.DEST_SPEED_FLAG);
			}
			Car_speed cs = null;
			if(map.containsKey(trackInfo.dInfo.destNo)){
				cs = map.get(trackInfo.dInfo.destNo);
				cs.speed = temp/j;
				map.put(trackInfo.dInfo.destNo, cs);
			}else{
				cs = new Car_speed();
				cs.dest_no = trackInfo.dInfo.destNo;
				cs.cartype = trackInfo.dInfo.carType;
				cs.speed = temp/j;
				map.put(trackInfo.dInfo.destNo, cs);
			}
			
		}
        int k=0,k_red=0,k_green=0,k_electric=0,k_accessible=0;
        float speed = 0,speed_red = 0,speed_green = 0,speed_electric = 0,speed_accessible = 0,avgspeed=0,avgspeed_red=0,avgspeed_green=0,avgspeed_electric=0,avgspeed_accessible=0;
		for(Iterator it = map.keySet().iterator();it.hasNext();){
			Car_speed car = map.get(it.next());
			speed = speed+car.speed;
			if(car.cartype == 1)
			{
				speed_red = speed_red +car.speed;
				k_red++;
			}
			if(car.cartype == 2)
			{
				speed_green = speed_green +car.speed;
				k_green++;
			}
			if(car.cartype == 3)
			{
				speed_electric = speed_electric +car.speed;
				k_electric++;
			}
			if(car.cartype == 4)
			{
				speed_accessible = speed_accessible +car.speed;
				k_accessible++;
			}
			k++;
		}
		avgspeed = speed/k;
		avgspeed_red = speed_red/k_red;
		avgspeed_green = speed_green/k_green;
		avgspeed_electric = speed_electric/k_electric;
		avgspeed_accessible = speed_accessible/k_accessible;
		
		MaintainStabilityRealTimeBean msrt[] = new MaintainStabilityRealTimeBean[1];
		MaintainStabilityRealTimeBean bean = new MaintainStabilityRealTimeBean();
		bean.setUid("maintainstability");
		bean.setStopcarnum(stopcarnum);
		bean.setStopcarnum_red(stopcarnum_red);
		bean.setStopcarnum_green(stopcarnum_green);
		bean.setStopcarnum_electric(stopcarnum_electric);
		bean.setStopcarnum_accessible(stopcarnum_accessible);
		bean.setBusinessdistance(new BigDecimal(distance/total_car).setScale(5,BigDecimal.ROUND_HALF_UP).doubleValue());
		bean.setBusinessdistance_red(new BigDecimal(distance_red/car_red).setScale(5,BigDecimal.ROUND_HALF_UP).doubleValue());
		bean.setBusinessdistance_green(new BigDecimal(distance_green/car_green).setScale(5,BigDecimal.ROUND_HALF_UP).doubleValue());
		bean.setBusinessdistance_electric(new BigDecimal(distance_electric/car_electric).setScale(5,BigDecimal.ROUND_HALF_UP).doubleValue());
		bean.setBusinessdistance_accessible(new BigDecimal(distance_accessible/car_accessible).setScale(5,BigDecimal.ROUND_HALF_UP).doubleValue());
		bean.setBusinessmoney(new BigDecimal(sum/total_car).setScale(5,BigDecimal.ROUND_HALF_UP).doubleValue());
		bean.setBusinessmoney_red(new BigDecimal(sum_red/car_red).setScale(5,BigDecimal.ROUND_HALF_UP).doubleValue());
		bean.setBusinessmoney_green(new BigDecimal(sum_green/car_green).setScale(5,BigDecimal.ROUND_HALF_UP).doubleValue());
		bean.setBusinessmoney_electric(new BigDecimal(sum_electric/car_electric).setScale(5,BigDecimal.ROUND_HALF_UP).doubleValue());
		bean.setBusinessmoney_accessible(new BigDecimal(sum_accessible/car_accessible).setScale(5,BigDecimal.ROUND_HALF_UP).doubleValue());
		bean.setBusinessnum(new BigDecimal(business_num/total_car).setScale(5,BigDecimal.ROUND_HALF_UP).doubleValue());
		bean.setBusinessnum_red(new BigDecimal(business_num_red/car_red).setScale(5,BigDecimal.ROUND_HALF_UP).doubleValue());
		bean.setBusinessnum_green(new BigDecimal(business_num_green/car_green).setScale(5,BigDecimal.ROUND_HALF_UP).doubleValue());
		bean.setBusinessnum_electric(new BigDecimal(business_num_electric/car_electric).setScale(5,BigDecimal.ROUND_HALF_UP).doubleValue());
		bean.setBusinessnum_accessible(new BigDecimal(business_num_accessible/car_accessible).setScale(5,BigDecimal.ROUND_HALF_UP).doubleValue());
		bean.setSpeed(new BigDecimal(avgspeed).setScale(3,BigDecimal.ROUND_HALF_UP).doubleValue());
		bean.setSpeed_red(new BigDecimal(avgspeed_red).setScale(3,BigDecimal.ROUND_HALF_UP).doubleValue());
		bean.setSpeed_green(new BigDecimal(avgspeed_green).setScale(3,BigDecimal.ROUND_HALF_UP).doubleValue());
		bean.setSpeed_electric(new BigDecimal(avgspeed_electric).setScale(3,BigDecimal.ROUND_HALF_UP).doubleValue());
		bean.setSpeed_accessible(new BigDecimal(avgspeed_accessible).setScale(3,BigDecimal.ROUND_HALF_UP).doubleValue());
		msrt[0] = bean;
		this.deleteInfo();
		RedisConnPoolServer.getSingleInstance().saveTableRecord(msrt);
		return false;
	}
	
	private void deleteInfo() throws Exception {
		MaintainStabilityRealTimeBean bean = new MaintainStabilityRealTimeBean();
		Set<String> keySet = RedisConnPoolServer.getSingleInstance()
				.getAllIdsForTable(bean);
		Iterator<String> it = keySet.iterator();
		String uid = "";
		List<MaintainStabilityRealTimeBean> list = new ArrayList<MaintainStabilityRealTimeBean>();
		while (it.hasNext()) {
			bean = new MaintainStabilityRealTimeBean();
			uid = it.next();
			bean.setUid(uid);
			list.add(bean);
		}

		if (list.size() > 0) {
			RedisTableBean[] beanArr = new MaintainStabilityRealTimeBean[list.size()];
			list.toArray(beanArr);
			RedisConnPoolServer.getSingleInstance().deleteTableRecord(beanArr);
		}
	}
	
	
	private class Car_speed{
		String dest_no;
		int cartype;
		float speed;
	}
	
}


