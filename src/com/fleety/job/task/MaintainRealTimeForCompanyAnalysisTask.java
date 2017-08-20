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
import java.util.Map;
import java.util.Set;

import com.fleety.analysis.realtime.MaintainStabilityRealTimeCompanyBean;
import com.fleety.analysis.track.DestInfo;
import com.fleety.analysis.track.TrackInfo;
import com.fleety.base.InfoContainer;
import com.fleety.track.TrackIO;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.fleety.util.pool.db.redis.RedisConnPoolServer;
import com.fleety.util.pool.db.redis.RedisTableBean;
import com.fleety.util.pool.thread.BasicTask;

import server.db.DbServer;
import server.track.TrackServer;

public class MaintainRealTimeForCompanyAnalysisTask extends BasicTask {
	int company_count=0;
	@Override
	public boolean execute() throws Exception {
		HashMap<String, Car_speed> map = new HashMap<String, Car_speed>();
		HashMap<Integer, CompanyType> resMap = new HashMap<Integer, CompanyType>(); // 保存一个企业的不同车型的营运次数，营运里程，营运金额
		DbHandle conn = DbServer.getSingleInstance().getConn();

		// 获取所有公司的id,添加进集合中
		StatementHandle stm = conn.prepareStatement("select term_id from term");
		ResultSet r = stm.executeQuery();
		while (r.next()) {
			CompanyType type = new CompanyType();
			int d = r.getInt("term_id");
			type.setCompany_id(d);
			resMap.put(d, type);
			company_count++;
		}
		// 获取每个公司车辆总数
		try {
			StatementHandle stmt = conn.createStatement();
			String sql = "select count(*) total_car,company_id,type_id from v_ana_dest_info group by type_id,company_id";
			ResultSet sets = stmt.executeQuery(sql);
			while (sets.next()) {
				int c_id = sets.getInt("company_id");
				if(resMap.get(c_id)==null)//排除企业表中不存在，但是车辆信息中却又车辆信息的情况
					continue;
				if (sets.getInt("type_id") == 1)
					resMap.get(c_id).red_num = sets.getInt("total_car");
				else if (sets.getInt("type_id") == 2)
					resMap.get(c_id).green_num = sets.getInt("total_car");
				else if (sets.getInt("type_id") == 3)
					resMap.get(c_id).electric_num = sets.getInt("total_car");
				else if (sets.getInt("type_id") == 4)
					resMap.get(c_id).accessible_num = sets.getInt("total_car");
				else
					resMap.get(c_id).other_num = sets.getInt("total_car");
			}
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("数据库操作失败！");
		}
		// 统计当前5分钟营运数据信息
		StatementHandle stmt = conn.prepareStatement(
				"select count(*) business_num,company_id,sum(distance) distance,sum(sum) sum,type_id from single_business_data_bs sbd left join v_ana_dest_info va on sbd.car_no = va.dest_no  where date_down >= sysdate-5/(60*24) and date_down <= sysdate  group by type_id,company_id");
		ResultSet rs = stmt.executeQuery();
		while (rs.next()) {
			int com_id = rs.getInt("company_id");
			if (rs.getInt("type_id") == 1)// 红的
			{
				resMap.get(com_id).setBusiness_num_red(rs.getFloat("business_num"));
				resMap.get(com_id).setDistance_red(rs.getFloat("distance"));
				resMap.get(com_id).setSum_red(rs.getFloat("sum"));

			} else if (rs.getInt("type_id") == 2) {
				resMap.get(com_id).setBusiness_num_green(rs.getFloat("business_num"));
				resMap.get(com_id).setDistance_green(rs.getFloat("distance"));
				resMap.get(com_id).setSum_green(rs.getFloat("sum"));
			} else if (rs.getInt("type_id") == 3) {
				resMap.get(com_id).setBusiness_num_electric(rs.getFloat("business_num"));
				resMap.get(com_id).setDistance_electric(rs.getFloat("distance"));
				resMap.get(com_id).setSum_electric(rs.getFloat("sum"));
			} else if (rs.getInt("type_id") == 4) {
				resMap.get(com_id).setBusiness_num_accessible(rs.getFloat("business_num"));
				resMap.get(com_id).setDistance_accessible(rs.getFloat("distance"));
				resMap.get(com_id).setSum_accessible(rs.getFloat("sum"));
			} else {// 电动绿的
				resMap.get(com_id).setBusiness_num_other(rs.getFloat("business_num"));
				resMap.get(com_id).setDistance_other(rs.getFloat("distance"));
				resMap.get(com_id).setSum_other(rs.getFloat("sum"));
			}
		}

		// 统计当前整分时间5分钟车辆速度一直为0的车辆数

		Calendar now = Calendar.getInstance();// 当前时间
		Calendar start = Calendar.getInstance();

		now.set(Calendar.SECOND, 0);
		now.set(Calendar.MILLISECOND, 0);

		start.add(Calendar.MINUTE, -5);
		start.set(Calendar.SECOND, 0);
		start.set(Calendar.MILLISECOND, 0);

		DestInfo dInfo;
		ArrayList destList = new ArrayList(1024);
		try {
			stmt = conn.prepareStatement(
					"select mdt_id,dest_no,company_id,company_name,type_id,gps_run_com_id,gps_run_com_name from v_ana_dest_info");
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
		} catch (Exception e) {
			throw e;
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}
		InfoContainer queryInfo = new InfoContainer();

		Date sDate = start.getTime();
		Date eDate = now.getTime();
		queryInfo.setInfo(TrackServer.START_DATE_FLAG, sDate);
		queryInfo.setInfo(TrackServer.END_DATE_FLAG, eDate);
		TrackInfo trackInfo;
		int company = 0;
		for (Iterator itr = destList.iterator(); itr.hasNext();) {
			dInfo = (DestInfo) itr.next();
			queryInfo.setInfo(TrackServer.DEST_NO_FLAG, dInfo.destNo);
			company = dInfo.companyId;
			trackInfo = new TrackInfo();
			trackInfo.dInfo = dInfo;
			trackInfo.sDate = sDate;
			trackInfo.eDate = eDate;
			trackInfo.trackArr = TrackServer.getSingleInstance().getTrackInfo(queryInfo);
			if (trackInfo.trackArr.length == 0) {
//				resMap.get(company).stopcarnum++;// 没有轨迹信息，记为停驶车辆
				continue;
			}
			// 有轨迹信息车辆判断其速度
			for (int i = 0; i < trackInfo.trackArr.length; i++) {
				if (trackInfo.trackArr[i].getInteger(TrackIO.DEST_SPEED_FLAG) != 0)
					break;// 有速度不做判断，这段轨迹内车辆有速度。
				if (i == trackInfo.trackArr.length - 1)// 直到最后一个轨迹点都没有速度记为停驶车辆
				{
					resMap.get(company).stopcarnum++;// 停驶总车数
					if (trackInfo.dInfo.carType == 1)
						resMap.get(company).stopcarnum_red++;
					if (trackInfo.dInfo.carType == 2)
						resMap.get(company).stopcarnum_green++;
					if (trackInfo.dInfo.carType == 3)
						resMap.get(company).stopcarnum_electric++;
					if (trackInfo.dInfo.carType == 4)
						resMap.get(company).stopcarnum_accessible++;
				}
			}
			float temp = 0;// 记录该辆车这段轨迹的速度之和
			int j;// 轨迹数组长度
			for (j = 0; j < trackInfo.trackArr.length; j++) {
				temp = temp + trackInfo.trackArr[j].getInteger(TrackIO.DEST_SPEED_FLAG);
			}
			Car_speed cs = null;// 车速度类，保存一辆车的车型，公司，平均速度
			if (map.containsKey(trackInfo.dInfo.destNo)) {
				cs = map.get(trackInfo.dInfo.destNo);
				cs.speed = temp / j;
				cs.com_id = company;
				cs.cartype = trackInfo.dInfo.carType;
				map.put(trackInfo.dInfo.destNo, cs);
			} else {
				cs = new Car_speed();
				cs.dest_no = trackInfo.dInfo.destNo;
				cs.cartype = trackInfo.dInfo.carType;
				cs.com_id = trackInfo.dInfo.companyId;
				cs.speed = temp / j;
				map.put(trackInfo.dInfo.destNo, cs);
			}
		}
		HashMap<Integer, SpeedInfo> speedmap = new HashMap<Integer, SpeedInfo>();// 包含了每个公司的各车辆类型的速度之和及车辆数
		for (Iterator it = map.keySet().iterator(); it.hasNext();) {
			Car_speed car = map.get(it.next());
			int id = car.com_id;
			SpeedInfo info;
			info = speedmap.get(id);
			if (info == null) {
				info = new SpeedInfo();
				info.com_id = id;
				
			}
			if (car.cartype == 1) {
				info.speed_red += car.speed;
				info.k_red++;
			}
			if (car.cartype == 2) {
				info.speed_green += car.speed;
				info.k_green++;
			}
			if (car.cartype == 3) {
				info.speed_electric += car.speed;
				info.k_electric++;
			}
			if (car.cartype == 4) {
				info.speed_accessible += car.speed;
				info.k_accessible++;
			}
			info.k++;// 该公司的总车数
			info.speed += car.speed;// 所有车的速度之和
			speedmap.put(id, info);
		}
		// 将每个公司的总速度和平均速度得出
		for (Iterator it = speedmap.keySet().iterator(); it.hasNext();) {
			SpeedInfo speed1 = speedmap.get(it.next());
			int id = speed1.com_id;
			if (speed1.k != 0)
				resMap.get(id).setAvgspeed(speed1.speed / speed1.k);// 公司整体
			else if (speed1.k_red != 0)
				resMap.get(id).setAvgspeed_red(speed1.speed_red / speed1.k_red);
			else if (speed1.k_green != 0)
				resMap.get(id).setAvgspeed_green(speed1.speed_green / speed1.k_green);
			else if (speed1.k_electric != 0)
				resMap.get(id).setAvgspeed_electric(speed1.speed_electric / speed1.k_electric);
			else if (speed1.k_accessible != 0)
				resMap.get(id).setAvgspeed_accessible(speed1.speed_accessible / speed1.k_accessible);
		}
		
		//将数据存到redis
		int i = 0;
		MaintainStabilityRealTimeCompanyBean msrt[] = new MaintainStabilityRealTimeCompanyBean[company_count];
		for (Iterator it = resMap.keySet().iterator(); it.hasNext();) {// resMap中存的是公司id为key,数据对象为value的键值对

			CompanyType comtype = resMap.get(it.next());
			int car_count = comtype.red_num + comtype.green_num + comtype.electric_num + comtype.accessible_num
					+ comtype.other_num;
			float all_money = comtype.getSum_accessible() + comtype.getSum_electric() + comtype.getSum_green()
					+ comtype.getSum_red() + comtype.getSum_other();
			float all_distance = comtype.getDistance_accessible() + comtype.getDistance_electric()
					+ comtype.getDistance_green() + comtype.getDistance_other() + comtype.getDistance_red();
			float all_businessNum = comtype.getBusiness_num_red() + comtype.getBusiness_num_other()
					+ comtype.getBusiness_num_green() + comtype.getBusiness_num_electric()
					+ comtype.getBusiness_num_accessible();
			comtype.all_num = car_count;
			MaintainStabilityRealTimeCompanyBean bean = new MaintainStabilityRealTimeCompanyBean();
			bean.setUid("company_"+comtype.getCompany_id());
			bean.setCompany_id(comtype.getCompany_id());
			bean.setStopcarnum(comtype.getStopcarnum());
			bean.setStopcarnum_red(comtype.getStopcarnum_red());
			bean.setStopcarnum_green(comtype.getStopcarnum_green());
			bean.setStopcarnum_electric(comtype.getStopcarnum_electric());
			bean.setStopcarnum_accessible(comtype.getStopcarnum_accessible());
			if(car_count!=0){
				bean.setBusinessdistance(
						new BigDecimal(all_distance / car_count).setScale(5, BigDecimal.ROUND_HALF_UP).doubleValue());
				bean.setBusinessnum(
						new BigDecimal(all_businessNum / car_count).setScale(5, BigDecimal.ROUND_HALF_UP).doubleValue());
				bean.setBusinessmoney(
						new BigDecimal(all_money / car_count).setScale(5, BigDecimal.ROUND_HALF_UP).doubleValue());
			}
			if(comtype.red_num!=0){
			bean.setBusinessdistance_red(new BigDecimal(comtype.getDistance_red() / comtype.red_num)
					.setScale(5, BigDecimal.ROUND_HALF_UP).doubleValue());
			bean.setBusinessmoney_red(new BigDecimal(comtype.getSum_red() /comtype.red_num)
					.setScale(5, BigDecimal.ROUND_HALF_UP).doubleValue());
			bean.setBusinessnum_red(new BigDecimal(comtype.getBusiness_num_red() / comtype.red_num)
					.setScale(5, BigDecimal.ROUND_HALF_UP).doubleValue());
			}
			else if(comtype.green_num!=0){
			bean.setBusinessdistance_green(new BigDecimal(comtype.getDistance_green() / comtype.green_num)
					.setScale(5, BigDecimal.ROUND_HALF_UP).doubleValue());
			bean.setBusinessmoney_green(new BigDecimal(comtype.getSum_green() / comtype.green_num)
					.setScale(5, BigDecimal.ROUND_HALF_UP).doubleValue());
			bean.setBusinessnum_green(new BigDecimal(comtype.getBusiness_num_green() / comtype.green_num)
					.setScale(5, BigDecimal.ROUND_HALF_UP).doubleValue());
			}
			else if(comtype.electric_num!=0){
			bean.setBusinessdistance_electric(new BigDecimal(comtype.getDistance_electric() / comtype.electric_num)
					.setScale(5, BigDecimal.ROUND_HALF_UP).doubleValue());
			bean.setBusinessmoney_electric(new BigDecimal(comtype.getSum_electric() / comtype.electric_num)
					.setScale(5, BigDecimal.ROUND_HALF_UP).doubleValue());
			bean.setBusinessnum_electric(new BigDecimal(comtype.getBusiness_num_electric() / comtype.electric_num)
					.setScale(5, BigDecimal.ROUND_HALF_UP).doubleValue());
			}
			else if(comtype.accessible_num!=0){
			bean.setBusinessdistance_accessible(
					new BigDecimal(comtype.getDistance_accessible() / comtype.accessible_num)
							.setScale(5, BigDecimal.ROUND_HALF_UP).doubleValue());
			bean.setBusinessmoney_accessible(new BigDecimal(comtype.getSum_accessible() / comtype.accessible_num)
					.setScale(5, BigDecimal.ROUND_HALF_UP).doubleValue());
			bean.setBusinessnum_accessible(new BigDecimal(comtype.getBusiness_num_accessible() / comtype.accessible_num)
					.setScale(5, BigDecimal.ROUND_HALF_UP).doubleValue());
			}
			bean.setSpeed(new BigDecimal(comtype.getAvgspeed()).setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue());
			bean.setSpeed_red(
					new BigDecimal(comtype.getAvgspeed_red()).setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue());
			bean.setSpeed_green(
					new BigDecimal(comtype.getAvgspeed_green()).setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue());
			bean.setSpeed_electric(
					new BigDecimal(comtype.getAvgspeed_electric()).setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue());
			bean.setSpeed_accessible(new BigDecimal(comtype.getAvgspeed_accessible())
					.setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue());
			
			msrt[i] = bean;
			i++;
		}
		this.deleteInfo();
		RedisConnPoolServer.getSingleInstance().saveTableRecord(msrt);
		return false;
	}

	private void deleteInfo() throws Exception {
		MaintainStabilityRealTimeCompanyBean bean = new MaintainStabilityRealTimeCompanyBean();
		Set<String> keySet = RedisConnPoolServer.getSingleInstance().getAllIdsForTable(bean);
		Iterator<String> it = keySet.iterator();
		String uid = "";
		List<MaintainStabilityRealTimeCompanyBean> list = new ArrayList<MaintainStabilityRealTimeCompanyBean>();
		while (it.hasNext()) {
			bean = new MaintainStabilityRealTimeCompanyBean();
			uid = it.next();
			bean.setUid(uid);
			list.add(bean);
		}

		if (list.size() > 0) {
			RedisTableBean[] beanArr = new MaintainStabilityRealTimeCompanyBean[list.size()];
			list.toArray(beanArr);
			RedisConnPoolServer.getSingleInstance().deleteTableRecord(beanArr);
		}
	}

	private class Car_speed {
		String dest_no;
		int com_id;
		int cartype;
		float speed;
	}
	private class SpeedInfo {
		int com_id = 0;
		float speed = 0, speed_red = 0, speed_green = 0, speed_electric = 0, speed_accessible = 0;
		int k = 0, k_red = 0, k_green = 0, k_electric = 0, k_accessible = 0;
	}
	private class CompanyType {
		int company_id = 0;
		float business_num_other = 0, distance_other = 0, sum_other = 0;
		float business_num_red = 0, distance_red = 0, sum_red = 0;
		float business_num_green = 0, distance_green = 0, sum_green = 0;
		float business_num_electric = 0, distance_electric = 0, sum_electric = 0;
		float business_num_accessible = 0, distance_accessible = 0, sum_accessible = 0;

		int stopcarnum = 0, stopcarnum_red = 0, stopcarnum_green = 0, stopcarnum_electric = 0,
				stopcarnum_accessible = 0;// 停驶数量
		float avgspeed = 0, avgspeed_red = 0, avgspeed_green = 0, avgspeed_electric = 0, avgspeed_accessible = 0;// 平均速度
		int all_num = 0, red_num = 0, green_num = 0, electric_num = 0, accessible_num = 0, other_num;// 车辆数

		public int getCompany_id() {
			return company_id;
		}
		public void setCompany_id(int company_id) {
			this.company_id = company_id;
		}
		public float getBusiness_num_other() {
			return business_num_other;
		}
		public void setBusiness_num_other(float business_num_other) {
			this.business_num_other = business_num_other;
		}
		public float getDistance_other() {
			return distance_other;
		}
		public void setDistance_other(float distance_other) {
			this.distance_other = distance_other;
		}

		public float getSum_other() {
			return sum_other;
		}

		public void setSum_other(float sum_other) {
			this.sum_other = sum_other;
		}

		public float getBusiness_num_red() {
			return business_num_red;
		}

		public void setBusiness_num_red(float business_num_red) {
			this.business_num_red = business_num_red;
		}

		public float getDistance_red() {
			return distance_red;
		}

		public void setDistance_red(float distance_red) {
			this.distance_red = distance_red;
		}

		public float getSum_red() {
			return sum_red;
		}

		public void setSum_red(float sum_red) {
			this.sum_red = sum_red;
		}

		public float getBusiness_num_green() {
			return business_num_green;
		}

		public void setBusiness_num_green(float business_num_green) {
			this.business_num_green = business_num_green;
		}

		public float getDistance_green() {
			return distance_green;
		}

		public void setDistance_green(float distance_green) {
			this.distance_green = distance_green;
		}

		public float getSum_green() {
			return sum_green;
		}

		public void setSum_green(float sum_green) {
			this.sum_green = sum_green;
		}

		public float getBusiness_num_electric() {
			return business_num_electric;
		}

		public void setBusiness_num_electric(float business_num_electric) {
			this.business_num_electric = business_num_electric;
		}

		public float getDistance_electric() {
			return distance_electric;
		}

		public void setDistance_electric(float distance_electric) {
			this.distance_electric = distance_electric;
		}

		public float getSum_electric() {
			return sum_electric;
		}

		public void setSum_electric(float sum_electric) {
			this.sum_electric = sum_electric;
		}

		public float getBusiness_num_accessible() {
			return business_num_accessible;
		}

		public void setBusiness_num_accessible(float business_num_accessible) {
			this.business_num_accessible = business_num_accessible;
		}

		public float getDistance_accessible() {
			return distance_accessible;
		}

		public void setDistance_accessible(float distance_accessible) {
			this.distance_accessible = distance_accessible;
		}

		public float getSum_accessible() {
			return sum_accessible;
		}

		public void setSum_accessible(float sum_accessible) {
			this.sum_accessible = sum_accessible;
		}

		public int getStopcarnum() {
			return stopcarnum;
		}

		public void setStopcarnum(int stopcarnum) {
			this.stopcarnum = stopcarnum;
		}

		public int getStopcarnum_red() {
			return stopcarnum_red;
		}

		public void setStopcarnum_red(int stopcarnum_red) {
			this.stopcarnum_red = stopcarnum_red;
		}

		public int getStopcarnum_green() {
			return stopcarnum_green;
		}

		public void setStopcarnum_green(int stopcarnum_green) {
			this.stopcarnum_green = stopcarnum_green;
		}

		public int getStopcarnum_electric() {
			return stopcarnum_electric;
		}

		public void setStopcarnum_electric(int stopcarnum_electric) {
			this.stopcarnum_electric = stopcarnum_electric;
		}

		public int getStopcarnum_accessible() {
			return stopcarnum_accessible;
		}

		public void setStopcarnum_accessible(int stopcarnum_accessible) {
			this.stopcarnum_accessible = stopcarnum_accessible;
		}

		public float getAvgspeed() {
			return avgspeed;
		}

		public void setAvgspeed(float avgspeed) {
			this.avgspeed = avgspeed;
		}

		public float getAvgspeed_red() {
			return avgspeed_red;
		}

		public void setAvgspeed_red(float avgspeed_red) {
			this.avgspeed_red = avgspeed_red;
		}

		public float getAvgspeed_green() {
			return avgspeed_green;
		}

		public void setAvgspeed_green(float avgspeed_green) {
			this.avgspeed_green = avgspeed_green;
		}

		public float getAvgspeed_electric() {
			return avgspeed_electric;
		}

		public void setAvgspeed_electric(float avgspeed_electric) {
			this.avgspeed_electric = avgspeed_electric;
		}

		public float getAvgspeed_accessible() {
			return avgspeed_accessible;
		}

		public void setAvgspeed_accessible(float avgspeed_accessible) {
			this.avgspeed_accessible = avgspeed_accessible;
		}

	}
}
