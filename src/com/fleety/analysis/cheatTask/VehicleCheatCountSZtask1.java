package com.fleety.analysis.cheatTask;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import server.db.DbServer;
import server.track.TrackServer;

import com.fleety.analysis.AnalysisServer;
import com.fleety.analysis.operation.IOperationAnalysis;
import com.fleety.base.InfoContainer;
import com.fleety.track.TrackIO;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;

public class VehicleCheatCountSZtask1 implements IOperationAnalysis {

	private HashMap<String, StandardInfo> StandardInfoMapping = null;
	private HashMap<String, CheatAnaResInfo> ResIfoMapping = null;
	private HashMap<String, OpreationInfo1> resMap = null;
	
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");

	@Override
	public boolean startAnalysisOperation(AnalysisServer parentServer,
			InfoContainer statInfo) {
		
		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
		Date eTime = statInfo.getDate(STAT_END_TIME_DATE);
		this.StandardInfoMapping = null;
		this.ResIfoMapping = null;
		this.resMap=null;
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			StatementHandle stmt = conn.createStatement();
			StringBuilder sb = new StringBuilder();
			sb.append("select count(*) as sum from GPS_BUSINESS_ANALYSIS")
					.append(" where record_time >= to_date('")
					.append(sdf2.format(sTime)).append("','yyyy-MM-dd')");
			System.out.println("判定里程比对表中是否有数据："+sb.toString());
			ResultSet sets = stmt.executeQuery(sb.toString());
			if (sets.next()) {
				int sum = sets.getInt("sum");
				if (sum == 0) {
					this.ResIfoMapping = new HashMap();
					this.StandardInfoMapping = new HashMap();
					this.resMap=new HashMap();
				}
			}//select count(*) as sum from GPS_BUSINESS_ANALYSIS where record_time >=to_date('2017-05-09','yyyy-mm-dd')
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}

		if (this.StandardInfoMapping == null) {
			System.out.println("VehicleCheatCountSZtask1 Not Need Analysis:"
					+ this.toString());
		} else {
			System.out.println("VehicleCheatCountSZtask1 Start Analysis:"
					+ this.toString());
		}

		return this.StandardInfoMapping != null;
	}

	@Override
	public void analysisDestOperation(AnalysisServer parentServer,
			InfoContainer statInfo) {
		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
		Date eTime = statInfo.getDate(STAT_END_TIME_DATE);
		
		/****gps里程结算结果分析数据保存GPS_BUSINESS_ANALYSIS表中****/
		DbHandle conn = DbServer.getSingleInstance().getConnWithUseTime(-1);
		try {
			StatementHandle stm = conn.createStatement();		
			String s_str = sdf.format(sTime);
			String e_str = sdf.format(eTime);

			String sql = "select CAR_NO,DATE_UP,DATE_DOWN,DISTANCE,FREE_DISTANCE,SUM,RECODE_TIME from SINGLE_BUSINESS_DATA_BS where DATE_UP>=to_date('"
					+ s_str+ "','yyyy-mm-dd hh24:mi:ss') and DATE_UP<=to_date('"+ e_str+ "','yyyy-mm-dd hh24:mi:ss')";
			System.out.println("GPSmileana_time:" + s_str + " " + e_str
					+ " \n  GPSmileana_sql:" + sql);
			OpreationInfo1 info = null;
			int count = 0;
			ResultSet res;

			res = stm.executeQuery(sql);

			while (res.next()) {
				count++;
				if(count%500==0){
					System.out.println("作弊数据分析进度1..."+count);
				}
				info = new OpreationInfo1();
				String car_no = res.getString(1);
				info.car_no = car_no;
				info.date_up = res.getTimestamp(2);// 营运开始时间
				info.date_down = res.getTimestamp(3);
				info.record_time = res.getTimestamp(7);
				info.distance = res.getDouble(4);
				info.sum = res.getDouble(6);
				List<Double> reslist = analysisTrack(info.date_up,
						info.date_down, car_no);
				info.gps_distance = round(reslist.get(0), 2);// GPS计算的营运里程
				info.gps_points = reslist.get(1);

				info.area_size = reslist.get(2);
				info.empty_points = reslist.get(3);
				info.load_points = reslist.get(4);
				info.task_points = reslist.get(5);
				info.other_points = reslist.get(6);
				info.un_fit = reslist.get(7);
				info.operate_time = (int) ((info.date_down.getTime() - info.date_up
						.getTime()) / 60000);
				resMap.put(car_no, info);
			}
			res.close();
			System.out.println("GPS里程分析查询到数据条数：" + count);
			if (count != 0) {
				StatementHandle stmt = conn.prepareStatement("insert into GPS_BUSINESS_ANALYSIS (id ,car_no ,date_up ,date_down ,sum ,gps_distance,distance,gps_points,area_size,empty_points,load_points,task_points,other_points,operate_time,record_time,un_fit ) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
				for (Iterator it = resMap.keySet().iterator(); it.hasNext();) {

					OpreationInfo1 op = resMap.get(it.next());
					stmt.setInt(1,(int) DbServer.getSingleInstance().getAvaliableId(conn, "GPS_BUSINESS_ANALYSIS", "id"));
					stmt.setString(2, op.car_no);
					stmt.setTimestamp(3, new Timestamp(op.date_up.getTime()));
					stmt.setTimestamp(4, new Timestamp(op.date_down.getTime()));
					stmt.setDouble(5, op.sum);
					stmt.setDouble(6, op.gps_distance);
					stmt.setDouble(7, op.distance);
					stmt.setDouble(8, op.gps_points);
					stmt.setDouble(9, op.area_size);
					stmt.setDouble(10, op.empty_points);
					stmt.setDouble(11, op.load_points);
					stmt.setDouble(12, op.task_points);
					stmt.setDouble(13, op.other_points);
					stmt.setDouble(14, op.operate_time);
					stmt.setTimestamp(15,new Timestamp(op.record_time.getTime()));
					stmt.setDouble(16,op.un_fit);
					stmt.addBatch();
					try {
						stmt.executeBatch();
					} catch (Exception e) {
						System.out.println("data exption" + op.toString());
					}
				}
				conn.commit();

			}
		} catch (SQLException e1) {

			e1.printStackTrace();
		} catch (Exception e) {

			e.printStackTrace();
		}
		DbServer.getSingleInstance().releaseConn(conn);

		/**** 新标准第二次数据校验 ******/
		this.StandardInfoMapping = new HashMap();
		this.ResIfoMapping = new HashMap();
		

		DbHandle conn1 = DbServer.getSingleInstance().getConnWithUseTime(0);
		try {
			// 获取标准sql 对误差率小于百分之五的数据统计。得到白天或者晚上，各车型，在各营运时长下的，营运次数，最大金额，最大里程。
			StringBuilder sql1 = new StringBuilder();
			sql1.append(
					"select business_time,count(*) as times,  max(DISTANCE) as max_distince,max(sum) as max_sum,type_id as car_type,hour_type from (select car_no, sum,DISTANCE,case when to_char(DATE_UP,'hh24')>6 and to_char(DATE_UP,'hh24')<=23 then 1 else 0 end hour_type, round((DATE_DOWN - DATE_UP)*60*24,0) as business_time,b.type_id from GPS_BUSINESS_ANALYSIS a left join car b on a.car_no=b.car_id where  DISTANCE >0 ")
					.append(" and date_up >= to_date('")
					.append(sdf.format(sTime))
					.append("','yyyy-mm-dd hh24:mi:ss')")
					.append(" and date_up <= to_date('")
					.append(sdf.format(eTime))
					.append("','yyyy-mm-dd hh24:mi:ss')")
					.append("and   abs( round( (DISTANCE-gps_distance)/ DISTANCE, 2)) < 0.05 ) group by business_time,type_id,hour_type");
			System.out.println("统计作弊标准数据sql" + sql1.toString());

			StatementHandle stmt1 = conn1.prepareStatement(sql1.toString());
			ResultSet res1 = stmt1.executeQuery();
			StandardInfo standInfo;
			while (res1.next()) {
				double operator_time = res1.getDouble("business_time");
				int hour_type = res1.getInt("hour_type");
				int car_type = res1.getInt("car_type");

				String key = car_type + "_" + hour_type + "_" + operator_time;
				if (!StandardInfoMapping.containsKey(key)) {
					standInfo = new StandardInfo();
					standInfo.setOperator_time(operator_time);
					standInfo.setHour_type(hour_type);
					standInfo.setCar_type(car_type);
					standInfo.setMax_DISTANCE(res1.getDouble("max_distince"));
					standInfo.setMax_sum(res1.getDouble("max_sum"));
					standInfo.setOperator_nums(res1.getDouble("times"));
					StandardInfoMapping.put(key, standInfo);
				}
			}
			res1.close();
			System.out.println("作弊标准数据条数" + StandardInfoMapping.size());

			// 获取营运数据 将营运数据
			/*
			 * 以天为单位，遍历所有的营运数据。逐条获取营运时长，车型，白班晚班信息。然后去标准数据中寻找对应数据。如：当前营运数据营运20分钟，
			 * 里程30公里，金额
			 * 45元。标准数据中营运时长20分钟，最大里程为28公里，最大金额50块。则判定该条营运数据不合理。（必须同时在两个最大标准之内才合格
			 * ）。反之则判定合理；如 果标准数据中没有营运时长20分钟的，则无标准，跳如下一条数据判断。（sum = 0 distance =
			 * 0 sum >200 distance >500的记录就不分析了）
			 */
			StringBuilder sql2 = new StringBuilder();
			sql2.append(
					"select s.id,s.car_no,round((s.DATE_DOWN - s.DATE_UP)*60*24,0) operator_time,s.distance,s.sum,case when to_char(DATE_UP,'hh24')>6 and to_char(DATE_UP,'hh24')<=23 then 1 else 0 end hour_type,to_char(s.date_up,'yyyy-mm-dd hh24:mi:ss') date_up,to_char(s.date_down,'yyyy-mm-dd hh24:mi:ss') date_down,c.type_id car_type from SINGLE_BUSINESS_DATA_BS s left join car c on s.car_no=c.car_id where 1=1 ")
					.append(" and s.date_up >= to_date('")
					.append(sdf.format(sTime))
					.append("','yyyy-mm-dd hh24:mi:ss')")
					.append(" and s.date_up <= to_date('")
					.append(sdf.format(eTime))
					.append("','yyyy-mm-dd hh24:mi:ss')");
			System.out.println("sql2:" + sql2.toString());
			StatementHandle stmt2 = conn1.prepareStatement(sql2.toString());
			ResultSet res2 = stmt2.executeQuery();
			CheatAnaResInfo cheatRes;
			int count1 = 0;
			int ninght = 0;
			int day = 0;
			while (res2.next()) {
				double operator_time = res2.getDouble("operator_time");
				int hour_type = res2.getInt("hour_type");
				int car_type = res2.getInt("car_type");
				String key = car_type + "_" + hour_type + "_" + operator_time;
				if (StandardInfoMapping.containsKey(key)) {
					count1++;
					cheatRes = new CheatAnaResInfo();
					cheatRes.setOp_id(res2.getLong(1));
					cheatRes.setCar_no(res2.getString(2));
					cheatRes.setOperator_time(res2.getDouble(3));
					cheatRes.setDISTANCE(res2.getDouble(4));
					cheatRes.setSum(res2.getDouble(5));
					cheatRes.setHour_type(res2.getInt(6));
					if (res2.getInt(6) == 0) {
						ninght++;
					} else {
						day++;
					}
					cheatRes.setDate_up(res2.getString(7));
					cheatRes.setDate_down(res2.getString(8));
					cheatRes.setCar_type(res2.getInt(9));
					standInfo = StandardInfoMapping.get(key);
					cheatRes.setMax__distance(standInfo.getMax_DISTANCE());
					cheatRes.setMax__sum(standInfo.getMax_sum());
					cheatRes.setOperator_nums(standInfo.getOperator_nums());
					if (res2.getDouble(4) > standInfo.getMax_DISTANCE()
							|| res2.getDouble(5) > standInfo.getMax_sum()) {
						cheatRes.setIs_fit(0);
					} else {
						cheatRes.setIs_fit(1);
					}
					ResIfoMapping.put(res2.getString(1), cheatRes);
				}

			}
			res2.close();
			System.out.println("VehicleCheatCountSZtask anasql: 判断分析了 "
					+ count1 + " 条营运数据 day:+" + day + " night:" + ninght + " "
					+ sql2.toString());

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DbServer.getSingleInstance().releaseConn(conn1);
		}

	}

	@Override
	public void endAnalysisOperation(AnalysisServer parentServer,
			InfoContainer statInfo) {
		System.out.println("VehicleCheatCountSZtask 准备插入数据量"
				+ ResIfoMapping.size());
		if (this.ResIfoMapping == null || StandardInfoMapping == null) {
			return;
		}
		int recordNum = 0;
		CheatAnaResInfo cheatRes;
		Date sDate = statInfo.getDate(STAT_START_TIME_DATE);
		DbHandle conn = DbServer.getSingleInstance().getConnWithUseTime(0);
		DbHandle conn2 = DbServer.getSingleInstance().getConnWithUseTime(0);
		try {

			conn2.setAutoCommit(false);
			StandardInfo standInfo;
			// 保存标准数据
			StatementHandle stmt2 = conn2
					.prepareStatement("insert into cheat_Standard_date(id,car_type,max_DISTANCE,operator_time,hour_type,operator_nums,record_date,max_sum)values(?,?,?,?,?,?,?,?)");
			for (Iterator itr = this.StandardInfoMapping.keySet().iterator(); itr
					.hasNext();) {
				standInfo = StandardInfoMapping.get(itr.next());
				stmt2.setInt(1, (int) DbServer.getSingleInstance()
						.getAvaliableId(conn, "cheat_Standard_date", "id"));
				stmt2.setInt(2, standInfo.getCar_type());
				stmt2.setDouble(3, standInfo.getMax_DISTANCE());
				stmt2.setDouble(4, standInfo.getOperator_time());
				stmt2.setInt(5, standInfo.getHour_type());
				stmt2.setDouble(6, standInfo.getOperator_nums());
				stmt2.setTimestamp(7, new Timestamp(new Date().getTime()));
				stmt2.setDouble(8, standInfo.getMax_sum());
				stmt2.addBatch();
				if (recordNum % 200 == 0) {
					stmt2.executeBatch();
				}
			}
			stmt2.executeBatch();
			conn2.commit();

			// 保存分析结果
			conn.setAutoCommit(false);
			StatementHandle stmt = conn
					.prepareStatement("insert into CheatAna_Record (id,op_id,car_no,car_type,date_up,date_down,distance,sum,operator_time,hour_type,max_distance,max_sum,operator_nums,is_fit,record_date) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
			for (Iterator itr = this.ResIfoMapping.keySet().iterator(); itr
					.hasNext();) {
				cheatRes = ResIfoMapping.get(itr.next());

				stmt.setInt(1, (int) DbServer.getSingleInstance()
						.getAvaliableId(conn, "CheatAna_Record", "id"));
				stmt.setLong(2, cheatRes.getOp_id());
				stmt.setString(3, cheatRes.getCar_no());
				stmt.setInt(4, cheatRes.getCar_type());
				stmt.setTimestamp(5,
						new Timestamp(sdf.parse(cheatRes.getDate_up())
								.getTime()));
				stmt.setTimestamp(6,
						new Timestamp(sdf.parse(cheatRes.getDate_down())
								.getTime()));
				stmt.setDouble(7, cheatRes.getDISTANCE());
				stmt.setDouble(8, cheatRes.getSum());
				stmt.setDouble(9, cheatRes.getOperator_time());
				stmt.setInt(10, cheatRes.getHour_type());
				stmt.setDouble(11, cheatRes.getMax__distance());
				stmt.setDouble(12, cheatRes.getMax__sum());
				stmt.setDouble(13, cheatRes.getOperator_nums());
				stmt.setDouble(14, cheatRes.getIs_fit());
				stmt.setTimestamp(15, new Timestamp(new Date().getTime()));
				stmt.addBatch();
				recordNum++;
				if (recordNum % 200 == 0) {
					stmt.executeBatch();
				}
			}
			stmt.executeBatch();
			conn.commit();
		} catch (Exception e) {
			System.out.println("VehicleCheatCountSZtask数据保存 异常");
			e.printStackTrace();
			if (conn != null) {
				try {
					conn.rollback();
				} catch (Exception ee) {
					ee.printStackTrace();
				}
			}
			recordNum = 0;
		} finally {
			DbServer.getSingleInstance().releaseConn(conn2);
			DbServer.getSingleInstance().releaseConn(conn);
		}
		System.out.println("Finish VehicleCheatCountSZtask Analysis:"
				+ this.toString() + " recordNum=" + recordNum);
	}

	private ArrayList<Double> analysisTrack(Date start, Date end, String car_no)
			throws Exception {
		ArrayList<Double> list = new ArrayList();
		double gps_points = 0;
		InfoContainer queryInfo = new InfoContainer();
		queryInfo.setInfo(TrackServer.START_DATE_FLAG, start);
		queryInfo.setInfo(TrackServer.END_DATE_FLAG, end);
		queryInfo.setInfo(TrackServer.DEST_NO_FLAG, car_no);
		InfoContainer[] trackArr = TrackServer.getSingleInstance()
				.getTrackInfo(queryInfo);
		double distance = 0;
		double old_lo = 0;
		double old_la = 0;
		double new_lo = 0;
		double new_la = 0;
		double max_la = 0;// 纬度
		double max_lo = 0;// 经度
		double min_la = 0;// 纬度
		double min_lo = 0;// 经度
		double load_points = 0;// 重车点数
		double empty_points = 0;// 空车点数
		double task_points = 0;// 任务车点数
		double other_points = 0;// 未知状态点数
		double un_fit = 0;// 不合格点数（两点之间距离大于10公里点数）
		
		int state = -1;
		for (int i = 0; i < trackArr.length; i++) {
			gps_points++;
			new_lo = trackArr[i].getDouble(TrackIO.DEST_LO_FLAG);
			new_la = trackArr[i].getDouble(TrackIO.DEST_LA_FLAG);
			state = trackArr[i].getInteger(TrackIO.DEST_STATUS_FLAG).intValue() & 0x0F;
			if (state == 0) {// 空车
				empty_points++;
			} else if (state == 1) {// 重车
				load_points++;
			} else if (state == 2) {// 任务车
				task_points++;
			} else {// 未知状态点
				other_points++;
			}
			if (old_lo > 0 || old_la > 0) {
				double dis = countDistance(new_lo, new_la, old_lo, old_la);
				if(dis>=10){
					un_fit++;//两点之间距离大于10公里，则算为不合格点。
				}else{
					distance += dis;
				}
			}
			if (new_lo > max_lo) {
				max_lo = new_lo;
			}
			if (new_lo < min_lo || min_lo == 0) {
				min_lo = new_lo;
			}
			if (new_la > max_la) {
				max_la = new_la;
			}
			if (new_la < min_la || min_la == 0) {
				min_la = new_la;
			}
			old_lo = new_lo;
			old_la = new_la;
		}

		DecimalFormat df = new DecimalFormat("#.##");
		DecimalFormat df2 = new DecimalFormat("#");

		double all_distance = countDistance(max_lo, max_la, min_lo, min_la);

		String area = df.format((all_distance * all_distance) / 2);
		double area_size = Double.parseDouble(area);
		list.add(distance);
		list.add(gps_points);
		list.add(area_size);// 营运面积
		list.add(empty_points);// 空
		list.add(load_points);// 重
		list.add(task_points);// 任务
		list.add(other_points);// 其它
		list.add(un_fit);// 不合格点数
		return list;
	}

	private static double rad(double d) {
		return d * Math.PI / 180.0;
	}

	private static double EARTH_RADIUS = 6378.137;

	public static double countDistance(double lo1, double la1, double lo2,
			double la2) {
		double radLat1 = rad(la1);
		double radLat2 = rad(la2);
		double a = radLat1 - radLat2;
		double b = rad(lo1) - rad(lo2);
		double s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2), 2)
				+ Math.cos(radLat1) * Math.cos(radLat2)
				* Math.pow(Math.sin(b / 2), 2)));
		s = s * EARTH_RADIUS;
		s = Math.round(s * 100000) / 100000.0;
		return s;
	}

	public static double round(double v, int scale) {
		if (scale < 0) {
			throw new IllegalArgumentException(
					"The scale must be a positive integer or zero");
		}
		BigDecimal b = new BigDecimal(Double.toString(v));
		BigDecimal one = new BigDecimal("1");
		return b.divide(one, scale, BigDecimal.ROUND_HALF_UP).doubleValue();
	}

}
