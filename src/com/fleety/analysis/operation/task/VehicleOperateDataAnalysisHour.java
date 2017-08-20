package com.fleety.analysis.operation.task;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import server.db.DbServer;
import server.var.VarManageServer;
import com.fleety.analysis.AnalysisServer;
import com.fleety.analysis.operation.IOperationAnalysis;
import com.fleety.base.GeneralConst;
import com.fleety.base.InfoContainer;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;

public class VehicleOperateDataAnalysisHour implements IOperationAnalysis {

	private List resultList = null;
	private float fuelSurcharges = 0;
	private int initiateRateKs = 0;
	private int maxKs = 0;

	@Override
	public boolean startAnalysisOperation(AnalysisServer parentServer,
			InfoContainer statInfo) {
		fuelSurcharges = Integer.parseInt(VarManageServer.getSingleInstance()
				.getVarStringValue("fuel_surcharges"));
		initiateRateKs = Integer.parseInt(VarManageServer.getSingleInstance()
				.getVarStringValue("initiate_rate_ks"));
		maxKs = Integer.parseInt(VarManageServer.getSingleInstance()
				.getVarStringValue("max_ks"));

		resultList = null;
		// TODO Auto-generated method stub
		// ��ѯ���ݿ����Ƿ���ڵ�������
		Date sDate = statInfo.getDate(IOperationAnalysis.STAT_START_TIME_DATE);
		DbHandle conn = null;
		try {
			conn = DbServer.getSingleInstance().getConn();
			StatementHandle stmt = conn.createStatement();
			StringBuilder sb = new StringBuilder();
			sb.append("select count(*) as sum from ANA_SINGLE_CAR_TPYE_STAT ")
					.append(" where stat_time >= to_date('")
					.append(GeneralConst.YYYY_MM_DD.format(sDate))
					.append("','yyyy-mm-dd') ");
			sb.append(" and stat_time < to_date('")
					.append(GeneralConst.YYYY_MM_DD.format(sDate))
					.append(" 23:59:59','yyyy-mm-dd hh24:mi:ss')");
			ResultSet sets = stmt.executeQuery(sb.toString());
			if (sets.next()) {
				int sum = sets.getInt("sum");
				if (sum == 0) {
					this.resultList = new ArrayList();
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			resultList = null;
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}
		return resultList != null;
	}

	@Override
	public void analysisDestOperation(AnalysisServer parentServer,
			InfoContainer statInfo) {
		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
		Date eTime = statInfo.getDate(STAT_END_TIME_DATE);

		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			Calendar cal = Calendar.getInstance();
			cal.add(Calendar.DAY_OF_MONTH, -1);
			cal.set(Calendar.HOUR_OF_DAY, 0);
			cal.set(Calendar.MINUTE, 0);
			cal.set(Calendar.SECOND, 0);
			cal.set(Calendar.MILLISECOND, 0);

			Date start = null;
			Date end = null;
			for (int i = 0; i < 24; i++) {
				cal.set(Calendar.HOUR_OF_DAY, i);
				start = cal.getTime();	
				cal.add(Calendar.HOUR_OF_DAY, 1);				
				end = cal.getTime();

				StringBuilder sql = new StringBuilder();
				sql.append(" select type_id,count(dispatch_car_no) as car_total_num,");
				sql.append(" sum(work_times) as work_times,");
				sql.append(" sum(total_distance) as total_distance,");
				sql.append(" sum(work_distance) as work_distance,");
				sql.append(" sum(free_distance) as free_distance,");
				sql.append(" sum(waiting_hour) as waiting_hour,");
				sql.append(" sum(waiting_minute) as waiting_minute,");
				sql.append(" sum(waiting_second) as waiting_second,");
				sql.append(" sum(work_income) as work_income,");
				sql.append(" sum(work_times_first) as work_times_first,");
				sql.append(" sum(work_times_second) as work_times_second,");
				sql.append(" sum(work_times_third) as work_times_third,");
				sql.append(" sum(work_distance_first) as work_distance_first,");
				sql.append(" sum(work_distance_second) as work_distance_second,");
				sql.append(" sum(work_distance_third) as work_distance_third,");
				sql.append(" sum(work_income_first) as work_income_first,");
				sql.append(" sum(work_income_second) as work_income_second,");
				sql.append(" sum(work_income_third) as work_income_third from (");

				sql.append("select a.*,b.type_id from (")
						.append(" select dispatch_car_no,")
						.append("  count(*) as work_times,")
						.append("  sum(distance+free_distance) as total_distance,")
						.append("  sum(decode(sign(distance),1,distance,-1,0,distance)) as work_distance,")
						.append("  sum(free_distance) as free_distance,")
						.append("  sum(waiting_hour) as waiting_hour,")
						.append("  sum(waiting_minute) as waiting_minute,")
						.append("  sum(waiting_second) as waiting_second,")
						.append("  sum(sum) as work_income,")
						.append("  sum(case when distance <= ")
						.append(initiateRateKs)
						.append(" then 1 else 0 end) as work_times_first,")
						.append("  sum(case when distance > ")
						.append(initiateRateKs)
						.append(" and distance <= ")
						.append(maxKs)
						.append(" then 1 else 0 end) as work_times_second,")
						.append("  sum(case when distance > ")
						.append(maxKs)
						.append(" then 1 else 0 end) as work_times_third,")
						.append("  sum(case when distance <= ")
						.append(initiateRateKs)
						.append(" then distance else 0 end) as work_distance_first,")
						.append("  sum(case when distance > ")
						.append(initiateRateKs)
						.append(" and distance <= ")
						.append(maxKs)
						.append(" then distance else 0 end) as work_distance_second,")
						.append("  sum(case when distance > ")
						.append(maxKs)
						.append(" then distance else 0 end) as work_distance_third,")
						.append("  sum(case when distance <= ")
						.append(initiateRateKs)
						.append(" then sum else 0 end) as work_income_first,")
						.append("  sum(case when distance > ")
						.append(initiateRateKs)
						.append(" and distance <= ")
						.append(maxKs)
						.append(" then sum else 0 end) as work_income_second,")
						.append("  sum(case when distance > ")
						.append(maxKs)
						.append(" then sum else 0 end) as work_income_third")
						.append(" from SINGLE_BUSINESS_DATA_BS ")
						.append(" where dispatch_car_no is not null ")
						.append("       and date_up >= to_date('")
						.append(GeneralConst.YYYY_MM_DD_HH_MM_SS.format(start))
						.append("','yyyy-mm-dd hh24:mi:ss')")
						.append("       and date_up < to_date('")
						.append(GeneralConst.YYYY_MM_DD_HH_MM_SS.format(end))
						.append("','yyyy-mm-dd hh24:mi:ss')")
						.append(" group by dispatch_car_no")
						.append(") a left join car b on a.dispatch_car_no=b.car_id");
				sql.append(") group by type_id");
				StatementHandle stmt = conn.createStatement();
				ResultSet rs = stmt.executeQuery(sql.toString());
				VehicleOperateInfo info = null;
				while (rs.next()) {
					info = new VehicleOperateInfo();
					info.statTime = start;// ͳ��ʱ��
					info.typeId = rs.getInt("type_id");// ����
					info.carTotalNum = rs.getInt("car_total_num");// �ܳ���
					info.workTimes = rs.getInt("work_times");// Ӫ���ܴ���
					info.totalDistance = rs.getFloat("total_distance");// ��ʻ���
					info.workDistance = rs.getFloat("work_distance");// Ӫ�����
					info.freeDistance = rs.getFloat("free_distance");// ��ʻ���
					info.waitingHour = rs.getInt("waiting_hour");// ���ٵȺ�ʱ��(ʱ)
					info.waitingMinute = rs.getInt("waiting_minute");// ���ٵȺ�ʱ��(��)
					info.waitingSecond = rs.getInt("waiting_second");// ���ٵȺ�ʱ��(��)
					info.workIncome = rs.getFloat("work_income");// �Ƽ�����
					info.fuelIncome = fuelSurcharges * info.workTimes;// ȼ�͸�������
					info.totalIncome = info.workIncome + info.fuelIncome;// ������
					info.workTimesFirst = rs.getInt("work_times_first");// С���𲽼۹�������Ӫ�˴���
					info.workTimesSecond = rs.getInt("work_times_second");// �����𲽼۹�����С�����������Ӫ�˴���
					info.workTimesThird = rs.getInt("work_times_third");// �������������Ӫ�˴���
					info.workDistanceFirst = rs.getFloat("work_distance_first");// С���𲽼۹�������Ӫ�����
					info.workDistanceSecond = rs
							.getFloat("work_distance_second");// �����𲽼۹�����С�����������Ӫ�����
					info.workDistanceThird = rs.getFloat("work_distance_third");// �������������Ӫ�����
					info.workIncomeFirst = rs.getFloat("work_income_first");// С���𲽼۹�������Ӫ������
					info.workIncomeSecond = rs.getFloat("work_income_second");// �����𲽼۹�����С�����������Ӫ������
					info.workIncomeThird = rs.getFloat("work_income_third");// �������������Ӫ������

					resultList.add(info);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}
	}

	@Override
	public void endAnalysisOperation(AnalysisServer parentServer,
			InfoContainer statInfo) {
		DbHandle conn = DbServer.getSingleInstance().getConn();
		int recordNum = 0;
		try {
			conn.setAutoCommit(false);
			StatementHandle stmt = conn
					.prepareStatement("insert into ana_single_car_tpye_stat"
							+ " (id, type_id, car_total_num, work_times, total_distance,"
							+ "  work_distance, free_distance, waiting_hour, waiting_minute, "
							+ " waiting_second, work_income, fuel_income, total_income,"
							+ " work_times_first, work_times_second, work_times_third,"
							+ " work_distance_first, work_distance_second, work_distance_third,"
							+ " work_income_first, work_income_second, work_income_third,"
							+ " stat_time, record_time) " + " values "
							+ " (?, ?, ?, ?, ?," + "  ?, ?, ?, ?, ?, "
							+ "  ?, ?, ?, ?, ?, " + "  ?, ?, ?, ?, ?, "
							+ "  ?, ?, ?, sysdate)");
			Iterator itr = this.resultList.iterator();
			VehicleOperateInfo vehicleOperateInfo = null;

			while (itr.hasNext()) {

				vehicleOperateInfo = (VehicleOperateInfo) itr.next();
				stmt.setInt(1, (int) DbServer.getSingleInstance()
						.getAvaliableId(conn, "ana_single_car_tpye_stat", "id"));
				stmt.setInt(2, vehicleOperateInfo.typeId);
				stmt.setInt(3, vehicleOperateInfo.carTotalNum);
				stmt.setInt(4, vehicleOperateInfo.workTimes);
				stmt.setFloat(5, vehicleOperateInfo.totalDistance);
				stmt.setFloat(6, vehicleOperateInfo.workDistance);
				stmt.setFloat(7, vehicleOperateInfo.freeDistance);
				stmt.setInt(8, vehicleOperateInfo.waitingHour);
				stmt.setInt(9, vehicleOperateInfo.waitingMinute);
				stmt.setInt(10, vehicleOperateInfo.waitingSecond);
				stmt.setFloat(11, vehicleOperateInfo.workIncome);
				stmt.setFloat(12, vehicleOperateInfo.fuelIncome);
				stmt.setFloat(13, vehicleOperateInfo.totalIncome);

				stmt.setInt(14, vehicleOperateInfo.workTimesFirst);
				stmt.setInt(15, vehicleOperateInfo.workTimesSecond);
				stmt.setInt(16, vehicleOperateInfo.workTimesThird);
				stmt.setFloat(17, vehicleOperateInfo.workDistanceFirst);
				stmt.setFloat(18, vehicleOperateInfo.workDistanceSecond);
				stmt.setFloat(19, vehicleOperateInfo.workDistanceThird);
				stmt.setFloat(20, vehicleOperateInfo.workIncomeFirst);
				stmt.setFloat(21, vehicleOperateInfo.workIncomeSecond);
				stmt.setFloat(22, vehicleOperateInfo.workIncomeThird);
				stmt.setTimestamp(23,
						new Timestamp(vehicleOperateInfo.statTime.getTime()));
				stmt.addBatch();
				recordNum++;
			}
			stmt.executeBatch();
			conn.commit();
		} catch (Exception e) {
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
			DbServer.getSingleInstance().releaseConn(conn);
		}
		System.out.println("Finish vehicle operate data Analysis:"
				+ this.toString() + " recordNum=" + recordNum);

	}

	private class VehicleOperateInfo {
		public int typeId;// ����
		public int carTotalNum;// ��������
		public int workTimes;// Ӫ���ܴ���
		public float totalDistance;// ��ʻ���
		public float workDistance;// Ӫ�����
		public float freeDistance;// ��ʻ���
		public int waitingHour;// ���ٵȺ�ʱ��(ʱ)
		public int waitingMinute;// ���ٵȺ�ʱ��(��)
		public int waitingSecond;// ���ٵȺ�ʱ��(��)
		public float workIncome;// �Ƽ�����
		public float fuelIncome;// ȼ�͸�������
		public float totalIncome;// ������
		public int workTimesFirst;// С���𲽼۹�������Ӫ�˴���
		public int workTimesSecond;// �����𲽼۹�����С�����������Ӫ�˴���
		public int workTimesThird;// �������������Ӫ�˴���
		public float workDistanceFirst;// С���𲽼۹�������Ӫ�����
		public float workDistanceSecond;// �����𲽼۹�����С�����������Ӫ�����
		public float workDistanceThird;// �������������Ӫ�����
		public float workIncomeFirst;// С���𲽼۹�������Ӫ������
		public float workIncomeSecond;// �����𲽼۹�����С�����������Ӫ������
		public float workIncomeThird;// �������������Ӫ������
		public Date statTime;// ��������,��Сʱ
		public Date record_time;// ͳ������
	}

	public static void main(String[] args) {
		int initiateRateKs = 15;
		int maxKs = 25;
		StringBuilder sql = new StringBuilder();
		sql.append(" select type_id,count(dispatch_car_no) as total_car_num,");
		sql.append(" sum(work_times) as work_times,");
		sql.append(" sum(total_distance) as total_distance,");
		sql.append(" sum(work_distance) as work_distance,");
		sql.append(" sum(free_distance) as free_distance,");
		sql.append(" sum(waiting_hour) as waiting_hour,");
		sql.append(" sum(waiting_minute) as waiting_minute,");
		sql.append(" sum(waiting_second) as waiting_second,");
		sql.append(" sum(work_income) as work_income,");
		sql.append(" sum(work_times_first) as work_times_first,");
		sql.append(" sum(work_times_second) as work_times_second,");
		sql.append(" sum(work_times_third) as work_times_third,");
		sql.append(" sum(work_distance_first) as work_distance_first,");
		sql.append(" sum(work_distance_second) as work_distance_second,");
		sql.append(" sum(work_distance_third) as work_distance_third,");
		sql.append(" sum(work_income_first) as work_income_first,");
		sql.append(" sum(work_income_second) as work_income_second,");
		sql.append(" sum(work_income_third) as work_income_third from (");

		sql.append("select a.*,b.type_id from (")
				.append(" select dispatch_car_no,")
				.append("  count(*) as work_times,")
				.append("  sum(distance+free_distance) as total_distance,")
				.append("  sum(decode(sign(distance),1,distance,-1,0,distance)) as work_distance,")
				.append("  sum(free_distance) as free_distance,")
				.append("  sum(waiting_hour) as waiting_hour,")
				.append("  sum(waiting_minute) as waiting_minute,")
				.append("  sum(waiting_second) as waiting_second,")
				.append("  sum(sum) as work_income,")
				.append("  sum(case when distance <= ").append(initiateRateKs)
				.append(" then 1 else 0 end) as work_times_first,")
				.append("  sum(case when distance > ").append(initiateRateKs)
				.append(" and distance <= ").append(maxKs)
				.append(" then 1 else 0 end) as work_times_second,")
				.append("  sum(case when distance > ").append(maxKs)
				.append(" then 1 else 0 end) as work_times_third,")
				.append("  sum(case when distance <= ").append(initiateRateKs)
				.append(" then distance else 0 end) as work_distance_first,")
				.append("  sum(case when distance > ").append(initiateRateKs)
				.append(" and distance <= ").append(maxKs)
				.append(" then distance else 0 end) as work_distance_second,")
				.append("  sum(case when distance > ").append(maxKs)
				.append(" then distance else 0 end) as work_distance_third,")
				.append("  sum(case when distance <= ").append(initiateRateKs)
				.append(" then sum else 0 end) as work_income_first,")
				.append("  sum(case when distance > ").append(initiateRateKs)
				.append(" and distance <= ").append(maxKs)
				.append(" then sum else 0 end) as work_income_second,")
				.append("  sum(case when distance > ").append(maxKs)
				.append(" then sum else 0 end) as work_income_third")
				.append(" from SINGLE_BUSINESS_DATA_BS ")
				.append(" where dispatch_car_no is not null ")
				.append("       and date_up >= to_date('")
				.append(GeneralConst.YYYY_MM_DD_HH_MM_SS.format(new Date()))
				.append("','yyyy-mm-dd hh24:mi:ss')")
				.append("       and date_up <= to_date('")
				.append(GeneralConst.YYYY_MM_DD_HH_MM_SS.format(new Date()))
				.append("','yyyy-mm-dd hh24:mi:ss')")
				.append(" group by dispatch_car_no")
				.append(") a left join car b on a.dispatch_car_no=b.car_id");

		sql.append(") group by type_id");
		System.out.println(sql.toString());
	}
}
