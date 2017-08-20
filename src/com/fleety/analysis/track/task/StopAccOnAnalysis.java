package com.fleety.analysis.track.task;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import server.db.DbServer;

import com.fleety.analysis.AnalysisServer;
import com.fleety.analysis.track.DestInfo;
import com.fleety.analysis.track.ITrackAnalysis;
import com.fleety.analysis.track.TrackInfo;
import com.fleety.base.InfoContainer;
import com.fleety.track.TrackIO;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;

public class StopAccOnAnalysis implements ITrackAnalysis {
	private HashMap mapping = null;
	private List<StopAccOn> resList = new ArrayList<StopAccOn>();

	@Override
	public boolean startAnalysisTrack(AnalysisServer parentServer,
			InfoContainer statInfo) {
		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
		Date eTime = statInfo.getDate(STAT_END_TIME_DATE);
		mapping = null;

		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			StringBuffer sb = new StringBuffer(
					"select * from stop_acc_on_info where start_time >= ? and end_time <= ?");
			StatementHandle stmt = conn.prepareStatement(sb.toString());
			stmt.setTimestamp(1, new Timestamp(sTime.getTime() + 1000));
			stmt.setTimestamp(2, new Timestamp(eTime.getTime()));
			ResultSet rs = stmt.executeQuery();
			if (!rs.next()) {
				this.mapping = new HashMap();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}
		if (this.mapping == null) {
			System.out.println("Not Need Analysis:" + this.toString());
		} else {
			System.out.println("Start Analysis:" + this.toString());
		}
		return this.mapping != null;
	}

	@Override
	public void analysisDestTrack(AnalysisServer parentServer,
			TrackInfo trackInfo) {
		if (this.mapping == null) {
			return;
		}
		if (trackInfo.trackArr == null) {
			return;
		}
		DestInfo destInfo = trackInfo.dInfo;
		String car_num = destInfo.destNo;
		int mdt_id = destInfo.mdtId;
		int term_id = destInfo.companyId;
		String term_name = destInfo.companyName;

		Date sDate = null;
		Date eDate = null;
		long time1 = -1;
		long time2 = -1;
		int count=0;
		for (int i = 0; i < trackInfo.trackArr.length; i++) {
			if (trackInfo.trackArr[i].getInteger(TrackIO.DEST_SPEED_FLAG) <= 5) {// 速度小于5
				if (time1 == -1) {
					sDate=trackInfo.trackArr[i]
							.getDate(TrackIO.DEST_TIME_FLAG);
					time1=sDate.getTime();
				}

			} else {// 速度大于5
				if (time1 != -1) {
					eDate = trackInfo.trackArr[i - 1]
							.getDate(TrackIO.DEST_TIME_FLAG);// 得到上一个速度小于5的时间作为结束时间
					time2=eDate.getTime();

					long duringTime = (time2 - time1) / 60000;
					if (duringTime > 20) {
						StopAccOn sco = new StopAccOn();
						sco.car_no = car_num;
						sco.term_id = term_id;
						sco.term_name = term_name;
						sco.start_time = time1;
						sco.end_time = time2;
						sco.mdt_id = mdt_id;
						sco.duringmintus = duringTime;
						resList.add(sco);
						count++;
					}
					time1 = -1;
					time2 = -1;
				}
			}
		}
	}

	@Override
	public void endAnalysisTrack(AnalysisServer parentServer,
			InfoContainer statInfo) {

		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			StatementHandle stmt = conn
					.prepareStatement("insert into stop_acc_on_info(id,car_no,mdt_id,start_time,end_time,term_id,duringminutes,term_name)values(?,?,?,?,?,?,?,?)");
			for (int i = 0; i < resList.size(); i++) {
				StopAccOn saAccOn = resList.get(i);
				stmt.setInt(1, (int) DbServer.getSingleInstance()
						.getAvaliableId(conn, "stop_acc_on_info", "id"));
				stmt.setString(2, saAccOn.car_no);
				stmt.setInt(3, saAccOn.mdt_id);
				stmt.setTimestamp(4, new Timestamp(saAccOn.start_time));
				stmt.setTimestamp(5,new Timestamp(saAccOn.end_time));
				stmt.setInt(6, saAccOn.term_id);
				stmt.setLong(7, saAccOn.duringmintus);
				stmt.setString(8, saAccOn.term_name);
				stmt.addBatch();
			}
			stmt.executeBatch();
		} catch (Exception e) {
			e.printStackTrace();
		}

		DbServer.getSingleInstance().releaseConn(conn);

	}

	public String toString() {
		return "StopAccOnAnalysis";
	}

	private  class StopAccOn {
		private int term_id;
		private String car_no;
		private String term_name;
		private long start_time;
		private long end_time;
		private int mdt_id;
		private long duringmintus;
	}
}
