package com.fleety.analysis.track;

import java.sql.ResultSet;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import server.db.DbServer;
import server.track.TrackServer;

import com.fleety.analysis.AnalysisServer;
import com.fleety.base.GeneralConst;
import com.fleety.base.InfoContainer;
import com.fleety.base.StrFilter;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.fleety.util.pool.thread.BasicTask;

public class TrackAnalysisServer extends AnalysisServer {
	private ArrayList taskList = new ArrayList(4);
	public int preTime = 5;
	private static TrackAnalysisServer instance = null;

	private TrackAnalysisServer() {
	}

	public static TrackAnalysisServer getSingleInstance() {
		if (instance == null) {
			instance = new TrackAnalysisServer();
		}
		return instance;
	}

	@Override
	public boolean startServer() {
		this.isRunning = super.startServer();

		Object obj = this.getPara("task_class");
		if (obj == null) {
			this.isRunning = true;
			return this.isRunning();
		}

		try {
			if (obj instanceof List) {
				for (Iterator itr = ((List) obj).iterator(); itr.hasNext();) {
					taskList.add((ITrackAnalysis) Class.forName(
							itr.next().toString()).newInstance());
				}
			} else {
				taskList.add((ITrackAnalysis) Class.forName(obj.toString())
						.newInstance());
			}
		} catch (Exception e) {
			e.printStackTrace();
			this.isRunning = false;
			return false;
		}
		ArrayList destList = new ArrayList(1024);
		DbHandle conn = DbServer.getSingleInstance().getConn();
		DestInfo dInfo;
		try {
			StatementHandle stmt = conn
					.prepareStatement("select mdt_id,dest_no,company_id,company_name,type_id from v_ana_dest_info");
			ResultSet sets = stmt.executeQuery();
			while (sets.next()) {
				dInfo = new DestInfo();
				dInfo.mdtId = sets.getInt("mdt_id");
				dInfo.destNo = sets.getString("dest_no");
				dInfo.companyId = sets.getInt("company_id");
				dInfo.companyName = sets.getString("company_name");
				dInfo.carType = sets.getInt("type_id");
				destList.add(dInfo);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}
		Date startDate = null, endDate = null;
		try {
			startDate = GeneralConst.YYYY_MM_DD.parse("2014-01-01");
			endDate = GeneralConst.YYYY_MM_DD.parse(GeneralConst.YYYY_MM_DD
					.format(new Date()));
			String temp = this.getStringPara("startDate");
			if (StrFilter.hasValue(temp)) {
				startDate = GeneralConst.YYYY_MM_DD.parse(temp);
			}
			temp = this.getStringPara("endDate");
			if (StrFilter.hasValue(temp)) {
				endDate = GeneralConst.YYYY_MM_DD.parse(temp);
			}

			while (endDate.getTime() > startDate.getTime()) {
				Calendar cal = Calendar.getInstance();
				cal.setTime(startDate);
				cal.set(Calendar.HOUR_OF_DAY, 0);
				cal.set(Calendar.MINUTE, 0);
				cal.set(Calendar.SECOND, 0);
				cal.set(Calendar.MILLISECOND, 0);
				TrackAnalysisServer.this
						.addExecTask(new ExecTask(cal, destList));
				// ·ÖÎö¹ì¼£
				startDate = new Date(startDate.getTime()
						+ GeneralConst.ONE_DAY_TIME);
			}
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			this.isRunning = false;
			return this.isRunning();
		}

		this.isRunning = true;
		return this.isRunning();
	}

	public void stopServer() {
		super.stopServer();
	}

	private void executeTask(Calendar anaDate, List destList) throws Exception {

		Date sDate = anaDate.getTime();
		anaDate.setTimeInMillis(anaDate.getTimeInMillis()
				+ GeneralConst.ONE_DAY_TIME - 1000);
		Date eDate = anaDate.getTime();
		InfoContainer statInfo = new InfoContainer();
		statInfo.setInfo(ITrackAnalysis.STAT_START_TIME_DATE, sDate);
		statInfo.setInfo(ITrackAnalysis.STAT_END_TIME_DATE, eDate);
		statInfo.setInfo(ITrackAnalysis.STAT_DEST_NUM_INTEGER, new Integer(
				destList.size()));

		System.out
				.println("Start Exec:" + GeneralConst.YYYY_MM_DD.format(sDate)
						+ " " + GeneralConst.YYYY_MM_DD.format(eDate) + " "
						+ destList.size());

		ArrayList execList = new ArrayList(this.taskList.size());
		ITrackAnalysis analysis;
		for (int i = 0; i < this.taskList.size(); i++) {
			analysis = (ITrackAnalysis) this.taskList.get(i);
			if (analysis.startAnalysisTrack(this, statInfo)) {
				execList.add(analysis);
			}
		}
		System.out.println("Exec Task Num:" + execList.size());

		String des = "ÕãBT8056";
		// String des = "ÕãBT2743";

		if (execList.size() > 0) {
			InfoContainer queryInfo = new InfoContainer();
			queryInfo.setInfo(TrackServer.START_DATE_FLAG, sDate);
			queryInfo.setInfo(TrackServer.END_DATE_FLAG, eDate);
			TrackInfo trackInfo;
			DestInfo dInfo;
			int totalCarNum = destList.size();
			for (Iterator itr = destList.iterator(); itr.hasNext();) {
				dInfo = (DestInfo) itr.next();
				dInfo.totalCarNum = totalCarNum;
				queryInfo.setInfo(TrackServer.DEST_NO_FLAG, dInfo.destNo);

				trackInfo = new TrackInfo();
				trackInfo.dInfo = dInfo;
				trackInfo.sDate = sDate;
				trackInfo.eDate = eDate;
				trackInfo.trackArr = TrackServer.getSingleInstance()
						.getTrackInfo(queryInfo);

				for (int i = 0; i < execList.size(); i++) {
					analysis = (ITrackAnalysis) execList.get(i);
					try {
						// if(dInfo.destNo.equals(des))
						analysis.analysisDestTrack(this, trackInfo);
					} catch (Exception e) {
						e.printStackTrace();
						System.out.println("Analysis Failure:"
								+ analysis.toString());
					}
				}
			}

			for (int i = 0; i < execList.size(); i++) {
				analysis = (ITrackAnalysis) execList.get(i);
				analysis.endAnalysisTrack(this, statInfo);
			}
		}

		System.out.println("Exec Finished");
	}

	private class ExecTask extends BasicTask {
		private Calendar anaDate = null;
		private List destList = null;

		public ExecTask(Calendar anaDate, ArrayList destList) {
			this.anaDate = anaDate;
			this.destList = destList;
		}

		public boolean execute() throws Exception {
			TrackAnalysisServer.this.executeTask(this.anaDate, this.destList);
			return true;
		}

		public String getDesc() {
			return "¹ì¼£Êý¾Ý·ÖÎö";
		}

		public Object getFlag() {
			return "TrackAnalysisServer";
		}
	}
}
