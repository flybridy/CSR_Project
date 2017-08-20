package com.fleety.server.emp_enti_change;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;

import server.db.DbServer;
import server.track.TrackServer;

import com.fleety.analysis.track.DestInfo;
import com.fleety.analysis.track.ITrackAnalysis;
import com.fleety.analysis.track.TrackInfo;
import com.fleety.base.GeneralConst;
import com.fleety.base.InfoContainer;
import com.fleety.track.TrackIO;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.fleety.util.pool.thread.BasicTask;

public class Emp_Enti_Change extends BasicTask {

	@Override
	public boolean execute() throws Exception {
		
		DbHandle conn = DbServer.getSingleInstance().getConn();
		ArrayList<DestInfo> destList = new ArrayList<DestInfo>(1024);//车辆列表
		DestInfo dInfo;
		
		Calendar anaDate = Calendar.getInstance();
		anaDate.add(Calendar.DAY_OF_YEAR, -1);
		anaDate.set(Calendar.HOUR_OF_DAY, 0);
		anaDate.set(Calendar.MINUTE, 0);
		anaDate.set(Calendar.SECOND, 0);
		anaDate.set(Calendar.MILLISECOND, 0);
		Date sDate = anaDate.getTime(); // 轨迹分析开始时间
		
		anaDate.set(Calendar.HOUR_OF_DAY, 23);
		anaDate.set(Calendar.MINUTE, 59);
		anaDate.set(Calendar.SECOND, 59);
		Date eDate = anaDate.getTime();// 轨迹分析结束时间
		
		String anadate = GeneralConst.YYYY_MM_DD.format(eDate);
		java.sql.Date date=java.sql.Date.valueOf(anadate);//转换日期类型
		try {
			String sql1 = "insert into emp_enti_change (car_no, ana_date, change_time,service_num ) values (?,to_char(?,'yyyy-mm-dd'),?,?)";
			StatementHandle stmt = conn
					.prepareStatement(sql1); //空重车表
			
			try {
				String sql2 = "select car_no,count(*) sum from single_business_data_bs where  to_char(date_down,'yyyy-mm-dd')=? group by car_no ";
				StatementHandle stmt2 = conn.prepareStatement(sql2);//车牌 统计日期
				stmt2.setString(1, anadate);
				ResultSet sets = stmt2.executeQuery();
				
				while(sets.next()){
					dInfo = new DestInfo();
					dInfo.destNo = sets.getString("car_no");//从single_business_data_bs表中获得车牌号
					dInfo.sum = sets.getInt("sum");
					destList.add(dInfo);
				}

			} catch (Exception e) {
				e.printStackTrace();
			}
			
			InfoContainer statInfo = new InfoContainer();
			statInfo.setInfo(ITrackAnalysis.STAT_START_TIME_DATE, sDate);
			statInfo.setInfo(ITrackAnalysis.STAT_END_TIME_DATE, eDate);
			statInfo.setInfo(ITrackAnalysis.STAT_DEST_NUM_INTEGER, new Integer(
					destList.size()));
			
			System.out.println("开始时间，结束时间，车辆数:"
					+ GeneralConst.YYYY_MM_DD.format(sDate) + " "
					+ GeneralConst.YYYY_MM_DD.format(eDate) + " "
					+ destList.size());
			
			InfoContainer queryInfo = new InfoContainer();
			queryInfo.setInfo(TrackServer.START_DATE_FLAG, sDate);
			queryInfo.setInfo(TrackServer.END_DATE_FLAG, eDate);
			TrackInfo trackInfo;//轨迹点信息
			
			int preStatus=0,status = 0,busytofree=0,freetobusy=0;//初始化前一个轨迹点,当前轨迹点和空中车次数
			
 			for(Iterator<DestInfo> itr = destList.iterator(); itr.hasNext();) { //遍历每辆车
				dInfo = (DestInfo) itr.next();
				queryInfo.setInfo(TrackServer.DEST_NO_FLAG, dInfo.destNo);//获得车辆的轨迹点信息
				
				trackInfo = new TrackInfo();
				trackInfo.dInfo = dInfo;
				trackInfo.sDate = sDate;
				trackInfo.eDate = eDate;
				trackInfo.trackArr = TrackServer.getSingleInstance().getTrackInfo(queryInfo);
				
				System.out.println(TrackServer.getSingleInstance()
						.getTrackFile(dInfo.destNo, sDate).getPath());  //打印分析轨迹点的路径
				
				if(trackInfo.trackArr != null && trackInfo.trackArr.length > 0){
					
					for(int i = 0; i < trackInfo.trackArr.length; i++){ //分析每一个轨迹点
						status = (trackInfo.trackArr[i].getInteger(
								TrackIO.DEST_STATUS_FLAG).intValue() & 0x0f);//当前空中车状态
						if (i == 0) {
							preStatus = status;
							continue;
						}
						
						if(preStatus!=0&&status==0){
							busytofree++;  //重车变空车
						}else if(preStatus!=1&&status==1){
							freetobusy++;  //空车变重车
						}			
						preStatus = status;
					}
					
					System.out.println("destno:" + dInfo.destNo + "sdate" + anadate + "busytofree" + busytofree+"freetobusy" + freetobusy);
					stmt.setString(1, dInfo.destNo);//车牌号
					stmt.setDate(2, date);//分析时间
					stmt.setInt(3, busytofree);//空重车变化次数
					stmt.setInt(4, dInfo.sum);
					stmt.execute();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DbServer.getSingleInstance().releaseConn(conn);
		}
		return true;
	}

}
