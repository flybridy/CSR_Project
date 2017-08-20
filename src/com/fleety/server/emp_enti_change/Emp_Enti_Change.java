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
		ArrayList<DestInfo> destList = new ArrayList<DestInfo>(1024);//�����б�
		DestInfo dInfo;
		
		Calendar anaDate = Calendar.getInstance();
		anaDate.add(Calendar.DAY_OF_YEAR, -1);
		anaDate.set(Calendar.HOUR_OF_DAY, 0);
		anaDate.set(Calendar.MINUTE, 0);
		anaDate.set(Calendar.SECOND, 0);
		anaDate.set(Calendar.MILLISECOND, 0);
		Date sDate = anaDate.getTime(); // �켣������ʼʱ��
		
		anaDate.set(Calendar.HOUR_OF_DAY, 23);
		anaDate.set(Calendar.MINUTE, 59);
		anaDate.set(Calendar.SECOND, 59);
		Date eDate = anaDate.getTime();// �켣��������ʱ��
		
		String anadate = GeneralConst.YYYY_MM_DD.format(eDate);
		java.sql.Date date=java.sql.Date.valueOf(anadate);//ת����������
		try {
			String sql1 = "insert into emp_enti_change (car_no, ana_date, change_time,service_num ) values (?,to_char(?,'yyyy-mm-dd'),?,?)";
			StatementHandle stmt = conn
					.prepareStatement(sql1); //���س���
			
			try {
				String sql2 = "select car_no,count(*) sum from single_business_data_bs where  to_char(date_down,'yyyy-mm-dd')=? group by car_no ";
				StatementHandle stmt2 = conn.prepareStatement(sql2);//���� ͳ������
				stmt2.setString(1, anadate);
				ResultSet sets = stmt2.executeQuery();
				
				while(sets.next()){
					dInfo = new DestInfo();
					dInfo.destNo = sets.getString("car_no");//��single_business_data_bs���л�ó��ƺ�
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
			
			System.out.println("��ʼʱ�䣬����ʱ�䣬������:"
					+ GeneralConst.YYYY_MM_DD.format(sDate) + " "
					+ GeneralConst.YYYY_MM_DD.format(eDate) + " "
					+ destList.size());
			
			InfoContainer queryInfo = new InfoContainer();
			queryInfo.setInfo(TrackServer.START_DATE_FLAG, sDate);
			queryInfo.setInfo(TrackServer.END_DATE_FLAG, eDate);
			TrackInfo trackInfo;//�켣����Ϣ
			
			int preStatus=0,status = 0,busytofree=0,freetobusy=0;//��ʼ��ǰһ���켣��,��ǰ�켣��Ϳ��г�����
			
 			for(Iterator<DestInfo> itr = destList.iterator(); itr.hasNext();) { //����ÿ����
				dInfo = (DestInfo) itr.next();
				queryInfo.setInfo(TrackServer.DEST_NO_FLAG, dInfo.destNo);//��ó����Ĺ켣����Ϣ
				
				trackInfo = new TrackInfo();
				trackInfo.dInfo = dInfo;
				trackInfo.sDate = sDate;
				trackInfo.eDate = eDate;
				trackInfo.trackArr = TrackServer.getSingleInstance().getTrackInfo(queryInfo);
				
				System.out.println(TrackServer.getSingleInstance()
						.getTrackFile(dInfo.destNo, sDate).getPath());  //��ӡ�����켣���·��
				
				if(trackInfo.trackArr != null && trackInfo.trackArr.length > 0){
					
					for(int i = 0; i < trackInfo.trackArr.length; i++){ //����ÿһ���켣��
						status = (trackInfo.trackArr[i].getInteger(
								TrackIO.DEST_STATUS_FLAG).intValue() & 0x0f);//��ǰ���г�״̬
						if (i == 0) {
							preStatus = status;
							continue;
						}
						
						if(preStatus!=0&&status==0){
							busytofree++;  //�س���ճ�
						}else if(preStatus!=1&&status==1){
							freetobusy++;  //�ճ����س�
						}			
						preStatus = status;
					}
					
					System.out.println("destno:" + dInfo.destNo + "sdate" + anadate + "busytofree" + busytofree+"freetobusy" + freetobusy);
					stmt.setString(1, dInfo.destNo);//���ƺ�
					stmt.setDate(2, date);//����ʱ��
					stmt.setInt(3, busytofree);//���س��仯����
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
