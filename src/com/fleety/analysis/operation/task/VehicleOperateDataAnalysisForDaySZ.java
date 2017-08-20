package com.fleety.analysis.operation.task;

import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import server.db.DbServer;

import com.fleety.analysis.AnalysisServer;
import com.fleety.analysis.operation.IOperationAnalysis;
import com.fleety.base.InfoContainer;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;

public class VehicleOperateDataAnalysisForDaySZ implements IOperationAnalysis{
	
	private HashMap          vehicleMapping  = null;
	private HashMap          exceptionMapping  = null;
	private SimpleDateFormat sdf             = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private SimpleDateFormat sdf2            = new SimpleDateFormat("yyyy-MM-dd");
	private float            fuelSurcharges_h  = 0; //�쳵ȼ�͸��ӷ�
	private float            fuelSurcharges_l  = 0; //�̳�ȼ�͸��ӷ�
	private float            fuelSurcharges_d  = 0;//�綯��
	private float            fuelSurcharges_w  = 0;//���ϰ�ȼ�͸��ӷ�
	
	List<filterInfo> list = null;
	
    private int total_car = 0;
    private int notfit_car = 0;
	
	@Override
	public boolean startAnalysisOperation(AnalysisServer parentServer,InfoContainer statInfo)
	{
		// ��ȡ��˾map�Ͷ�ȡȼ�͸��ӷ���Ϣ
		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
		Date eTime = statInfo.getDate(STAT_END_TIME_DATE);
		this.vehicleMapping = null;
		this.exceptionMapping = null;
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try{
			boolean bol = queryCondition();
			if(!bol){
				System.out.println("VehicleOperateDataAnalysisForDaySZ ȼ�͸��ӷ��������������������������");
				return false;
			}
			StatementHandle stmt = conn.createStatement();
			StringBuilder sb = new StringBuilder();
			sb.append("select count(*) as sum from ana_business_day_stat_sz ")
			  .append(" where stat_time = to_date('").append(sdf2.format(sTime)).append("','yyyy-MM-dd')");
			System.out.println(sb.toString());
			ResultSet sets = stmt.executeQuery(sb.toString());
			if(sets.next()){
				int sum = sets.getInt("sum");
				if(sum == 0){
					this.vehicleMapping = new HashMap();
					this.exceptionMapping = new HashMap();				
	}		
			}
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
		
		if(this.vehicleMapping == null){
			System.out.println("Not Need Analysis:"+this.toString());
		}else{
			System.out.println("Start Analysis:"+this.toString());
		}
		
		return this.vehicleMapping != null;
	}

	private boolean queryCondition() throws Exception{
		this.list = new ArrayList<filterInfo>();
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try {
			StatementHandle stmt = conn.createStatement();
			ResultSet sets = stmt.executeQuery("select * from business_screen_condition  order by operator_time desc");
			while(sets.next()){
				filterInfo fi =new filterInfo();
				fi.con_one_dura = sets.getInt("con_one_dura");//����Ӫ��ʱ������� 1
				fi.con_one_mile = sets.getDouble("con_one_mile");//����Ӫ�����С���� 0.3
				fi.con_two_num_m = sets.getInt("con_two_num_m");// Ӫ�˱���С���� 15
				fi.con_two_num_b = sets.getInt("con_two_num_b");// Ӫ�˱��������� 85
				fi.con_three_mony_b = sets.getDouble("con_three_mony_b");// ����һ��Ӫ�˽������� 1500
				fi.con_three_mony_m = sets.getDouble("con_three_mony_m");//����һ��Ӫ�˽��С���� 500
				fi.con_four_mile_m = sets.getDouble("con_four_mile_m");// ����һ���ܹ���С���� �̵�170�����190
				fi.con_four_mile_b = sets.getDouble("con_four_mile_b");// ��������Ӫ����̴����� 200
				fi.con_five_time1 = sets.getInt("con_five_time1");//ɸѡ��ʼʱ��
				fi.con_five_time2 = sets.getInt("con_five_time2");//ɸѡ����ʱ��
				fi.con_five_dura = sets.getInt("con_five_dura");//ɸѡ2������ʱ������4Сʱ
				fi.filter_type = sets.getInt("id");
				if(fi.filter_type!=0)//δ�������ʱ���ܻ��0�ӽ�ȥ
				{
				list.add(fi);
				}
			}
			sets.close();
			stmt.close();
			stmt = conn.prepareStatement("select id,val,type from cost_manage where (valid_start_time is null or valid_start_time<=?) and (valid_end_time is null or valid_end_time>=?) order by id ");
			stmt.setDate(1, new java.sql.Date(new Date().getTime()));
			stmt.setDate(2, new java.sql.Date(new Date().getTime()));
			sets = stmt.executeQuery();
			while (sets.next()) {
				String temp = sets.getString("type");
				if(temp!=null&&temp.equals("���ȼ�͸��ӷ�")){
					this.fuelSurcharges_h = sets.getFloat("val");
				}else if (temp!=null&&temp.equals("�̵�ȼ�͸��ӷ�")) {
					this.fuelSurcharges_l = sets.getFloat("val");
				}else if (temp!=null&&temp.equals("���ϰ���ʿȼ�͸��ӷ�")) {
					this.fuelSurcharges_w = sets.getFloat("val");
				}
			}
			sets.close();
			stmt.close();
//			if(fuelSurcharges_h==0 || fuelSurcharges_l==0 || fuelSurcharges_w==0){
//				return false;
//			}
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
		return true;
	}

	@Override
	public void analysisDestOperation(AnalysisServer parentServer, InfoContainer statInfo)
	{   
		for(int j=0;j<list.size();j++){
		this.vehicleMapping = new HashMap();
		this.exceptionMapping = new HashMap();	
		Date sTime = statInfo.getDate(STAT_START_TIME_DATE);
		Date eTime = statInfo.getDate(STAT_END_TIME_DATE);
		System.out.println("sTime:"+sdf.format(sTime)+" eTime:"+sdf.format(sTime));
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try{
			StringBuilder sql = new StringBuilder();
			sql.append("select * from (select * from (")
			   .append(" select dispatch_car_no as plate_no,taxi_company as company_id,")
			   .append("  (distance+free_distance) as total_distance,")
			   .append("  decode(sign(distance),1,distance,-1,0,distance) as work_distance,")
			   .append("  free_distance,date_up,date_down,")
			   .append("  waiting_hour,")
			   .append("  waiting_minute,")
			   .append("  waiting_second,")
			   .append("  sum work_income")
			   .append(" from SINGLE_BUSINESS_DATA_BS ")
			   .append(" where dispatch_car_no is not null ")
			   .append("       and date_up >= to_date('").append(sdf.format(sTime)).append("','yyyy-mm-dd hh24:mi:ss')")
			   .append("       and date_up <= to_date('").append(sdf.format(eTime)).append("','yyyy-mm-dd hh24:mi:ss')")//shijian
			   .append(") a")
			   .append(" left join ")
			   .append(" (")
			   .append("   select TERM_ID,TERM_NAME as company_name from term")
			   .append(" ) d on a.company_id = d.TERM_ID ")
			   .append("left join ")
			   .append(" (select car_id,type_id from car) car on car.car_id=a.plate_no")
			   .append(" ) order by date_up");
			System.out.println(sql.toString());
			StatementHandle stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql.toString());
			Calendar startTime = Calendar.getInstance();
			startTime.setTimeInMillis(sTime.getTime());
			//startTime.add(Calendar.DAY_OF_MONTH, -1);
			startTime.set(Calendar.HOUR_OF_DAY, list.get(j).con_five_time1);
			startTime.set(Calendar.MINUTE, 0);
			startTime.set(Calendar.SECOND, 0);
			startTime.set(Calendar.MILLISECOND, 0);
			Calendar endTime = Calendar.getInstance();
			endTime.setTimeInMillis(sTime.getTime());
			//endTime.add(Calendar.DAY_OF_MONTH, -1);
			endTime.set(Calendar.HOUR_OF_DAY, (list.get(j).con_five_time2-1));
			endTime.set(Calendar.MINUTE, 23);
			endTime.set(Calendar.SECOND, 59);
			endTime.set(Calendar.MILLISECOND, 59);
			VehicleOperateInfo vInfo =  null;
			int count=0;
			int record=0;
			while(rs.next())
			{
				count++;
				String plateNo = rs.getString("plate_no");
				if(vehicleMapping.containsKey(plateNo)){
					vInfo = (VehicleOperateInfo)vehicleMapping.get(plateNo);
				}else {
					vInfo = new VehicleOperateInfo();
				}
				vInfo.startTime = sTime;
				vInfo.plateNo = plateNo;
				vInfo.companyId = rs.getInt("company_id");
				vInfo.companyName = rs.getString("company_name");
				vInfo.workTimes++;// Ӫ�˴���
				//kaka �������ӳ������ͣ�
				
				
				long dateUp = rs.getTimestamp("date_up").getTime();
				long dateDown = rs.getTimestamp("date_down").getTime();
				float workDistance = rs.getFloat("work_distance");
				boolean isAdd = false;//false���ϸ�true�ϸ�
				//����һ��//      ����Ӫ��ʱ������� 1  &&      ����Ӫ�˾������ 0.3
				if(dateDown-dateUp>=(list.get(j).con_one_dura*60*1000)&&workDistance>=list.get(j).con_one_mile){
					vInfo.conditionOne++;
					isAdd = true;
				}else {
					isAdd = false;
				}
				//������  �ϳ�ʱ����ͳ��ʱ���24Сʱ֮��;����Ӫ������֮�����con_five_dura ʱ��
				if(vInfo.dateUp==0L)
				{
					vInfo.conditionFive++;
					isAdd = true;
				}
				else if(vInfo.dateUp>=startTime.getTimeInMillis()&&vInfo.dateUp<=endTime.getTimeInMillis()
						&&dateUp>=startTime.getTimeInMillis()&&dateUp<=endTime.getTimeInMillis()
						&&dateUp-vInfo.dateDown<=list.get(j).con_five_dura*60*60*1000){
					vInfo.conditionFive++;
					isAdd = true;
				}else {
					isAdd = false;
				}
				//���ʹ��������con_four_mile_b������С��200����ϸ�
				if(rs.getFloat("total_distance")<=list.get(j).con_four_mile_b){
					vInfo.conditionFour++;
					isAdd = true;
				}else {
					isAdd = false;
				}
				vInfo.dateUp = dateUp;
				vInfo.dateDown = dateDown;
				if(isAdd){
					record++;
					vInfo.totalDistance += rs.getFloat("total_distance");// ��ʻ���
					vInfo.workDistance += workDistance;// Ӫ�����
				    vInfo.freeDistance += rs.getFloat("free_distance");// ��ʻ���
					vInfo.waitingHour += rs.getInt("waiting_hour");// ���ٵȺ�ʱ��(ʱ)
					vInfo.waitingMinute += rs.getInt("waiting_minute");// ���ٵȺ�ʱ��(��)
					vInfo.waitingSecond += rs.getInt("waiting_second");// ���ٵȺ�ʱ��(��)
					vInfo.workIncome += rs.getFloat("work_income");//�Ƽ�����
					vInfo.typeId = rs.getInt("type_id");//
					vInfo.filter_type = list.get(j).filter_type;
					vInfo.condition ++ ;//�ϸ�Ӫ�˴���
				}
				vehicleMapping.put(plateNo, vInfo);
			}
			System.out.println("���ԭʼ�����в�ѯ��Ҫ����:"+count+" ������.isAdd "+record+" �����ݡ�");
			List tempList = new ArrayList();
			Map<String,VehicleOperateInfo> tempMap = new HashMap<String, VehicleOperateInfo>();
			for (Iterator iterator = vehicleMapping.keySet().iterator(); iterator.hasNext();) {
				String plateNo = (String) iterator.next();
				vInfo = (VehicleOperateInfo)vehicleMapping.get(plateNo);
				if(vInfo.typeId==1){//ȼ�͸��ӷ�*�ϸ�Ӫ�˴���*�ٷֱ�
					vInfo.fuelIncome += (fuelSurcharges_h * vInfo.condition*0.9);// ȼ�͸�������
				}else if(vInfo.typeId==2||vInfo.typeId==9){//���������̵ġ�
					vInfo.fuelIncome += (fuelSurcharges_l * vInfo.condition*0.9);// ȼ�͸�������
				}else if(vInfo.typeId==4){
					vInfo.fuelIncome += (fuelSurcharges_w * vInfo.condition*0.9);// ȼ�͸�������
				}
				vInfo.totalIncome = vInfo.totalIncome + vInfo.workIncome + vInfo.fuelIncome;// ������
				vehicleMapping.put(plateNo, vInfo);
				//����һ
				boolean is_exception = false;
				if(vInfo.conditionOne<=0){

					is_exception = true;
				}
				//������
				else if(vInfo.conditionFive<=0){
					is_exception = true;
				}
				//������ ����Ӫ�˴������� con_two_num_m 85���ϸ� ��Ӫ�˴���С��con_two_num_b 15���ϸ�
				else if(!(vInfo.workTimes>=list.get(j).con_two_num_m&&vInfo.workTimes<=list.get(j).con_two_num_b)){

					is_exception = true;
				}
				//������ ����Ӫ��������� con_three_mony_b 
				else if(!(vInfo.workIncome>=list.get(j).con_three_mony_m&&vInfo.workIncome<=list.get(j).con_three_mony_b)){

					is_exception = true;
				}
				//������ ����Ӫ�˾��� ���� �̵�170�����190��
				else if(vInfo.conditionFour<=0){				
					is_exception = true;
				}
				else if(vInfo.totalDistance<170&&(vInfo.typeId==2||vInfo.typeId==9)){
					is_exception = true;
				}
				else if(vInfo.totalDistance<190&&(vInfo.typeId!=2||vInfo.typeId!=9)){
					is_exception = true;
				}
				if(is_exception){
					vInfo.isfit=1;
					vInfo.filter_type = list.get(j).filter_type;
					exceptionMapping.put(plateNo, vInfo);
				}
			}
			String temp = null;
			total_car= vehicleMapping.keySet().size();
			Iterator<String> it = exceptionMapping.keySet().iterator();
			notfit_car = exceptionMapping.keySet().size();
			while(it.hasNext()){
				temp = it.next();
				vehicleMapping.remove(temp);
			}
			endAnalysisOperationSaveData(parentServer,statInfo);
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
		}
	}
	
	public void endAnalysisOperationSaveData(AnalysisServer parentServer,InfoContainer statInfo) {
		if(this.vehicleMapping == null){ 
			return ;
		}
		int recordNum = 0;
		String plateNo = "";
		VehicleOperateInfo vehicleOperateInfo;
		Date sDate = statInfo.getDate(STAT_START_TIME_DATE);
		DbHandle conn = DbServer.getSingleInstance().getConn();
		try{
			conn.setAutoCommit(false);
			StatementHandle stmt = conn
					.prepareStatement("insert into ana_business_day_stat_sz(id,dest_no,company_id,company_name," +
							"business_num,mile,business_mile,free_mile,waiting_hour,waiting_minute,waiting_second," +
							"business_money_t,business_money,stat_time,condition,type_id,is_fit,filter_type) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
			for(Iterator itr = this.vehicleMapping.keySet().iterator();itr.hasNext();){
				plateNo = (String)itr.next();
				vehicleOperateInfo = (VehicleOperateInfo)this.vehicleMapping.get(plateNo);
				stmt.setInt(1, (int)DbServer.getSingleInstance().getAvaliableId(conn, "ana_business_day_stat_sz", "id"));
				stmt.setString(2, vehicleOperateInfo.plateNo);
				stmt.setInt(3, vehicleOperateInfo.companyId);
				stmt.setString(4, vehicleOperateInfo.companyName);
				stmt.setInt(5, vehicleOperateInfo.workTimes);
				stmt.setFloat(6, vehicleOperateInfo.totalDistance);
				stmt.setFloat(7, vehicleOperateInfo.workDistance);
				stmt.setFloat(8, vehicleOperateInfo.freeDistance);
				stmt.setInt(9, vehicleOperateInfo.waitingHour);
				stmt.setInt(10, vehicleOperateInfo.waitingMinute);
				stmt.setInt(11, vehicleOperateInfo.waitingSecond);
				stmt.setFloat(12, vehicleOperateInfo.totalIncome);
				stmt.setFloat(13, vehicleOperateInfo.workIncome);
				stmt.setDate(14, new java.sql.Date(vehicleOperateInfo.startTime.getTime()));
				stmt.setInt(15, vehicleOperateInfo.condition);
				stmt.setInt(16, vehicleOperateInfo.typeId);
				stmt.setInt(17, vehicleOperateInfo.isfit);
				stmt.setInt(18, vehicleOperateInfo.filter_type);
				stmt.addBatch();
				recordNum ++;
				if(recordNum%200==0){
					stmt.executeBatch();
				}
			}
			stmt.executeBatch();
			
			//���ϸ����ĳ��ƺ�Ҳ����
			for(Iterator itr = this.exceptionMapping.keySet().iterator();itr.hasNext();){
				plateNo = (String)itr.next();
				vehicleOperateInfo = (VehicleOperateInfo)this.exceptionMapping.get(plateNo);
				stmt.setInt(1, (int)DbServer.getSingleInstance().getAvaliableId(conn, "ana_business_day_stat_sz", "id"));
				stmt.setString(2, vehicleOperateInfo.plateNo);
				stmt.setInt(3, vehicleOperateInfo.companyId);
				stmt.setString(4, vehicleOperateInfo.companyName);
				stmt.setInt(5, 0);
				stmt.setFloat(6, 0);
				stmt.setFloat(7, 0);
				stmt.setFloat(8, 0);
				stmt.setInt(9, 0);
				stmt.setInt(10,0);
				stmt.setInt(11, 0);
				stmt.setFloat(12, 0);
				stmt.setFloat(13, 0);
				stmt.setDate(14, new java.sql.Date(vehicleOperateInfo.startTime.getTime()));
				stmt.setInt(15, 0);
				stmt.setInt(16, vehicleOperateInfo.typeId);
				stmt.setInt(17, vehicleOperateInfo.isfit);
				stmt.setInt(18, vehicleOperateInfo.filter_type);
				stmt.addBatch();
				recordNum ++;
				if(recordNum%200==0){
					stmt.executeBatch();
				}
			}
			stmt.executeBatch();
			conn.commit();
			int singlecount=0;
			int doublecount=0;
			//�Ե綯����˫��״��λ���и���
			String singleElectric="select car_id, count(*) from v_ana_driver_info where car_id in (select dest_no from v_ana_dest_info where type_id=3 or type_id=7 ) group by car_id having count(*)=1";
			String doubleElectric="select car_id, count(*) from v_ana_driver_info where car_id in (select dest_no from v_ana_dest_info where type_id=3 or type_id=7 ) group by car_id having count(*)=2";
			StatementHandle stmtElectric=  conn.createStatement();
			Calendar calendar = Calendar.getInstance();//��ʱ��ӡ����ȡ����ϵͳ��ǰʱ��
	        calendar.add(Calendar.DATE, -1);    //�õ�ǰһ��
	        String  yestedayDate= new SimpleDateFormat("yyyy-MM-dd").format(calendar.getTime());
	        StatementHandle updateStmt=conn.prepareStatement("update ana_business_day_stat_sz set single_doubel_stat=?"
	        		+ " where dest_no=? and stat_time>=to_date(?,'yyyy-mm-dd')");
			ResultSet singleSet=stmtElectric.executeQuery(singleElectric);
			while (singleSet.next()) {
				String car_id=singleSet.getString("car_id");
				updateStmt.setInt(1, 1);//����
				updateStmt.setString(2, car_id);
				updateStmt.setString(3, yestedayDate);
				updateStmt.addBatch();
				singlecount++;
			}
			updateStmt.executeBatch();
			System.out.println("count");
			ResultSet doubleSet=stmtElectric.executeQuery(doubleElectric);
			while (doubleSet.next()) {
				String car_id=doubleSet.getString("car_id");
				updateStmt.setInt(1, 2);//˫��
				updateStmt.setString(2, car_id);
				updateStmt.setString(3, yestedayDate);
				updateStmt.addBatch();
				doublecount++;
			}
			updateStmt.executeBatch();
			conn.commit();
			System.out.println("singlecount:"+singlecount+"--doublecount:"+doublecount);
		}catch(Exception e){
			e.printStackTrace();
			if(conn != null){
				try{
					conn.rollback();
				}catch(Exception ee){
					ee.printStackTrace();
				}
			}
			recordNum = 0;
		}finally{
			DbServer.getSingleInstance().releaseConn(conn);
		}
		System.out.println("Finish vehicle operate data Analysis:"+this.toString()+" recordNum="+recordNum);
	}
	
	public void endAnalysisOperation(AnalysisServer parentServer,InfoContainer statInfo) {
		return ;
	}
	
	private class VehicleOperateInfo
	{
		public String plateNo;//���ƺ�
		public int    companyId;//��˾id
		public String companyName;//��˾��
		private int   typeId;//����
		public int    workTimes = 0;//Ӫ�˴���
		public float  totalDistance = 0;//��ʻ����
		public float  workDistance = 0;//Ӫ�˾���
		public float  freeDistance = 0;//��ʻ����
		public int    waitingHour = 0;//�Ⱥ�
		public int    waitingMinute = 0;
		public int    waitingSecond = 0;
		public float  totalIncome = 0;//�ܽ��
		public float  workIncome = 0;//Ӫ�˽��
		public float  fuelIncome = 0;//ȼ�ͽ��
		public Date   startTime;//ʱ��
		public long dateUp = 0;
		public long dateDown = 0;
		public int conditionOne = 0;
		public int conditionFive = 0;
		public int conditionFour=0;
		public int condition = 0;
		public int isfit = 0 ;//����Ӫ���Ƿ��Ƿ�ϸ�Ĭ��ֵ0�ϸ�1���ϸ�
		public int filter_type = 0;//���˷�����
	}
	
	private class filterInfo{
		public int con_one_dura = 0;//����Ӫ��ʱ��
		public double con_one_mile = 0;//����Ӫ�����
		public int con_two_num_m = 0;//Ӫ�˱���������
		public int con_two_num_b = 0;//-Ӫ�˱���С����
		public double con_three_mony_b = 0;//Ӫ�˽��С����
		public double con_three_mony_m = 0;//Ӫ�˽�������
		public double con_four_mile_m = 0;//�ܹ���С����
		public double con_four_mile_b = 0;//�ܹ���С����
		public int con_five_time1 = 0;//ɸѡ��ʼʱ��
		public int con_five_time2 = 0;//ɸѡ����ʱ��
		public int con_five_dura = 0;//ɸѡ2������ʱ��
		public int filter_type = 0;//���˷�����
	}
}
