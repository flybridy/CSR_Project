package com.fleety.job.task;

import java.io.File;
import java.io.FileInputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import server.db.DbServer;
import server.track.TrackServer;

import com.fleety.base.GeneralConst;
import com.fleety.base.InfoContainer;
import com.fleety.track.TrackIO;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.fleety.util.pool.thread.BasicTask;

public class OperationDstanceAnasisTask extends BasicTask {
	public static void main(String[] args) {
		 SimpleDateFormat sd= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		;
		System.out.println(sd.format(new Date(1494172800000l)));
	}
	int record=0;
	int commit=0;
	public boolean execute() throws Exception {
		Map<String, OpreationInfo> resMap=new HashMap<String, OpreationInfo>();
		DbHandle conn=DbServer.getSingleInstance().getConnWithUseTime(-1);
		StatementHandle stm=conn.createStatement();
		
		Date e_date=new Date();
		
		long e_time=e_date.getTime();
		long s_time_long=e_time-30*60*1000;//查询最近半小时内上传的营运数据。
		Date s_date=new Date(s_time_long);
		String s_str=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(s_date);
		String e_str=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(e_date);
	
		String sql="select CAR_NO,DATE_UP,DATE_DOWN,DISTANCE,FREE_DISTANCE,SUM,RECODE_TIME from SINGLE_BUSINESS_DATA_BS where DATE_UP>=to_date('"+s_str+"','yyyy-mm-dd hh24:mi:ss')" +"and DATE_UP<=to_date('"+e_str+"','yyyy-mm-dd hh24:mi:ss')";
		System.out.println("GPSmileana_time:"+s_str+" "+e_str+" \n  GPSmileana_sql:"+sql);
		OpreationInfo info=null;
		int count=0;
		ResultSet res=stm.executeQuery(sql);
		while(res.next()){
			count++;
			info=new OpreationInfo();
			String car_no=res.getString("CAR_NO");
			info.car_no=car_no;
			info.date_up = res.getTimestamp("DATE_UP");//营运开始时间
			info.date_down = res.getTimestamp("DATE_DOWN");
			info.record_time = res.getTimestamp("RECODE_TIME");
			info.distance=res.getDouble("DISTANCE");
			info.sum=res.getDouble("SUM");
			List<Double> reslist=analysisTrack(info.date_up ,info.date_down,car_no);		
			info.gps_distance = round(reslist.get(0),2);//GPS计算的营运里程
			info.gps_points=reslist.get(1);
			
			info.area_size=reslist.get(2);
			info.empty_points=reslist.get(3);
			info.load_points=reslist.get(4);
			info.task_points=reslist.get(5);
			info.other_points=reslist.get(6);
			info.operate_time=(int) ((info.date_down.getTime()-info.date_up.getTime())/60000);
			resMap.put(car_no, info);
		}
		System.out.println("GPS里程分析查询到数据条数："+count);
		if(count!=0){
			StatementHandle stmt = conn.prepareStatement("insert into GPS_BUSINESS_ANALYSIS (id ,car_no ,date_up ,date_down ,sum ,gps_distance,distance,gps_points,area_size,empty_points,load_points,task_points,other_points,operate_time,record_time ) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
			for (Iterator it = resMap.keySet().iterator(); it.hasNext();) {
				commit++;
				OpreationInfo op=resMap.get(it.next());
				stmt.setInt(1, (int)DbServer.getSingleInstance().getAvaliableId(conn, "GPS_BUSINESS_ANALYSIS", "id"));
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
				stmt.setTimestamp(15, new Timestamp(op.record_time.getTime()));
				stmt.addBatch();
				try {
						stmt.executeBatch();
				} catch (Exception e) {
					 System.out.println("data exption"+op.toString());
				}
			}				
				conn.commit();
			
		}
		DbServer.getSingleInstance().releaseConn(conn);
		return false;
	}
	private ArrayList<Double> analysisTrack(Date start,Date end,String car_no) throws Exception{
		ArrayList<Double> list=new ArrayList();
		double gps_points=0;
		InfoContainer queryInfo = new InfoContainer();
		queryInfo.setInfo(TrackServer.START_DATE_FLAG,start);
		queryInfo.setInfo(TrackServer.END_DATE_FLAG, end);
		queryInfo.setInfo(TrackServer.DEST_NO_FLAG, car_no);
		record++;
		InfoContainer[] trackArr = TrackServer.getSingleInstance().getTrackInfo(queryInfo);	
		double distance=0;
		double old_lo=0;
		double old_la=0;
		double new_lo=0;
		double new_la=0;
		double max_la=0;//纬度
		double max_lo=0;//经度
		double min_la=0;//纬度
		double min_lo=0;//经度
		double load_points=0;//重车点数
		double empty_points=0;//空车点数
		double task_points=0;//任务车点数
		double other_points=0;//未知状态点数
		int state=-1;
		for(int i=0;i<trackArr.length;i++){
			    gps_points++;
				new_lo=trackArr[i].getDouble(TrackIO.DEST_LO_FLAG);
				new_la=trackArr[i].getDouble(TrackIO.DEST_LA_FLAG);
			    state =trackArr[i].getInteger(TrackIO.DEST_STATUS_FLAG).intValue()&0x0F;
				if(state == 0){//空车
					empty_points++;
				}
				else if(state == 1){//重车
					load_points++;
				}
				else if(state==2){//任务车
					task_points++;
				}else{//未知状态点
					other_points++;
				}
				if (old_lo > 0 || old_la > 0) {
					double dis = countDistance(new_lo,new_la,old_lo,old_la);
						distance += dis;
				}
				if(new_lo>max_lo){
					max_lo=new_lo;
				}
				if(new_lo<min_lo||min_lo==0){
					min_lo=new_lo;
				}
				if(new_la>max_la){
					max_la=new_la;
				}
				if(new_la<min_la||min_la==0){
					min_la=new_la;
					}
				old_lo = new_lo;
				old_la = new_la;
			}	
		
		DecimalFormat df = new DecimalFormat("#.##");
		DecimalFormat df2 = new DecimalFormat("#");
		
		double all_distance = countDistance(max_lo,max_la,min_lo,min_la);
		
		String area=df.format((all_distance*all_distance)/2);
		double area_size=Double.parseDouble(area);
		list.add(distance);
		list.add(gps_points);
		list.add(area_size);//营运面积
		list.add(empty_points);//空
		list.add(load_points);//重
		list.add(task_points);//任务
		list.add(other_points);//其它
		return list;
	}
	private static double rad(double d)
	{
		return d * Math.PI / 180.0;
	}
	private static double EARTH_RADIUS = 6378.137;
	public static double countDistance(double lo1, double la1, double lo2,
			double la2)
	{
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
	class OpreationInfo{
		String car_no;
		Date date_up;
		Date date_down;
		Date record_time;
		
		double sum;
		double distance;
		double gps_distance;
		double gps_points;
		
		int operate_time;//营运时长
		double area_size;//面积
		double load_points=0;//重车点数
		double empty_points=0;//空车点数
		double task_points=0;//任务车点数
		double other_points=0;//未知状态点数
		@Override
		public String toString() {
			return "OpreationInfo [car_no=" + car_no + ", date_up=" + date_up
					+ ", date_down=" + date_down + ", record_time="
					+ record_time + ", sum=" + sum + ", distance=" + distance
					+ ", gps_distance=" + gps_distance + ", gps_points="
					+ gps_points + ", operate_time=" + operate_time
					+ ", area_size=" + area_size + ", load_points="
					+ load_points + ", empty_points=" + empty_points
					+ ", task_points=" + task_points + ", other_points="
					+ other_points + "]";
		}
		
		
		
	}	
}
