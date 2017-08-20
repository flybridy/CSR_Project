package com.csr.ana;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.fleety.util.pool.thread.BasicTask;
import com.labServer.manager.LabDisplayParamterManager;
import com.labServer.manager.LabDisplayParamterManagerImpl;
import com.labServer.manager.LabDisprobeNumberManager;
import com.labServer.manager.LabDisprobeNumberManagerImpl;
import com.labServer.manager.LabModifyManager;
import com.labServer.manager.LabModifyManagerImpl;
import com.labServer.model.LabDisplayParamter;
import com.labServer.model.LabDisprobeNumber;
import com.labServer.model.LabInputParamter;
import com.labServer.model.LabModify;
import com.labServer.util.RegexUtil;
import com.labServer.util.SCMUtil;

import server.db.DbServer;

public class SaveTask extends BasicTask{
	
	String messageInfo;
	
	
	public SaveTask(String messageInfo) {
		super();
		this.messageInfo = messageInfo;
	}
	private LabModifyManager labModifyManager = new LabModifyManagerImpl();
	private static LabDisprobeNumberManager labDisprobeNumberManager = new LabDisprobeNumberManagerImpl();
	private static Map<String, LabDisprobeNumber> labDisprobeNumber = labDisprobeNumberManager.resultSetToListFromDisProbe();// 显示数据实例
	// 查找该探头的校准
	Map<String, LabModify> modifys = labModifyManager.resultSetToMapFromModify();
	
	
	
	@Override
	public boolean execute() throws Exception {
		
		 LabDisplayParamterManager labDisplayParamterManager = new LabDisplayParamterManagerImpl();
		  
		 
		 
		 List<String> sql_list=new ArrayList<>();
		 String inputProbNum = "";// 板号+端口�?
		String createdOn = "";// 采集时间（单片机端）
		Double temperature;// 采集温度
		Double humidity;// 采集湿度
		double tempCheck = -25.00;// 设置25摄氏度为�?��采集温度
		double humCheck = -20.00;// 设置20度为�?��采集湿度
		
		String displayProbNum = "";// 探头编号（客户定制）
		String displayTabName = "";// 显示表名
		String inputTabName = "";// 原数据表�?
		LabInputParamter labInputParamter;
		LabDisplayParamter labDisplayParamter;
		
		String[] paramterStr = SCMUtil.getArrayFromSCM(RegexUtil.getParams(messageInfo));// 将长数据按分号分割成数组
	    System.out.println("parse data:  "+paramterStr[0]+" "+paramterStr[1]);
		for (int i = 0; i < paramterStr.length; i++) {					
			String[] paramters = SCMUtil.getParamterFromArray(paramterStr[i]);
			inputProbNum = paramters[0];
			createdOn = SCMUtil.getSimpledDateTime();
			temperature = Double.valueOf(paramters[1]);
			humidity = Double.valueOf(paramters[2]);
			// 查找原数据表
			inputTabName = labDisprobeNumber.get(inputProbNum).getTab_InputName();
			// 查找显示表名
			displayTabName = labDisprobeNumber.get(inputProbNum).getTab_DisplayName();
			// 查找该探头对应的商
			displayProbNum = labDisprobeNumber.get(inputProbNum).getDisplayProbeNumber();

			if (Double.valueOf(temperature) > tempCheck && Double.valueOf(humidity) > humCheck) {
				// 组装原数据对
				labInputParamter = new LabInputParamter(inputProbNum, createdOn, temperature, humidity,
						inputTabName);
				// 写入原数据分表（为了数据优化只能舍弃原数据的分表批量业务									
				// 组装显示显示数据对象
				labDisplayParamter = new LabDisplayParamter(inputProbNum, displayProbNum, createdOn,
						temperature, humidity, displayTabName);		
				labDisplayParamterManager.calParamterByModify(labDisplayParamter, modifys);
				// 加入显示批量数据，表二
				String table_2="insert into "+labDisplayParamter.getDisplayTableName()+" (INPUTPROBENUMBER,DISPROBENUMBER,CREATEDON,DISTEMPERATURE,DISHUMIDITY)VALUES('"+ labDisplayParamter.getInputProbeNumber()+"','"+labDisplayParamter.getDisProbeNumber()+"','"+labDisplayParamter.getCreatedOn()+"',"+labDisplayParamter.getDisTemperature()+","+labDisplayParamter.getDisHumidity()+")";
				String table_2_all="insert into lab_displayparamter (INPUTPROBENUMBER,DISPROBENUMBER,CREATEDON,DISTEMPERATURE,DISHUMIDITY)VALUES('"+ labDisplayParamter.getInputProbeNumber()+"','"+labDisplayParamter.getDisProbeNumber()+"','"+labDisplayParamter.getCreatedOn()+"',"+labDisplayParamter.getDisTemperature()+","+labDisplayParamter.getDisHumidity()+")";				
				//displayQueue.add(labDisplayParamter);	
				sql_list.add(table_2_all);
				sql_list.add(table_2);
			}
		}
	
		DbHandle con=DbServer.getSingleInstance().getConn();
		
		try {
			con.setAutoCommit(false);
			StatementHandle stmt=con.createStatement();
			System.out.println("sql_list_size:"+sql_list.size());
			for(int i=0;i<sql_list.size();i++){
				stmt.addBatch(sql_list.get(i));
				//System.out.println("save date sqls::"+sql_list.get(i));
			}
			stmt.executeBatch();
			con.commit();
			DbServer.getSingleInstance().releaseConn(con);
		} catch (SQLException e) {
			e.printStackTrace();
		}				
		return false;
	}

}
