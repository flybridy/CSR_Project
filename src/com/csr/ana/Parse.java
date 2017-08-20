package com.csr.ana;

import java.util.Map;
import java.util.concurrent.BlockingQueue;

import com.fleety.util.pool.timer.FleetyTimerTask;
import com.labServer.manager.LabDisplayParamterManager;
import com.labServer.manager.LabDisplayParamterManagerImpl;
import com.labServer.manager.LabDisprobeNumberManager;
import com.labServer.manager.LabDisprobeNumberManagerImpl;
import com.labServer.manager.LabInputParamterManager;
import com.labServer.manager.LabInputParamterManagerImpl;
import com.labServer.manager.LabModifyManager;
import com.labServer.manager.LabModifyManagerImpl;
import com.labServer.model.LabDisplayParamter;
import com.labServer.model.LabDisprobeNumber;
import com.labServer.model.LabInputParamter;
import com.labServer.model.LabModify;
import com.labServer.util.RegexUtil;
import com.labServer.util.SCMUtil;

public class Parse extends FleetyTimerTask{
	
	private BlockingQueue<String> reciverQueue;
	private BlockingQueue<LabDisplayParamter> displayQueue;
	private BlockingQueue<LabInputParamter> inputQueue;
	private LabInputParamterManager labInputParamterManager = new LabInputParamterManagerImpl();
	private LabDisplayParamterManager labDisplayParamterManager = new LabDisplayParamterManagerImpl();
	// 查找配置信息
	private LabModifyManager labModifyManager = new LabModifyManagerImpl();
	private static LabDisprobeNumberManager labDisprobeNumberManager = new LabDisprobeNumberManagerImpl();
	private static Map<String, LabDisprobeNumber> labDisprobeNumber = labDisprobeNumberManager.resultSetToListFromDisProbe();// 显示数据实例
	// 查找该探头的校准
	Map<String, LabModify> modifys = labModifyManager.resultSetToMapFromModify();

	private static int resetInit = 0;// 定时刷新预加载信�?
	private final double tempCheck = -25.00;// 设置25摄氏度为�?��采集温度
	private final double humCheck = -20.00;// 设置20度为�?��采集湿度

	String inputProbNum = "";// 板号+端口�?
	String createdOn = "";// 采集时间（单片机端）
	Double temperature;// 采集温度
	Double humidity;// 采集湿度
	//String displayTemprature = "";// 显示数据温度
	String displayProbNum = "";// 探头编号（客户定制）
	String displayTabName = "";// 显示表名
	String inputTabName = "";// 原数据表�?
	LabInputParamter labInputParamter;
	LabDisplayParamter labDisplayParamter;

	public Parse(BlockingQueue<String> reciverQueue, BlockingQueue<LabDisplayParamter> displayQueue,
			BlockingQueue<LabInputParamter> inputQueue) {
		this.reciverQueue = reciverQueue;
		this.displayQueue = displayQueue;
		this.inputQueue = inputQueue;
	}

	@Override
	public void run() {
		
		while (true) {
			try {
				if (reciverQueue.size() > 0) {				
					String[] paramterStr = SCMUtil.getArrayFromSCM(RegexUtil.getParams(reciverQueue.take()));// 将长数据按分号分割成数组
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
							labInputParamterManager.addLabInputParamter(labInputParamter);//
							// 组装显示显示数据对象
							labDisplayParamter = new LabDisplayParamter(inputProbNum, displayProbNum, createdOn,
									temperature, humidity, displayTabName);
							// AVG for Temperture 10sec
							labDisplayParamter.setDisTemperature(labInputParamterManager
									.getAVGInputTemperatureByCreatedOn(labInputParamter, inputTabName));
							// 校准值
							labDisplayParamterManager.calParamterByModify(labDisplayParamter, modifys);
							// 加入原始批量数据,表一
							inputQueue.add(labInputParamter);
							// 加入显示批量数据，表二
							displayQueue.add(labDisplayParamter);
						}
					}
					resetInit++;
					if (resetInit > 20) {
						init();
					}
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 初始化预加载信息
	 */
	private void init() {
		resetInit = 0;
		labDisprobeNumber = labDisprobeNumberManager.resultSetToListFromDisProbe();// 显示数据实例
		modifys = labModifyManager.resultSetToMapFromModify();
		System.out.println("初始化校正值数据。。。");
	}

}
