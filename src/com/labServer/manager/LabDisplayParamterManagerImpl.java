package com.labServer.manager;

import java.util.List;
import java.util.Map;

import com.labServer.dao.LabDisplayParamterDaoImpl;
import com.labServer.model.LabDisplayParamter;
import com.labServer.model.LabModify;


public class LabDisplayParamterManagerImpl implements LabDisplayParamterManager {
	LabDisplayParamterDaoImpl labDisplayParamterDaoImpl=new LabDisplayParamterDaoImpl();

	/**
	 * 传入显示探头参数对象、校准值，并计算。
	 * 
	 * @param labDisplayParamter
	 * @param modify
	 * @return
	 */
	public LabDisplayParamter calParamterByModify(LabDisplayParamter labDisplayParamter,
			Map<String, LabModify> modifys) {
		// 遍历校准 并对温湿进行赋值（暂无光照）
		String inputNum = labDisplayParamter.getInputProbeNumber();
		labDisplayParamter
				.setDisTemperature(modifys.get(inputNum).getModifyTemp() + labDisplayParamter.getDisTemperature());
		labDisplayParamter.setDisHumidity(modifys.get(inputNum).getModifyHum() + labDisplayParamter.getDisHumidity());
		return labDisplayParamter;

	}

	/**
	 * 插入显示数据汇总表
	 * 
	 */
	public void addListItemsToSumDisplay(List<LabDisplayParamter> list) {
		labDisplayParamterDaoImpl.addListItemsToSumDisplay(list);
	}

	/**
	 * 插入显示数据分表
	 * 
	 * 
	 */
	public void addListItemsToDiffDisplay(List<LabDisplayParamter> list) {
		labDisplayParamterDaoImpl.addListItemsToDiffDisplay(list);
	}

}
