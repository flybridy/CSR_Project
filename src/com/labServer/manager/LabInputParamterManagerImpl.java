package com.labServer.manager;

import java.util.List;
import com.labServer.dao.LabInputParamterDaoImpl;
import com.labServer.model.LabInputParamter;

public class LabInputParamterManagerImpl implements LabInputParamterManager {

	LabInputParamterDaoImpl labInputParamterDaoImpl = new LabInputParamterDaoImpl();

	public void addLabInputParamter(LabInputParamter labInputParamter) {
		labInputParamterDaoImpl.addLabInputParamter(labInputParamter);
	}

	public void addListItemsToSumInput(List<LabInputParamter> list) {
		labInputParamterDaoImpl.addListItemsToSumInput(list);
	}

	public Double getAVGInputTemperatureByCreatedOn(LabInputParamter labInputParamter, String inputTable) {
		Double avgInputTemperature = findAVGInputTemperature(labInputParamter, inputTable);
		avgInputTemperature = OptimizedTemp(labInputParamter.getInputTemperature(), avgInputTemperature);
		return avgInputTemperature;
	}

	/**
	 * 求平均
	 * 
	 * @param labInputParamter
	 * @param inputTable
	 * @return
	 */
	public Double findAVGInputTemperature(LabInputParamter labInputParamter, String inputTable) {
		return labInputParamterDaoImpl.findAVGInputTemperature(labInputParamter, inputTable);
	}

	public Double OptimizedTemp(Double temperature, Double avgTemperature) {
		if (avgTemperature != null) {
			return avgTemperature;
		}
		return temperature;
	}
}
