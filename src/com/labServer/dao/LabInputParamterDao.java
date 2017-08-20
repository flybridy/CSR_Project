package com.labServer.dao;

import java.util.List;

import com.labServer.model.LabInputParamter;

public interface LabInputParamterDao {

	void addLabInputParamter(LabInputParamter labInputParamter);

	Double findAVGInputTemperature(LabInputParamter labInputParamter, String inputTable);

	void addListItemsToSumInput(List<LabInputParamter> list);
	
}
