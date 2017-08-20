package com.labServer.manager;

import java.util.List;

import com.labServer.model.LabInputParamter;

public interface LabInputParamterManager {

	void addLabInputParamter(LabInputParamter labInputParamter);

	void addListItemsToSumInput( List<LabInputParamter> list);

	Double getAVGInputTemperatureByCreatedOn(LabInputParamter labInputParamter, String inputTable);

	Double OptimizedTemp(Double temperature, Double avgTemperature);
}
