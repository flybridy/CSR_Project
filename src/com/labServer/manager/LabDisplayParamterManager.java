package com.labServer.manager;

import java.util.List;
import java.util.Map;


import com.labServer.model.LabDisplayParamter;
import com.labServer.model.LabModify;

public interface LabDisplayParamterManager {

	// void addLabDiaplayParamter(LabDisplayParamter labDisplayParamter,
	// List<Map<String, Double>> modify);
	//
	// void addLabDiaplayParamter(LabDisplayParamter labDisplayParamter,
	// List<Map<String, Double>> modifys, String displayTable);

	void addListItemsToSumDisplay( List<LabDisplayParamter> list);

	void addListItemsToDiffDisplay(List<LabDisplayParamter> list);

	LabDisplayParamter calParamterByModify(LabDisplayParamter labDisplayParamter, Map<String, LabModify> modifys);

}
