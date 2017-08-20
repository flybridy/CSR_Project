package com.labServer.dao;

import java.util.List;

import com.labServer.model.LabDisplayParamter;

public interface LabDisplayParamterDao {

	void addListItemsToSumDisplay(List<LabDisplayParamter> list);

	void addListItemsToDiffDisplay(List<LabDisplayParamter> list);
}
