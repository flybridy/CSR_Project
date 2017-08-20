package com.labServer.manager;

import java.sql.ResultSet;
import java.util.List;
import java.util.Map;

import com.labServer.model.LabDisprobeNumber;

public interface LabDisprobeNumberManager {

	public Map<String, LabDisprobeNumber> resultSetToListFromDisProbe();
}
