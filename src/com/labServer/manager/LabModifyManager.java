package com.labServer.manager;

import java.util.Map;

import com.labServer.model.LabModify;

public interface LabModifyManager {

	Map<String, LabModify> resultSetToMapFromModify();
}
