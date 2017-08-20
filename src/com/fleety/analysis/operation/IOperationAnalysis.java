package com.fleety.analysis.operation;

import com.fleety.analysis.AnalysisServer;
import com.fleety.base.InfoContainer;

public interface IOperationAnalysis {
	public static final Object STAT_START_TIME_DATE = new Object();
	public static final Object STAT_END_TIME_DATE = new Object();
	public static final Object STAT_DEST_NUM_INTEGER = new Object();
	
	public boolean startAnalysisOperation(AnalysisServer parentServer,InfoContainer statInfo);
	public void analysisDestOperation(AnalysisServer parentServer,InfoContainer statInfo);
	public void endAnalysisOperation(AnalysisServer parentServer,InfoContainer statInfo);
}
