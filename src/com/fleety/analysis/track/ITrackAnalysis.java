package com.fleety.analysis.track;

import com.fleety.analysis.AnalysisServer;
import com.fleety.base.InfoContainer;

public interface ITrackAnalysis {
	public static final Object STAT_START_TIME_DATE = new Object();
	public static final Object STAT_END_TIME_DATE = new Object();
	public static final Object STAT_DEST_NUM_INTEGER = new Object();
	
	public boolean startAnalysisTrack(AnalysisServer parentServer,InfoContainer statInfo);
	public void analysisDestTrack(AnalysisServer parentServer,TrackInfo trackInfo);
	public void endAnalysisTrack(AnalysisServer parentServer,InfoContainer statInfo);
}
