package com.fleety.analysis.feedback;

import java.util.Date;

import com.fleety.analysis.AnalysisServer;

public interface FeedBackAnalysis {
	public boolean startAnalysisFeedBack(AnalysisServer parentServer,Date sDate,Date eDate);
	public void analysisDestFeedBack(AnalysisServer parentServer,Date sDate,Date eDate);
	public void endAnalysisFeedBack(AnalysisServer parentServer,Date sDate,Date eDate);
}
