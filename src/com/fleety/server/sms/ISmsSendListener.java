package com.fleety.server.sms;

public interface ISmsSendListener {
	public void smsSendResponse(String seq,String tel,String result);
}
