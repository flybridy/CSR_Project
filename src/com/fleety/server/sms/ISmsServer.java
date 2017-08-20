package com.fleety.server.sms;


public interface ISmsServer{
	public void setSendListener(ISmsSendListener listener);
	
	public boolean sendSms(String seq,String tel,String content);
	public boolean canSend(String tel);
}
