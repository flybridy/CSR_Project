package com.fleety.server.sms;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

import org.w3c.dom.Element;

import server.socket.help.UTFCmdReader;
import server.socket.inter.CmdInfo;
import server.socket.inter.ConnectSocketInfo;
import server.socket.inter.ICmdReleaser;
import server.socket.socket.FleetySocket;
import server.threadgroup.ThreadPoolGroupServer;

import com.fleety.base.Util;
import com.fleety.base.xml.XmlParser;


public class SmsServer extends FleetySocket implements ISmsServer{
	private static final String SEND_SMS_MSG = "1";
	private static final String SUCCESS_FAILURE_MSG = "1";
	private static final String SUCCESS_FLAG = "0";
	private static final String FAILURE_FLAG = "1";
	
	private Properties scopeMapping = new Properties();
	private String scopeGetUrl = null;
	private String okScope = null;
	private String telMappingFileName = "mapping/telScope.map";
	
	private String sendSourceFlag = null;
	
	private static SmsServer singleInstance = null;
	
	public static SmsServer getSingleInstance(){
		if(singleInstance == null){
			singleInstance = new SmsServer();
		}
		return singleInstance;
	}
	
	public boolean startServer(){
		this.addPara(CMD_READER_FLAG, UTFCmdReader.class.getName());
		this.addPara(CMD_RELEASER_FLAG, SmsResponseReleaser.class.getName());
		
		this.scopeGetUrl = this.getStringPara("telScopeUrl");
		if(this.scopeGetUrl != null){
			if(this.scopeGetUrl.trim().length() == 0){
				this.scopeGetUrl = null;
			}
		}
		this.okScope = this.getStringPara("okScope");
		if(this.okScope != null){
			if(this.okScope.trim().length() == 0){
				this.okScope = null;
			}
		}
		this.sendSourceFlag = this.getStringPara("source_flag");
		if(this.sendSourceFlag != null && this.sendSourceFlag.trim().length() == 0){
			this.sendSourceFlag = null;
		}
		
		this.isRunning = super.startServer();
		
		byte[] data = Util.loadFileWithSecurity(telMappingFileName);
		if(data != null){
			ByteArrayInputStream in = new ByteArrayInputStream(data);
			try{
				this.scopeMapping.load(in);
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		Timer timer = ThreadPoolGroupServer.getSingleInstance().createTimer("save_timer");
		timer.schedule(new TimerTask(){
			public void run(){
				try{
					ByteArrayOutputStream out = new ByteArrayOutputStream(1024*1024);
					scopeMapping.store(out, "tel->scope mapping");
					
					Util.saveFileWithSecurity(out.toByteArray(), telMappingFileName);
				}catch(Exception e){
					e.printStackTrace();
				}
			}
		}, 300000, 300000);
		
		
		return this.isRunning();
	}
	
	protected ISmsSendListener listener = null;
	public void setSendListener(ISmsSendListener listener){
		this.listener = listener;
	}
	
	/**
	 * 只接受手机号码的。判断法则：长度不能小于11,倒序第11位必需是1.
	 */
	public boolean canSend(String tel) {
		int len = tel.length();
		if(len >= 11){
			tel = tel.substring(len-11);
		}else{
			return false;
		}
		if(tel.startsWith("1")){
			return this.validScope(tel);
		}else {
			return false;
		}
	}
	
	private boolean validScope(String tel){
		if(this.okScope == null){
			return true;
		}
		
		String str = (String)this.scopeMapping.getProperty(tel);
		if(str == null){
			InputStream in = null;
			try{
				System.out.println("Start Query Tel Scope:"+tel);
				URL url = new URL(this.scopeGetUrl+tel);
				URLConnection conn = url.openConnection();
				conn.setConnectTimeout(2000);
				conn.setReadTimeout(3000);
				Element root = XmlParser.parse(new BufferedInputStream(in = conn.getInputStream()));
				str = Util.getNodeText(Util.getSingleElementByTagName(root, "Province"));

				System.out.println("End Query Tel Scope:"+tel+"="+str);
				this.scopeMapping.setProperty(tel, str);
			}catch(Exception e){
				e.printStackTrace();
			}finally{
				try{
					if(in != null){
						in.close();
					}
				}catch(Exception e){}
			}
		}
		
		if(str == null){
			return false;
		}else{
			return this.okScope.contains(str);
		}
	}

	public boolean sendSms(String seq, String tel, String content) {
		try{
			String text = SEND_SMS_MSG+" "+seq+" "+tel+" "+URLEncoder.encode(content, "GBK");
			if(this.sendSourceFlag != null){
				text = text + " " + URLEncoder.encode(this.sendSourceFlag, "GBK");
			}
			System.out.println("Sms:"+text);
			
			ByteArrayOutputStream out = new ByteArrayOutputStream(1024);
			DataOutputStream utfOut = new DataOutputStream(out);
			utfOut.writeUTF(text);
			byte[] sendArr = out.toByteArray();
			
			this.sendData(sendArr, 0, sendArr.length);
			return true;
		}catch(Exception e){
			e.printStackTrace();
			return false;
		}
	}
	
	private void disposeSmsResponse(String[] infoArr){
		if(infoArr == null || infoArr.length < 4){
			return ;
		}
		String seq = infoArr[1];
		String resultFlag = infoArr[2];
		String telListStr = infoArr[3];
		
		ISmsSendListener temp = this.listener;
		if(temp != null){
			String[] telArr = telListStr.split(",");
			String[] resultArr = new String[telArr.length];
			if(resultFlag.endsWith(SUCCESS_FLAG) && infoArr.length > 4){
				resultArr = infoArr[4].split(",");
			}
			for(int i=0;i<telArr.length;i++){
				temp.smsSendResponse(seq, telArr[i], resultArr[i]);
			}
		}
	}

	public static class SmsResponseReleaser implements ICmdReleaser {
		private SmsServer server = null;
		public void init(Object server) {
			this.server = (SmsServer)server;
		}
		
		public void releaseCmd(CmdInfo info){
			ConnectSocketInfo conn = (ConnectSocketInfo)info.getInfo(CmdInfo.SOCKET_FLAG);
			
			Object cmd = info.getInfo(CmdInfo.CMD_FLAG);
			if(cmd == CmdInfo.SOCKET_CONNECT_CMD){
				System.out.println(cmd+" "+conn.getRemoteSocketAddress());
			}else if(cmd == CmdInfo.SOCKET_DISCONNECT_CMD){
				System.out.println(cmd+" "+conn.getRemoteSocketAddress());
			}else{
				String infoStr = (String)info.getString(CmdInfo.DATA_FLAG);
				System.out.println(infoStr);
				
				String[] arr = infoStr.split(" ");
				String cmdStr = arr[0];
				
				if(cmdStr.equals(SUCCESS_FAILURE_MSG)){
					this.server.disposeSmsResponse(arr);
				}
			}
			
		}

	}
	
	public static void main(String[] argv){
		try{
			Properties scopeMapping = new Properties();
			scopeMapping.put("aa", "无锡");
			ByteArrayOutputStream out = new ByteArrayOutputStream(1024*1024);
			scopeMapping.store(out, "tel->scope mapping");
					
			Util.saveFileWithSecurity(out.toByteArray(), "test.mapping");
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}
