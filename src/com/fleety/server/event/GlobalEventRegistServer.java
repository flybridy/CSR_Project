/**
 * ���� Created on 2008-12-5 by edmund
 */
package com.fleety.server.event;

import java.io.File;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import com.fleety.base.Util;
import com.fleety.base.event.IEventListener;
import com.fleety.server.BasicServer;

public class GlobalEventRegistServer extends BasicServer{
	private static final String EVENT_NODE_NAME = "event";
	private static final String ACTION_NODE_NAME = "action";

	public static final String CFG_PATH_KEY = "action_path";
	

	private File cfgFile = null;
	public boolean startServer(){
		String cfgPath = (String)this.getPara(CFG_PATH_KEY);
		if(cfgPath == null){
			System.out.println("����������ļ�·��!");
			return false;
		}
		this.cfgFile = new File(cfgPath);
		if(!this.cfgFile.exists()){
			System.out.println("����������ļ�·��!");
			return false;
		}
		
		boolean isSuccess =  this.loadFromXml();
		
		this.isRunning = isSuccess;
		
		//��Ҫ��ص������loadFromXml���Ա��ֶ�̬���ء�
		GlobalEventCenter.getSingleInstance().setServer(this);
		
		return isSuccess;
	}
	
	private long lastLoadTime = 0;
	public boolean loadFromXml(){
		if(!cfgFile.exists()){
			return true;
		}
		if(cfgFile.lastModified() == this.lastLoadTime){
			return true;
		}
		this.lastLoadTime = cfgFile.lastModified();
		
		HashMap newMapping = new HashMap();
		try{
			//���������ļ�
			DocumentBuilderFactory domfac=DocumentBuilderFactory.newInstance();
			DocumentBuilder builder = domfac.newDocumentBuilder();
			Document document = builder.parse(cfgFile);
			
			Element root = document.getDocumentElement();
			NodeList allMsgNodeList = root.getElementsByTagName(EVENT_NODE_NAME);
			int msgNum = allMsgNodeList.getLength();
			Element eventNode;
			String eventName;
			Integer eventId;
			List eventList = null;
			for(int i=0;i<msgNum;i++){
				eventNode = (Element)allMsgNodeList.item(i);
	
				eventName = Util.getNodeAttr(eventNode, "name");
				if(eventName == null || eventName.trim().length()==0){
					continue;
				}
				eventName = eventName.trim();
				
				try{
					eventId = new Integer(eventName);
				}catch(Exception e){
					e.printStackTrace();
					System.out.println("����:���������¼���!");
					continue;
				}
				
				eventList = (List)newMapping.get(eventId);
				if(eventList == null){
					eventList = new LinkedList();
					newMapping.put(eventId, eventList);
				}
				this.loadActionFromXml(Util.getElementsByTagName(eventNode, ACTION_NODE_NAME),eventList);
			}
			
			GlobalEventCenter.getSingleInstance().updateAllEventListener(newMapping);
		}catch(Exception e){
			e.printStackTrace();
			System.out.println("�����¼�ӳ����Ϊ["+this.getServerName()+"]�������ļ�["+this.getPara(CFG_PATH_KEY)+"]ʧ��!");
			return false;
		}
		System.out.println("�����¼�ӳ����Ϊ["+this.getServerName()+"]�������ļ�["+this.getPara(CFG_PATH_KEY)+"]�ɹ�!");
		
		return true;
	}
	
	private void loadActionFromXml(Node[] actionNodeArr,List actionList) throws Exception{
		int num = actionNodeArr.length;
		Node actionNode ;
		String name,className;
		IEventListener listener;
		String enable;
		for(int i=0;i<num;i++){
			actionNode = actionNodeArr[i];
			name= Util.getNodeAttr(actionNode, "name");
			
			className = Util.getNodeText(Util.getSingleElementByTagName(actionNode, "class_name"));
			enable = Util.getNodeText(Util.getSingleElementByTagName(actionNode, "enable"));
			if(enable != null  && enable.equalsIgnoreCase("false")){
				System.out.println("�¼���Ϊ��"+name+"��������!");
				continue;
			}
			String createMethod = Util.getNodeText(Util.getSingleElementByTagName(actionNode, "create_method"));
			try{
				Class cls = Class.forName(className);
				if(createMethod == null || createMethod.trim().length() == 0){
					listener = (IEventListener)cls.newInstance();
				}else{
					Method method = cls.getMethod(createMethod, new Class[0]);
					listener = (IEventListener)method.invoke(null, new Object[0]);
				}
			}catch(Exception e){
				System.out.println("��("+className+")ʵ�����쳣:"+e);
				continue;
			}
			
			listener.setPara(IEventListener._NAME_FLAG, name);
			
			Node[] paraArr = Util.getElementsByTagName(actionNode, "para");
			Node paraNode;
			int paraNum = paraArr.length;
			for(int j=0;j<paraNum;j++){
				paraNode = paraArr[j];
				listener.setPara(Util.getNodeAttr(paraNode, "key"), Util.getNodeAttr(paraNode, "value"));
			}
			listener.setPara(IEventListener._ACTION_CONTAINER_FLAG, this);
	
			try{
				listener.init();
			}catch(Exception e){
				e.printStackTrace();
				System.out.println("�¼���Ϊ�ĳ�ʼ�����������쳣��������ӳ��!");
				continue;
			}
	
			actionList.add(listener);
		}
	}

	public void stopServer(){
		this.isRunning = false;
	}
}
