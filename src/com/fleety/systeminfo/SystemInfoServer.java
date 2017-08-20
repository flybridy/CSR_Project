/**
 * ¾øÃÜ Created on 2008-6-10 by edmund
 */
package com.fleety.systeminfo;
import com.fleety.server.BasicServer;
public class SystemInfoServer extends BasicServer
{
	public boolean startServer(){
		System.out.println("system version: "+SystemInfoParam.VERSION);
		System.out.println("cvs tag:"+SystemInfoParam.TAG_INFO);		
		
		return true;
	}

	public void stopServer(){
		
	}	
}
