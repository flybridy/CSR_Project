package com.fleety.server;

import org.quartz.*;
import org.quartz.impl.*;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

public class JobLoader
{
    private static DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
	private static DocumentBuilder db = null;
	private NodeList job_list = null;
	private static Scheduler scheduler = null;

	public JobLoader(String property_file_name) throws Exception
	{			
	    if(db == null)
			db = dbf.newDocumentBuilder();
		Document doc = db.parse(property_file_name);
		Element root = doc.getDocumentElement();		
		job_list = root.getElementsByTagName("job");
		getScheduler();
		scheduler.start();
	}
	
	public static Scheduler getScheduler() throws Exception
	{
	    if(scheduler==null)
	    {
	        StdSchedulerFactory schedFac = new StdSchedulerFactory();
	        scheduler = schedFac.getScheduler();		
	    }
	    return scheduler;
	}
	

	public void loadAllJobs() throws Exception
	{			
		for(int i=0;i<job_list.getLength();i++)
		{
		    Element e = (Element)job_list.item(i);
		    String sn = e.getAttribute("name");
			String sclass = e.getAttribute("class");
			String sgroup = e.getAttribute("group");
			if(sgroup==null||sgroup.equals("")) sgroup = Scheduler.DEFAULT_GROUP;
			String trigger_exp = e.getAttribute("trigger_exp");
			Class c = Class.forName(sclass);
			JobDetail jd = new JobDetail(sn,sgroup,c);
			CronTrigger ct = new CronTrigger(sn,sgroup,trigger_exp);
			scheduler.scheduleJob(jd,ct);
		}
	}
}