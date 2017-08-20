/**
 * 绝密 Created on 2008-12-5 by edmund
 */
package com.fleety.server.event;

import java.util.Timer;
import java.util.TimerTask;

import server.threadgroup.PoolInfo;
import server.threadgroup.ThreadPoolGroupServer;

import com.fleety.base.StrFilter;
import com.fleety.base.event.Event;
import com.fleety.base.event.EventRegister;
import com.fleety.base.event.IEventListener;
import com.fleety.util.pool.thread.ThreadPool;

public class GlobalEventCenter extends EventRegister
{
    private static int               MAX_EVENT_NUM         = 10000;
    private static GlobalEventCenter singleInstance        = null;

    private ThreadPool               globalEventThreadPool = null;
    
    
    //车辆营运数据统计完成时间
    public static final int CAR_BUSINESS_STAT_FINISH = 1;

    public static GlobalEventCenter getSingleInstance()
    {
        if (singleInstance == null)
        {
            synchronized (GlobalEventCenter.class)
            {
                if (singleInstance == null)
                {
                    singleInstance = new GlobalEventCenter();
                }
            }
        }
        return singleInstance;
    }



    private EventRegister   privateEventRegister         = new EventRegister(
                                                                 MAX_EVENT_NUM);

    private GlobalEventCenter()
    {
        super(MAX_EVENT_NUM);
        PoolInfo poolInfo = new PoolInfo(ThreadPool.SINGLE_TASK_LIST_POOL,5,1000,true);
        try
        {
            this.globalEventThreadPool = ThreadPoolGroupServer
                    .getSingleInstance().createThreadPool("eventThreadPool",
                            poolInfo);
        }
        catch (Exception e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        new Timer().schedule(new TimerTask() {
            public void run()
            {
                StringBuffer buff = new StringBuffer(64);
                int num = GlobalEventCenter.this.privateEventRegister
                        .getEventNum(0);
                if(num > 10){
	                buff.append("系统队列消息数：");
	                buff.append(num);
                }
                num = GlobalEventCenter.this.getEventNum(0);
                if(num > 10){
	                buff.append(" ;配置队列消息数:");
	                buff.append(num);
                }
                if(buff.length() > 0){
                	System.out.println(buff);
                }
            }
        }, 600000, 300000);
    }

    public ThreadPool getGlobalThreadPool()
    {
        return this.globalEventThreadPool;
    }
    private GlobalEventRegistServer server = null;

    public void setServer(GlobalEventRegistServer server)
    {
        this.server = server;
    }

    public void addSystemEventListener(int type, IEventListener listener)
    {
        this.privateEventRegister.addEventListener(type, listener);
    }

    public void removeSystemEventListener(int type, IEventListener listener)
    {
        this.privateEventRegister.removeEventListener(type, listener);
    }

    public void dispatchEvent(Event e)
    {
        if (this.server != null)
        {
            this.server.loadFromXml();
        }

        super.dispatchEvent(e);
        this.privateEventRegister.dispatchEvent(e);
    }
}
