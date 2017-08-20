package com.csr.ana;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

import com.csr.client.TcpReciverFor18;
import com.fleety.server.BasicServer;
import com.fleety.util.pool.timer.FleetyTimerTask;
import com.fleety.util.pool.timer.TimerPool;
import com.labServer.model.LabDisplayParamter;
import com.labServer.model.LabInputParamter;

import server.threadgroup.ThreadPoolGroupServer;

public class TcpProReceiveServer extends BasicServer {



	public boolean startServer() {
     System.out.println("tcp数据服务");
		String log_dir = null;
		try {
			log_dir = this.getStringPara("log_dir");
			
			System.out.println(log_dir);

			ServerSocket socket;
			try {
				socket=new ServerSocket(8089);
				TcpReciverFor18 tcp=new TcpReciverFor18(socket);
				TimerPool pool = ThreadPoolGroupServer.getSingleInstance().createTimerPool("user_data_get", 50);
				pool.schedule(tcp, 0);	
				pool.schedule(tcp, 0);
				pool.schedule(tcp, 0);
			} catch (UnknownHostException e) {
			
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
			this.isRunning = true;		
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return this.isRunning();
	}

	public void stopServer() {
		super.stopServer();
	}

}
