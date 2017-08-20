package com.csr.ana;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

import com.fleety.server.BasicServer;
import com.fleety.util.pool.timer.FleetyTimerTask;
import com.fleety.util.pool.timer.TimerPool;
import com.labServer.model.LabDisplayParamter;
import com.labServer.model.LabInputParamter;

import server.threadgroup.ThreadPoolGroupServer;

public class ProReceiveServer extends BasicServer {
	private long count = 0;

	BlockingQueue<String> reciverQueue = new LinkedBlockingDeque<>();
	BlockingQueue<LabDisplayParamter> displayQueue = new LinkedBlockingQueue<>();
	BlockingQueue<LabInputParamter> inputQueue = new LinkedBlockingQueue<>();

	// �˿ں�808��IP��ַĬ��Ϊ����127.0.0.1
	DatagramPacket packet = null;

	public boolean startServer() {

		String log_dir = null;
		try {
			log_dir = this.getStringPara("log_dir");
			DatagramSocket socket = new DatagramSocket(808);//
			System.out.println(log_dir);

			UdpReciverFor400 reciver1 = new UdpReciverFor400(reciverQueue, socket, packet);	
			TimerPool pool = ThreadPoolGroupServer.getSingleInstance().createTimerPool("user_data_get", 5);
			pool.schedule(reciver1, 0);
			
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
