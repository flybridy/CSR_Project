package com.csr.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

import com.csr.ana.ParseAndSaveData;
import com.fleety.util.pool.timer.FleetyTimerTask;

/**
 * 
 */
public class TcpReciverFor18 extends FleetyTimerTask  {
	

	
	ServerSocket serverSocket  = null;

	public TcpReciverFor18(ServerSocket serverSocket) {
		this.serverSocket = serverSocket;
	}
public static void main(String[] args) {
		
		ServerSocket socket;
		try {
			socket=new ServerSocket(8089);
			TcpReciverFor18 tcp=new TcpReciverFor18(socket);
			tcp.run();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	@Override
	public void run() {
		try {				
			// 获取Socket的输出流，用来向客户端（单片机）发送数据
			System.out.println("run methods start!!!!");
			 Socket socket = serverSocket.accept(); 
			PrintStream out = new PrintStream(socket.getOutputStream());
			BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			boolean flag = true;
			while (flag) {				
				String messageInfo = in.readLine();
				System.out.println("receiver::"+messageInfo);
				ParseAndSaveData.parse(messageInfo);
			}
			out.write("true".getBytes());
			out.flush();
			out.close();
			socket.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	


}