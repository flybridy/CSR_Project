package com.csr.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;

public class TcpClient implements Runnable {

	String msg = "";
	Socket socket = null;

	OutputStream socketOut = null;
	BufferedReader br = null;

	public TcpClient(String msg) {
		this.msg = msg;
	}
public static void main(String[] args) {
	String msg1 = "+YAV:0005AABB,000 000 000 007 001 ,000 000 000 007 001 ,007 001 007 000 000 ,009 001 008 000 000 ,000 000 004 000 000 ,004 000 008 001 003 ,001 005 004 000 002 ,008 00C 00B 008 008 ,0 0,0 0,0 0 0 0,00,FF0203FF,V V V V V V V V,8AD00001,X,EEFF \n";
	TcpClient cl=new TcpClient(msg1);
	cl.run();
}
	@Override
	public void run() {
		try {
			while (true) {
				socket = new Socket("localhost", 8089);
				// 发送消息
				System.out.println("send : " + msg);
				socketOut = socket.getOutputStream();
				socketOut.write(msg.getBytes());
				socketOut.flush();
				// 接收服务器的反馈
				br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				String res = br.readLine();
				if (res != null) {
					System.out.println("get message	:" + res);
				}
				Thread.sleep(1000);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

}
