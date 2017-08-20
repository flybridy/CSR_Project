package com.csr.client;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ClientWithThreads {

	public static void main(String[] args) {
		String msg1 = "+YAV:0005AABB,000 000 000 007 001 ,000 000 000 007 001 ,007 001 007 000 000 ,009 001 008 000 000 ,000 000 004 000 000 ,004 000 008 001 003 ,001 005 004 000 002 ,008 00C 00B 008 008 ,0 0,0 0,0 0 0 0,00,FF0203FF,V V V V V V V V,8AD00001,X,EEFF";
		String msg2 = "+YAV:0005AABB,000 000 000 007 001 ,000 000 000 007 001 ,007 001 007 000 000 ,009 001 008 000 000 ,000 000 004 000 000 ,004 000 008 001 003 ,001 005 004 000 002 ,008 00C 00B 008 008 ,0 0,0 0,0 0 0 0,00,FF0203FF,V V V V V V V V,8AD00002,X,EEFF";
		String msg3 = "+YAV:0005AABB,000 000 000 007 001 ,000 000 000 007 001 ,007 001 007 000 000 ,009 001 008 000 000 ,000 000 004 000 000 ,004 000 008 001 003 ,001 005 004 000 002 ,008 00C 00B 008 008 ,0 0,0 0,0 0 0 0,00,FF0203FF,V V V V V V V V,8AD00003,X,EEFF";
		String msg4 = "+YAV:0005AABB,000 000 000 007 001 ,000 000 000 007 001 ,007 001 007 000 000 ,009 001 008 000 000 ,000 000 004 000 000 ,004 000 008 001 003 ,001 005 004 000 002 ,008 00C 00B 008 008 ,0 0,0 0,0 0 0 0,00,FF0203FF,V V V V V V V V,8AD00004,X,EEFF";
		String msg5 = "+YAV:0005AABB,000 000 000 007 001 ,000 000 000 007 001 ,007 001 007 000 000 ,009 001 008 000 000 ,000 000 004 000 000 ,004 000 008 001 003 ,001 005 004 000 002 ,008 00C 00B 008 008 ,0 0,0 0,0 0 0 0,00,FF0203FF,V V V V V V V V,8AD00005,X,EEFF";

		
		UdpClient uc1 = new UdpClient(msg1);
		//UdpClient uc2 = new UdpClient(msg2);
		//UdpClient uc3 = new UdpClient(msg3);
		//UdpClient uc4 = new UdpClient(msg4);
		//UdpClient uc5 = new UdpClient(msg5);
		//UdpClient uc6 = new UdpClient(msg5);
		
		ExecutorService service = Executors.newCachedThreadPool();
		// 启动接收线程
		service.execute(uc1);
		//service.execute(uc2);
		//service.execute(uc3);
		//service.execute(uc4);
		//service.execute(uc5);
		// service.execute(uc6);

	}

}
