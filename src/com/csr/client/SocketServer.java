package com.csr.client;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
/* 
* �򵥵�socket����ˣ�ʵ�ֺͿͻ��˵���ݽ��� 
*/  
public class SocketServer {  
    public static final int PORT = 8222;//�˿�  
  
    
	public static void start() throws IOException {        
    	ServerSocket serverSocket = new ServerSocket(PORT);    
    	while(true){
        Socket s = serverSocket.accept();  
        System.out.println("");        
        DataInputStream dis = new DataInputStream(new BufferedInputStream(s  
                .getInputStream()));  
        byte[] res=new byte[1024];
        int len=dis.read(res);
         
        StringBuffer sb = new StringBuffer();
		for (int i = 0; i <len; i++) {
			sb.append((res[i]&0xff)+" ");
		}
		
		System.out.println("接收到数据"+sb.toString());
		if(len>0){
			OutputStream ps = s.getOutputStream();  
	        ps.write("true".getBytes());
	        ps.flush();
	        ps.close();  
		}
		len=0;
          
        
       
        dis.close();  
      
       
    	}
  
    }  
  
    public static void main(String[] arge) throws UnknownHostException,  
            IOException {  
    		SocketServer.start();         
    }   
}  
