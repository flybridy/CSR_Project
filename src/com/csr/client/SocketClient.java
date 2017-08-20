package com.csr.client;

import java.io.BufferedInputStream;  
import java.io.DataInputStream;  
import java.io.DataOutputStream;  
import java.io.IOException;  
import java.net.Socket;  
import java.net.UnknownHostException;  
/* 
* �򵥵�socket�ͻ��ˣ�ʵ�ֺͷ���˵���ݽ��� 
*/  
public class SocketClient {  
    public static final String IP = "127.0.0.1";//�����ip��ַ  
    public static final int PORT = 8222;//����˼���˿�  
  
    public static void start() throws UnknownHostException, IOException {  
        //����socket����  
        Socket s = new Socket(IP, PORT);  
        String clientMessage = "clien 数据 date";  
        System.out.println("Socket 发送");  
        //�����˷�����Ϣ  
        DataOutputStream ps = new DataOutputStream(s.getOutputStream());  
        ps.writeUTF(clientMessage);  
        ps.flush();  
        //���շ���˵���Ϣ  
        DataInputStream dis = new DataInputStream(new BufferedInputStream(s  
                .getInputStream()));  
        System.out.println("server return data:" + dis.readUTF());  
        dis.close();  
        ps.close();  
    }  
  
    public static void main(String[] arge) throws UnknownHostException,  
            IOException {  
        SocketClient.start();  
    }  
}  