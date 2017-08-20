package com.fleety.server.area;

import java.awt.Point;
import java.awt.Polygon;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;

import server.threadgroup.ThreadPoolGroupServer;

import com.fleety.server.BasicServer;
import com.fleety.util.pool.timer.FleetyTimerTask;

public class AreaLoadServer extends BasicServer{
	public static AreaLoadServer areaLoadServer = new AreaLoadServer();
	public static HashMap<String, Polygon> polygonMap = null;
	public static HashMap<String, Long> lastTimeMap = null;
	private String timeName = "sz_area_load_pool";
	private long dura = 30*60*1000;
	public List<AreaFile> arealist;

	public boolean startServer() {
		if(this.getStringPara("dura")!=null){
			dura = Integer.valueOf(this.getStringPara("dura"))*60*1000;
		}
		arealist = new ArrayList<AreaFile>();
		AreaFile areaFile = null;
		String temp = this.getStringPara("area");
		if(temp!=null&&!temp.equals("")){
			String[] tempArr = temp.split(";");
			for (int i = 0; i < tempArr.length; i++) {
				String[] areaArr = tempArr[i].split(",");
				areaFile = new AreaFile();
				areaFile.areaName = areaArr[0];
				areaFile.fileUrl = areaArr[1];
				arealist.add(areaFile);
			}
		}else{
			System.out.println("区域文件未配置！");
			return false;
		}
		try {
			ThreadPoolGroupServer.getSingleInstance().createTimerPool(timeName).schedule(this.task, 0, dura);
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	private AreaLoadServer(){
	}
	
	public static AreaLoadServer getSingleInstance(){
		return areaLoadServer;
	}
	public HashMap<String, Polygon> loadPolygonMap(){
		if(lastTimeMap==null){
			lastTimeMap = new HashMap<String, Long>();
		}
		HashMap<String, Polygon> pMap = new HashMap<String, Polygon>();
		try {
			if(arealist!=null){
				Polygon polygon = null;
				for (int i = 0; i < arealist.size(); i++) {
					AreaFile areaFile = arealist.get(i);
					String path = new File(areaFile.fileUrl).getAbsolutePath();
					File file = new File(path);
					long lastTime = 0;
					if(lastTimeMap.containsKey(areaFile.areaName)){
						lastTime = lastTimeMap.get(areaFile.areaName);
					}
					if(lastTime==file.lastModified()){
						pMap = polygonMap;
						continue;
					}
					BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"));
					String line = null;
					String templo = null;//经度
					String templa = null;//纬度
					polygon = new Polygon();
					while ((line=reader.readLine())!=null) {
						String[] lola = line.split(" ");
						templo = lola[0];
						templa = lola[1];
						if(templa!=null&&!templa.equals("")&&templo!=null&&!templo.equals("")){
							int lo = (int) (Double.valueOf(templo)*10000000);
							int la = (int) (Double.valueOf(templa)*10000000);
							polygon.addPoint(lo, la);
						}
					}
					pMap.put(areaFile.areaName, polygon);
					lastTimeMap.put(areaFile.areaName, file.lastModified());
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return pMap;
	}
	private FleetyTimerTask task = new FleetyTimerTask(){
		public void run(){
			polygonMap = AreaLoadServer.getSingleInstance().loadPolygonMap();
		};
	};
	
	public void stopServer() {
		if(this.task!=null){
			this.task.cancel();
		}
		this.task = null;
		super.stopServer();
	}
	
	private class AreaFile{
		public String areaName;
		public String fileUrl;
	}
	public static void main(String[] args) {
		
		try {
			Polygon pl = new Polygon();
			int lo1 = 114063*10000;
			int la1 = 22640*10000;
			String path = "D:\\area\\shenzhend.mif";
			File file = new File(path);
			BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"));
			String line = null;
			String templo = null;//经度
			String templa = null;//纬度
			while ((line=reader.readLine())!=null) {
				String[] lola = line.split(" ");
				templa = lola[1];
				templo = lola[0];
				if(templa!=null&&!templa.equals("")&&templo!=null&&!templo.equals("")){
					int lo = (int) (Double.valueOf(templo)*10000000);
					int la = (int) (Double.valueOf(templa)*10000000);
					System.out.println(lo+"   "+la);
					pl.addPoint(lo, la);
				}
			}
			System.out.println(pl.contains(lo1,la1)+"  "+pl.contains(new Point(lo1,la1))+"  "+file.lastModified());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
