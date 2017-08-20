package com.fleety.analysis.track.task.overspeed_area;

import java.awt.Polygon;
import java.util.ArrayList;
import java.util.HashMap;

public class OverSpeedAreaInfo 
{
	private int areaId;
	private String cname;
	private int pointsNum = 0;
	protected ArrayList<Double> los = new ArrayList<Double>();
	protected ArrayList<Double> las = new ArrayList<Double>();
	private int type;
	
	public HashMap statMap=new HashMap();
	public final static double delta = 1E7;
	private Polygon polygon;
	
	public int getAreaId() {
		return areaId;
	}
	public void setAreaId(int areaId) {
		this.areaId = areaId;
	}
	public String getCname() {
		return cname;
	}
	public void setCname(String cname) {
		this.cname = cname;
	}
	public int getPointsNum() {
		return pointsNum;
	}
	public void setPointsNum(int pointsNum) {
		this.pointsNum = pointsNum;
	}
	public int getType() {
		return type;
	}
	public void setType(int type) {
		this.type = type;
	}
	
	public void initPolygon(){
		int npoints = los.size();
		polygon = new Polygon();
		for (int i = 0; i < npoints; i++) {
			polygon.addPoint((int) (los.get(i) * delta),
					(int) (las.get(i) * delta));
		}
	}
	
	public boolean isInArea(double lo, double la) {
		return polygon.contains(lo * delta, la * delta);
	}
}
