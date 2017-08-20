package com.fleety.server.area;

import java.awt.Polygon;
import java.awt.Shape;
import java.awt.geom.Ellipse2D;
import java.awt.geom.Rectangle2D;
import java.util.HashMap;

public class AreaInfo{
    private Shape areaShape = null;
    private int areaId = 0;
    private String cname = null;
    private int areaType=0;
    private HashMap<Integer,PlanInfo> pInfos;
    
    public AreaInfo(int areaId, String cname, int areaType) {
		this.areaId = areaId;
		this.cname = cname;
		this.areaType = areaType;
	}

	public Shape getAreaShape() {
		return areaShape;
	}


	public void setAreaShape(Polygon areaShape) {
		this.areaShape = areaShape;
	}


	public HashMap<Integer,PlanInfo> getpInfos() {
		return pInfos;
	}


	public void setpInfos(HashMap<Integer,PlanInfo> pInfos) {
		this.pInfos = pInfos;
	}


	public void setAreaId(int areaId) {
		this.areaId = areaId;
	}


	public int getAreaId(){
        return this.areaId;
    }
    /**
     * 添加多边形区域点
     * @param lo 为整型数据
     * @param la
     */
    public void addPointPolygon(int lo,int la){
        if(this.areaShape == null){
            this.areaShape = new Polygon();
        }
        Polygon polygon = (Polygon)this.areaShape;
        polygon.addPoint(lo, la);
        this.areaShape = polygon;
    }
    /**
     * 添加椭圆型区域
     * @param lo 为double数据 例：141.2310
     * @param la
     */
    public void addPointEllipse(double lo,double la){
        if(this.areaShape == null){
            this.areaShape = new Ellipse2D.Double(lo, la, 0, 0);
            return;
        }
        Ellipse2D.Double ellipse = (Ellipse2D.Double)this.areaShape;
        double x = ellipse.getX();
        double y = ellipse.getY();
        double width = x - lo;
        double height = y - la;
        ellipse.setFrame(Math.min(lo, x), Math.min(la, y), Math.abs(width), Math.abs(height));
        this.areaShape = ellipse;
    }
    /**
     * 添加矩形区域点
     * @param lo
     * @param la
     */
    public void addPointRectangle(double lo,double la){
        if(this.areaShape == null){
            this.areaShape = new Rectangle2D.Double(lo, la, 0, 0);
            return;
        }
        Rectangle2D.Double rectangle = (Rectangle2D.Double)this.areaShape;
        double x = rectangle.getX();
        double y = rectangle.getY();
        double width = x - lo;
        double height = y - la;
        rectangle.setFrame(Math.min(lo, x), Math.min(la, y), Math.abs(width), Math.abs(height));
        this.areaShape = rectangle;
    }
    public boolean contains(double lo,double la){
    	int loint = (int)(lo*10000000);
		int laint = (int)(la*10000000);
		if(this.areaShape instanceof Polygon){
			return this.areaShape.contains(loint, laint);
		}else if (this.areaShape instanceof Ellipse2D.Double) {
			return this.areaShape.contains(lo, la);
		}else if (this.areaShape instanceof Rectangle2D.Double) {
			return this.areaShape.contains(lo, la);
		}
		return false;
    }
    public boolean contains(int lo,int la){
    	double lod = ((double)lo/10000000);
    	double lad = ((double)la/10000000);
		if(this.areaShape instanceof Polygon){
			return this.areaShape.contains(lo, la);
		}else if (this.areaShape instanceof Ellipse2D.Double) {
			return this.areaShape.contains(lod, lad);
		}else if (this.areaShape instanceof Rectangle2D.Double) {
			return this.areaShape.contains(lod, lad);
		}
		return false;
    }
    
    public String getCname(){
        return cname;
    }

    public void setCname(String cname){
        this.cname = cname;
    }

    public int getAreaType() {
        return areaType;
    }

    public void setAreaType(int areaType) {
        this.areaType = areaType;
    }
    public static void main(String[] args) {
		AreaInfo areaInfo = new AreaInfo(1,"aa",2);
		areaInfo.addPointRectangle(1.1, 1.1);
		areaInfo.addPointRectangle(3.3, 3.3);
		System.out.println(areaInfo.areaShape.contains(2.2,2.2));
	}
}
