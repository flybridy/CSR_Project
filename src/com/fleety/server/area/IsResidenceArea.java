package com.fleety.server.area;

import java.awt.Polygon;
import java.awt.Shape;
import java.awt.geom.Ellipse2D;
import java.awt.geom.Rectangle2D;

public class IsResidenceArea {
	private int lo;
	private int la;
	private int status;
	private double mile;
	private long time;
	private Shape shape;
	private boolean nw;
	
	public IsResidenceArea(double lo, double la, Shape shape) {
		this.lo = (int)(lo*10000000);
		this.la = (int)(la*10000000);
		this.shape = shape;
		if(shape instanceof Polygon){
			this.nw = shape.contains(this.lo, this.la);
		}else if (shape instanceof Ellipse2D.Double) {
			this.nw = shape.contains(lo, la);
		}else if (shape instanceof Rectangle2D.Double) {
			this.nw = shape.contains(lo, la);
		}
	}

	public IsResidenceArea(int lo, int la,Shape shape,boolean nw) {
		this.lo = lo;
		this.la = la;
		this.shape = shape;
		this.nw = nw;
	}

	public int getLo() {
		return lo;
	}

	public void setLo(int lo) {
		this.lo = lo;
	}

	public int getLa() {
		return la;
	}

	public void setLa(int la) {
		this.la = la;
	}

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public Shape getShape() {
		return shape;
	}

	public void setShape(Shape shape) {
		this.shape = shape;
	}

	public boolean isNw() {
		return nw;
	}

	public void setNw(boolean nw) {
		this.nw = nw;
	}

	public double getMile() {
		return mile;
	}

	public void setMile(double mile) {
		this.mile = mile;
	}

	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}
	
}
