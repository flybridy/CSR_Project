package com.fleety.analysis.realtime;

import com.fleety.util.pool.db.redis.RedisTableBean;

public class OrdinaryAreaCarBean extends RedisTableBean{
		private String car_no;
		private int FreeLoadStatu;//0�ճ���1Ϊ�س�
		private String OnlieStatu;//����,���ߣ�����
		private int area_id;
		private int come_id;
		private double lo;//����
		private double la;//γ��
		
		public double getLo() {
			return lo;
		}
		public void setLo(double lo) {
			this.lo = lo;
		}
		public double getLa() {
			return la;
		}
		public void setLa(double la) {
			this.la = la;
		}
		public String getCar_no() {
			return car_no;
		}
		public void setCar_no(String car_no) {
			this.car_no = car_no;
		}
		public int getFreeLoadStatu() {
			return FreeLoadStatu;
		}
		public void setFreeLoadStatu(int freeLoadStatu) {
			FreeLoadStatu = freeLoadStatu;
		}
		
		public String getOnlieStatu() {
			return OnlieStatu;
		}
		public void setOnlieStatu(String onlieStatu) {
			OnlieStatu = onlieStatu;
		}
		public int getArea_id() {
			return area_id;
		}
		public void setArea_id(int area_id) {
			this.area_id = area_id;
		}
		public int getCome_id() {
			return come_id;
		}
		public void setCome_id(int come_id) {
			this.come_id = come_id;
		}
		
		
}
