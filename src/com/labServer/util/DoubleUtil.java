package com.labServer.util;

import java.math.BigDecimal;

public class DoubleUtil {
	// Ĭ�ϳ����㾫�ȣ�С����2λ
	private static final Integer DEF_DIV_SCALE = 2;

	public static void main(String[] args) {

		Double x = 1.0;
		Double y = 3.0;
		System.out.println(getDoubleToStringValueByBigDecimalDiv(x, y));
		System.out.println(x + y);

	}

	/**
	 * Double���ͼӷ����㣬����String���ͣ�ʹ��BigDecimal���㣩
	 * 
	 * @param x
	 * @param y
	 * @return x+y
	 */
	public static String getDoubleValueByBigDecimalAdd(Double x, Double y) {
		BigDecimal x1 = new BigDecimal(Double.toString(x));
		BigDecimal y1 = new BigDecimal(Double.toString(y));
		return x1.add(y1).toString();
	}

	/**
	 * Double���ͼ������㣨x-y��������String���ͣ�ʹ��BigDecimal���㣩
	 * 
	 * @param x
	 * @param y
	 * @return x-y
	 */
	public static String getDoubleToStringValueByBigDecimalSub(Double x, Double y) {
		BigDecimal x1 = new BigDecimal(Double.toString(x));
		BigDecimal y1 = new BigDecimal(Double.toString(y));
		return x1.subtract(y1).toString();

	}

	/**
	 * Double���ͳ˷����㣨x*y��������String���ͣ�ʹ��BigDecimal���㣩
	 * 
	 * @param x
	 * @param y
	 * @return x*y
	 */
	public static String getDoubleToStringValueByBigDecimalMul(Double x, Double y) {
		BigDecimal x1 = new BigDecimal(Double.toString(x));
		BigDecimal y1 = new BigDecimal(Double.toString(y));
		return x1.multiply(y1).toString();

	}

	public static String getDoubleToStringValueByBigDecimalDiv(Double x, Double y) {
		BigDecimal x1 = new BigDecimal(Double.toString(x));
		BigDecimal y1 = new BigDecimal(Double.toString(y));
		System.out.println(x1.divide(y1));
		return x1.divide(y1, 2, BigDecimal.ROUND_HALF_DOWN).toString();

	}

}
