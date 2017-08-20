package com.labServer.util;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexUtil {

	public static void main(String[] args) {

		System.out.println("Start");
		String str = "+YAV:0005AABB,000 000 000 007 001 ,000 000 000 007 001 ,007 001 007 000 000 ,009 001 008 000 000 ,000 000 004 000 000 ,004 000 008 001 003 ,001 005 004 000 002 ,008 00C 00B 008 008 ,0 0,0 0,0 0 0 0,00,FF0203FF,V V V V V V V V,8AD00001,X,EEFF";

		// int[] o = getParams(str);

		// getVoltagesParam("9");

		// System.out.println(getTemperatureByVol(getVoltagesParam(hex2Decimal("266"))));
		// Double [] params=getParams(str);
		// for (int j = 0; j < params.length; j++) {
		// System.out.println(params[j]);
		// }
		// System.out.println(getParams(str).toString());
		getParams(str);
		System.out.println("boardNumber: " + getBoardNumber(str));
		System.out.println(getParams(str));
	}

	/**
	 * vthe 8 segment of the message and hex to Decimal before stored in the
	 * HashMap
	 * 
	 * @param str
	 * @return Map<String, Double> { 8AD0000101T=-20.0,8AD0000101H=0.0,
	 *         8AD0000102T=-19.83,8AD0000102H=0.22,
	 *         8AD0000103T=-20.0,8AD0000103H=0.1,
	 *         8AD0000104T=-19.98,8AD0000104H=0.2 }
	 * @author Chenhao.Yao 2017-03-31
	 * 
	 */
	public static String getParams(String str) {
		Map<String, Double> paramsMap = new TreeMap<String, Double>();
		Double[] params = new Double[8];
		String pattern = ",[0-9A-F]{3}\\s";
		Pattern r = Pattern.compile(pattern);
		Matcher m = r.matcher(str);
		int index = 0;
		// #01:proberNum�� T��temp�� H��Hum
		String[] probeNum = { "01", "02", "03", "04" };
		String boardNumber = getBoardNumber(str);
		StringBuffer paramStr = new StringBuffer();
		while (m.find()) {
			if (index == 0) {
				paramStr.append(boardNumber + probeNum[0] + ":"
						+ getTemperatureByVol(getVoltagesParam(hex2Decimal(m.group().substring(1, 4)))));
			} else if (index == 1) {
				paramStr.append(":" + getHumidityByVol(getVoltagesParam(hex2Decimal(m.group().substring(1, 4)))) + ";");
			} else if (index == 2) {
				paramStr.append(boardNumber + probeNum[1] + ":"
						+ getTemperatureByVol(getVoltagesParam(hex2Decimal(m.group().substring(1, 4)))));
			} else if (index == 3) {
				paramStr.append(":" + getHumidityByVol(getVoltagesParam(hex2Decimal(m.group().substring(1, 4)))) + ";");
			} else if (index == 4) {
				paramStr.append(boardNumber + probeNum[2] + ":"
						+ getTemperatureByVol(getVoltagesParam(hex2Decimal(m.group().substring(1, 4)))));
			} else if (index == 5) {
				paramStr.append(":" + getHumidityByVol(getVoltagesParam(hex2Decimal(m.group().substring(1, 4)))) + ";");
			} else if (index == 6) {
				paramStr.append(boardNumber + probeNum[3] + ":"
						+ getTemperatureByVol(getVoltagesParam(hex2Decimal(m.group().substring(1, 4)))));
			} else if (index == 7) {
				paramStr.append(":" + getHumidityByVol(getVoltagesParam(hex2Decimal(m.group().substring(1, 4)))) + ";");
			}
			index++;
		}
		return paramStr.toString();
	}

	/**
	 * Through the regular expression to parse BoardNumber
	 * 
	 * @param str
	 * @return boardNumber
	 * @author Chenhao.Yao 2017-03-31
	 */
	public static String getBoardNumber(String str) {
		// String pattern = ",8AD[0-9]{5}";
		String strings = "";
		String pattern = "8AD[0-9]{5}";
		Pattern r = Pattern.compile(pattern);
		Matcher m = r.matcher(str);
		while (m.find()) {
			strings = m.group().toString();
			break;
		}
		return strings;
		// System.out.println(strings);
	}

	/**
	 * Hex to Decimal
	 * 
	 * @param str
	 * @return int
	 * @return array
	 * @author Chenhao.Yao 2017-03-31
	 *
	 */
	public static Double hex2Decimal(String str) {
		Integer num = 0;
		num = Integer.parseInt(str, 16);
		return num.doubleValue();
	}

	/**
	 * String To Vol
	 * 
	 * @param params
	 * @return
	 */
	public static Double getVoltagesParam(Double params) {
		double vol = params * 10 / 4095;
		DecimalFormat df = new DecimalFormat("0.000");
		Double.parseDouble(df.format(vol));
		// System.out.println(Double.parseDouble(df.format(vol)));
		return Double.parseDouble(df.format(vol));
	}

	/**
	 * 0-10v=0-100%rh Hum y=kx ======> y=10x
	 * 
	 * @param param
	 * @return
	 */
	public static Double getHumidityByVol(Double param) {
		BigDecimal k = new BigDecimal(10);
		BigDecimal x = new BigDecimal(Double.toString(param));
		return x.multiply(k).doubleValue();
	}

	/**
	 * 0-10V=-20~80 Temp y=kx+b =====> y=10x-20
	 * 
	 * @param param
	 * @return
	 */
	public static Double getTemperatureByVol(Double param) {
		BigDecimal k = new BigDecimal(10);
		BigDecimal b = new BigDecimal(20);
		BigDecimal x = new BigDecimal(Double.toString(param));
		return x.multiply(k).subtract(b).doubleValue();
	}
}
