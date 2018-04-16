package com.ucloudlink.css.util;

import java.util.Random;

public class StringUtil {
	public static boolean isEmpty(String target){
		if(target==null||"".equals(target)||"null".equals(target)){
			return true;
		}
		return false;
	}
	public static String random(int len){
		String result="";
		Random random = new Random();
		while(result.length()<len){
			int diff = len-result.length();
			result+=""+random.nextInt(Double.valueOf(Math.pow(10, diff)).intValue());
		}
		return result;
	}
}
