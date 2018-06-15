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
	public static String letter(int len){
		String result="";
		int standard = 127;//255
		Random random = new Random();
		while(result.length()<len){
			int letter = random.nextInt(standard);
			if(letter>32&&letter<127){
				result+=""+Character.toString((char)letter);
			}
		}
		return result;
	}
}
