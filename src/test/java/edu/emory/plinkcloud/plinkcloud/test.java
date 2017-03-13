package edu.emory.plinkcloud.plinkcloud;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class test {
	private static final int POS_LENGTH = 10;
	private static final int CHR_LENGTH = 2;
enum Size{
	SMALL("S"), LARGE("L");
	private String desc;
	private Size(String s){
		this.desc = s;
	}
	public String getDesc(){
		return desc;
	}
	
};


private static String getRowKey(String chr, String pos){
	char[] pos_array = new char[POS_LENGTH];
	char[] chr_array = new char[CHR_LENGTH];
	int pos_start_offset = POS_LENGTH - pos.length();
	int chr_start_offset = CHR_LENGTH - chr.length();
	System.arraycopy(pos.toCharArray(), 0, pos_array, pos_start_offset, pos.length());
	System.arraycopy(chr.toCharArray(), 0, chr_array, chr_start_offset, chr.length());
	return new String(chr_array)+"-"+new String(chr_array);			
}

public static void main(String args[]){
	Size test = Enum.valueOf(Size.class, "SMALL");
	Size test1 = Size.LARGE;
//	Pattern  pattern = Pattern.compile("[xym\\d]{1,2}",Pattern.CASE_INSENSITIVE);
//	String input = "abxd";
//	Matcher matcher = pattern.matcher(input);
//	if(matcher.find()){
//		int start = matcher.start();
//		int end = matcher.end();
//		String chr = input.substring(start,end);
//		System.out.println(chr);
//	}
	System.out.println(getRowKey("123","2"));
}
}
