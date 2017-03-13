package org.plinkcloud.hbase;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.plinkcloud.hbase.HBaseVCF2TPED.Quality;

public class TextParser {
	private static final int POS_LENGTH = 10;
	private static final int CHR_LENGTH = 2;
	private String chrm;
	private int chr_num;
	private String pos;
	private String rs;
	private String genotype;
	private String ref;
	private String [] alts;
	private String rowKey; //row key for HBase row chr_pos;
	private Quality quality;
	public int parseChrnum(String chr){
		int chrm;
		if(chr.equalsIgnoreCase("X")) chrm = 23;
		else if (chr.equalsIgnoreCase("Y")) chrm = 24;
		else if (chr.equalsIgnoreCase("M")) chrm = 25;
		else chrm = Integer.parseInt(chr);
		return chrm;
	}
	
	private Quality getQuality(String line){
		if(line.startsWith("#")) return null; // header
		if(line.contains("PASS")) return Quality.PASS;
		else if(line.contains("q20")) return Quality.Q10;
		else if(line.contains("q10")) return Quality.Q20;
		else return null;
		
	}
	public String getRowKey(String chr, String pos){
		char[] pos_array = new char[POS_LENGTH];
		char[] chr_array = new char[CHR_LENGTH];
		Arrays.fill(pos_array, '0');
		Arrays.fill(chr_array, '0');
		int pos_start_offset = POS_LENGTH - pos.length();
		int chr_start_offset = CHR_LENGTH - chr.length();
		System.arraycopy(pos.toCharArray(), 0, pos_array, pos_start_offset, pos.length());
		System.arraycopy(chr.toCharArray(), 0, chr_array, chr_start_offset, chr.length());
		return  String.valueOf(chr_array)+"-"+ String.valueOf(pos_array);			
	}
	
	public static String[] parseRowKey(String rowkey){
		String row_key[] = new String[2];  //row_key[0] = chr, row_key[1] = pos
		String tmp1 = rowkey.substring(0,rowkey.indexOf("-"));
		String tmp2 = rowkey.substring(rowkey.indexOf("-")+1);
		int chr = Integer.parseInt(tmp1);
		long pos = Long.parseLong(tmp2);
		row_key[0] = String.valueOf(chr);
		row_key[1] = String.valueOf(pos);
		return row_key;
	}
	
	private String parseGenotype(String line){
		
		StringBuilder genotype = new StringBuilder();
		String numbered_genotype = null;//1/0, 1/1...
		Pattern genotypePattern = Pattern.compile("[\\d]{1}([\\/\\|]{1}[\\d]{1})+");
		String [] fields = line.split("\\s");
		String genotype_field = fields[9].trim();
		String [] alts = fields[4].trim().split(",");
		String ref = fields[3].trim();
		Matcher matcher = genotypePattern.matcher(genotype_field);
		if(matcher.find())
			numbered_genotype = genotype_field.substring(matcher.start(),matcher.end());
		String [] genotype_numbers = numbered_genotype.split("[\\/\\|]");
		for (int i=0;i<genotype_numbers.length;i++){
			int number = Integer.parseInt(genotype_numbers[i].trim());
			if(number==0)
				genotype.append(ref).append(" ");
			else
				genotype.append(alts[number-1]).append(" ");	
		}
		return genotype.toString().trim();
	}
	
	public TextParser(){
		
	}
	
	public TextParser(String line){
		quality = getQuality(line);
		if(null != quality){
		String[] fields = line.split("\\s+");
		chrm = fields[0].substring(fields[0].indexOf("r")+1).trim();
		chr_num = parseChrnum(chrm);
		genotype = parseGenotype(line);
		pos = fields[1].trim();
		rs =  fields[2].trim();
		ref = fields[3].trim();
		alts = fields[4].trim().split(",");
		rowKey = getRowKey(String.valueOf(chr_num),pos);
		}
		
	}
	
	public String getChr(){
		return chrm;
	}
	
	public int getChrNum(){
		return chr_num;
	}
	
	public String getPos(){
		return pos;
	}
	
	public String getRs(){
		return rs;
	}
	
	public String getGenotype(){
		return genotype;
	}
	
	public String getRef(){
		return ref;
	}
	
	public String[] getAlts(){
		return alts;
	}
	
	public String getRowkey(){
		return rowKey;
	}
	
	public Quality getQuality(){
		return quality;
	}
}
