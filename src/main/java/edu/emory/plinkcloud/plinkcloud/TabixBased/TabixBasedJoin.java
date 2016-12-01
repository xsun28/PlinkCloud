package edu.emory.plinkcloud.plinkcloud.TabixBased;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TabixBasedJoin {
	private final int READ_SUCCESS = 1;
	private TabixReader[] readerArray;
	private PrintWriter pw=null;
	protected final Logger logger = LoggerFactory.getLogger(getClass());  
	private ConcurrentSkipListSet<BitSet> posSet;
	private ExecutorService threadPool;
	private String outputDir;
//	class Pos implements Comparable<Pos>{
//		String chr;
//		int seq;
//		public Pos(String chr, int seq){
//			this.chr=chr;
//			this.seq=seq;
//		}
//		@Override
//		public int compareTo(Pos second){
//			if(!this.chr.toLowerCase().equals(second.chr.toLowerCase())){
//				int chrnum1, chrnum2;
//				switch (chr.toLowerCase()) {
//				case "m":
//					chrnum1 = 25; break;
//				case "x":
//					chrnum1 = 23; break;
//				case "y":
//					chrnum1 = 24; break;
//
//				default:
//					chrnum1 = Integer.parseInt(chr);
//					break;
//				}
//				
//				switch (second.chr.toLowerCase()) {
//				case "m":
//					chrnum2 = 25; break;
//				case "x":
//					chrnum2 = 23; break;
//				case "y":
//					chrnum2 = 24; break;
//
//				default:
//					chrnum2 = Integer.parseInt(second.chr);
//					break;
//				}
//				return chrnum1-chrnum2;
//			}
//			else{
//				return this.seq-second.seq;
//			}
//		}
//		public String getChr(){
//			return chr;
//		}
//		
//		public int getSeq(){
//			return seq;
//		}
//	}
	
	class paraReader implements Callable<Integer>{
		private TabixReader treader;
		public paraReader(TabixReader treader){
			this.treader=treader;
		}
		@Override
		public Integer call() {
			try{
			extractPosToSet();
			}
			catch(IOException ioe){
				logger.error("IOE exception in paraReader {} ", ioe.getStackTrace());
			}
			catch(Exception e){
				logger.error("Exception {}",e.getStackTrace());
			}
			return READ_SUCCESS;
		}
		
		private void extractPosToSet() throws IOException, Exception{
			String line;
			boolean header=true;
			while ((line = treader.readLine()) != null){
				if(!line.toLowerCase().startsWith("#chrom")&&header)
					continue;
				else if (line.toLowerCase().startsWith("#chrom")){
					header=false;
					continue;
				}
				else{
					String[] fields = line.split("\\s");
					int chr =  parseChr(fields[0].trim()) ;
					int seq = Integer.parseInt(fields[1].trim());
					BitSet bs = new BitSet(37);
					bs = setBits(bs, chr,seq);
//					Pos pos = new Pos(chr, seq);
					posSet.add(bs);
//					if(logger.isDebugEnabled()){
//						logger.debug("the first chr {}, the first pos {}",fields[0],fields[1]);
//					}
				
					
				}
					
				
			}
		}
	}
		
		public BitSet setBits(BitSet bs, int chr, int seq){
			int remain = chr;
			for (int i=3;i>=0;i--){
				if(remain==0) break;
				int residual = remain%2;
				remain = remain/2;				
				if(residual==1) bs.set(i);				
			}
			
			remain = seq;
			for(int i=36;i>=4;i--){
				if(remain==0) break;
				int residual = remain%2;
				remain = remain/2;
				if(residual==1) bs.set(i);
			}
			return bs;
		}
		
		private int parseChr(String input) throws Exception{
			
			Pattern  pattern = Pattern.compile("[xym\\d]{1,2}",Pattern.CASE_INSENSITIVE);
			Matcher matcher = pattern.matcher(input);
			if(matcher.find()){
				int start = matcher.start();
				int end = matcher.end();
				String chr = input.substring(start,end);
				int chrnum;
				switch (chr.toLowerCase()) {
				case "m":
					chrnum = 25; break;
				case "x":
					chrnum = 23; break;
				case "y":
					chrnum = 24; break;

				default:
					chrnum = Integer.parseInt(chr);
					break;
				}
				return chrnum;
			}
			else{
				throw new Exception("Chromosome can't be parsed");
			}
			
		}
	
	
	 private String convertToChr(int i){
		 switch (i){
		 case 23:
			 return "X";
		 case 24:
			 return "Y";
		 case 25:
			 return "M";	 
		default:
			return Integer.toString(i);
			
		 }
	 }
	 
//	private Set<Pos> splitSet(int i){
//		String chr = convertToChr(i+1);
//		return posSet.headSet(new Pos(chr,0));	
//	}
	
	public TabixBasedJoin(String input, String OutputDir){
		posSet = new ConcurrentSkipListSet<BitSet>();
		threadPool=Executors.newCachedThreadPool();
		
		File dir=new File(input);
		String[] fileNames=dir.list(new FilenameFilter(){
			@Override
			public boolean accept(File dir,String name){
				String regex = ".*gz$";
				return name.matches(regex);
			}
		});
		String context=dir.getAbsolutePath()+"/";

		ArrayList<String> nameList = new ArrayList<String>(Arrays.asList(fileNames));
		Collections.sort(nameList,new Comparator<String>(){
			@Override
			public int compare(String name1,String name2){
				int number1 = Integer.parseInt(name1.substring(0,name1.indexOf(".")));
				int number2 = Integer.parseInt(name2.substring(0,name1.indexOf(".")));
				return number1-number2;
			}
		});
		
		try{
			ArrayList<TabixReader> readerList=new ArrayList<TabixReader>();
			for(String name:nameList){
				readerList.add(new TabixReader(context+name));
//				if(logger.isDebugEnabled()){
//					logger.debug("File name {}",context+name);
//				}
				}
			this.readerArray=new TabixReader[readerList.size()];
			this.readerArray=readerList.toArray(readerArray);// Change to array for more efficient access
			this.outputDir = outputDir;
			File output = new File(outputDir);
			output.mkdirs();
			
		}catch(IOException ioe){
			ioe.printStackTrace();
		}
		}
	
	
	public void readPosToSet() throws IOException, InterruptedException{
		List<paraReader> taskList = new ArrayList<paraReader>();
		for (int i=0; i<readerArray.length;i++)
			taskList.add(new paraReader(readerArray[i]));
		threadPool.invokeAll(taskList);
		//Pos[] posArray = new Pos[posSet.size()];
		//posArray = posSet.toArray(posArray);
		
		for(Pos pos:posSet){
			logger.debug("chr {} seq {}", pos.getChr(),pos.getSeq());
	}
		logger.debug("posarray length {}", posSet.size());
}
	
	public void Join() throws IOException{
		for(int i=0;i<25;i++){
			writeToFile(splitSet(i),i);
		}
	}
	
	private void writeToFile(Set<Pos> subset, int i) throws IOException{
		String chrString = convertToChr(i);
		String outputFile = outputDir+"/"+chrString;
		this.pw= new PrintWriter( new BufferedWriter(new FileWriter(outputFile)));
		String chr;
		int seq;
		String query;
		for(Pos pos: subset){
			 chr = pos.getChr();
			 seq= pos.getSeq();
			 StringBuilder builder = new StringBuilder();
			 query = builder.append("chr").append(chr).append(":").append(seq).append("-").append(seq).toString();
			for (TabixReader reader: readerArray){
				
			}
		}
	}
	
	public static void main(String[] args) {
		TabixBasedJoin tbj=new TabixBasedJoin(args[0],args[1]);
	try{	
		BitSet test= new BitSet(37);
		int chr = 7;
		int seq = 1023;
		test=tbj.setBits(test,chr,seq);
		System.out.println(test.toString());
		System.exit(0);
		tbj.readPosToSet();
		}catch(IOException ioe){
			ioe.printStackTrace();
		}catch(InterruptedException ie){
			ie.printStackTrace();
		}

	}

}
