package edu.emory.plinkcloud.plinkcloud.TabixBased;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
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
	private ConcurrentSkipListSet<Pos> posSet;
	private ExecutorService threadPool;
	class Pos implements Comparable<Pos>{
		String chr;
		int seq;
		public Pos(String chr, int seq){
			this.chr=chr;
			this.seq=seq;
		}
		@Override
		public int compareTo(Pos second){
			if(!this.chr.toLowerCase().equals(second.chr.toLowerCase())){
				int chrnum1, chrnum2;
				switch (chr.toLowerCase()) {
				case "m":
					chrnum1 = 25; break;
				case "x":
					chrnum1 = 23; break;
				case "y":
					chrnum1 = 24; break;

				default:
					chrnum1 = Integer.parseInt(chr);
					break;
				}
				
				switch (second.chr.toLowerCase()) {
				case "m":
					chrnum2 = 25; break;
				case "x":
					chrnum2 = 23; break;
				case "y":
					chrnum2 = 24; break;

				default:
					chrnum2 = Integer.parseInt(chr);
					break;
				}
				return chrnum1-chrnum2;
			}
			else{
				return this.seq-second.seq;
			}
		}
		public String getChr(){
			return chr;
		}
		
		public int getSeq(){
			return seq;
		}
	}
	
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
					String chr =  parseChr(fields[0]) ;
					int seq = Integer.parseInt(fields[1]);
					Pos pos = new Pos(chr, seq);
					posSet.add(pos);
//					if(logger.isDebugEnabled()){
//						logger.debug("the first chr {}, the first pos {}",fields[0],fields[1]);
//					}
				
					
				}
					
				
			}
		}
		
		private String parseChr(String input) throws Exception{
			
			Pattern  pattern = Pattern.compile("[xym\\d]{1,2}",Pattern.CASE_INSENSITIVE);
			Matcher matcher = pattern.matcher(input);
			if(matcher.find()){
				int start = matcher.start();
				int end = matcher.end();
				String chr = input.substring(start,end);
				return chr;
			}
			else{
				throw new Exception("Chromosome can't be parsed");
			}
			
		}
	}
	
	public TabixBasedJoin(String input, String OutputFile){
		posSet = new ConcurrentSkipListSet<Pos>();
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
				if(logger.isDebugEnabled()){
					logger.debug("File name {}",context+name);
				}
				}
			this.readerArray=new TabixReader[readerList.size()];
			this.readerArray=readerList.toArray(readerArray);// Change to array for more efficient access
			this.pw= new PrintWriter( new BufferedWriter(new FileWriter(OutputFile)));
			
		}catch(IOException ioe){
			ioe.printStackTrace();
		}
		}
	
	
	public void join() throws IOException, InterruptedException{
		List<paraReader> taskList = new ArrayList<paraReader>();
		for (int i=0; i<readerArray.length;i++)
			taskList.add(new paraReader(readerArray[i]));
		threadPool.invokeAll(taskList);
		Pos[] posArray = new Pos[posSet.size()];
		posArray = posSet.toArray(posArray);
		
		for(Pos pos:posArray){
			logger.debug("chr {} seq {}", pos.getChr(),pos.getSeq());
	}
		logger.debug("posarray length {}", posArray.length);
}
	
	
	
	
	public static void main(String[] args) {
		TabixBasedJoin tbj=new TabixBasedJoin(args[0],args[1]);
	try{	
		tbj.join();
		}catch(IOException ioe){
			ioe.printStackTrace();
		}catch(InterruptedException ie){
			ie.printStackTrace();
		}

	}

}
