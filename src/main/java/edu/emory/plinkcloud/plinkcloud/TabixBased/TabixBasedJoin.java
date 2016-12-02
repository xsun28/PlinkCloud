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
import java.util.NavigableSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TabixBasedJoin {
	
	public static final int READ_SUCCESS = 1;
	public static final int WRITE_SUCCESS = 2;
	
	private TabixReader[] readerArray;
	protected final Logger logger = LoggerFactory.getLogger(getClass());  
	private ConcurrentSkipListSet<Pos> posSet;
	private ExecutorService threadPool;
	private String outputDir;
	
	class Pos implements Comparable<Pos>{
		private String chr;
		private String ref;
		private int seq;
		
		public Pos(String chr, int seq){
			this.chr = chr;
			this.seq = seq;
		}
		
		public Pos(String chr, int seq, String ref){
			this.chr = chr;
			this.seq = seq;
			this.ref = ref;
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
					chrnum2 = Integer.parseInt(second.chr);
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
		
		public String getRef(){
			return ref;
		}
	}// end of Pos class
	
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
					String chr =  parseChr(fields[0].trim()) ;
					int seq = Integer.parseInt(fields[1].trim());
					String ref = fields[3].trim();
					Pos pos = new Pos(chr, seq, ref);
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
		
	}// end of paraReader
	

	public TabixBasedJoin(String input, String outputDir){
		posSet = new ConcurrentSkipListSet<Pos>();
		threadPool=Executors.newCachedThreadPool();
		this.outputDir=outputDir;
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
			File out_dir = new File(outputDir);
			if(!out_dir.exists()) out_dir.mkdirs();
			
			
		}catch(IOException ioe){
			ioe.printStackTrace();
		}
		
	}//end of the constructor of TabixBasedJoin
	
	public void readPosToSet() throws IOException, InterruptedException{
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
	
	public void JoinToTPed() throws IOException, InterruptedException{
		ArrayList<writeToFileAsTPed> writeTaskList = new ArrayList<writeToFileAsTPed>();  
		for(int i=1;i<=25;i++)
           writeTaskList.add(new writeToFileAsTPed(i));
		
		threadPool.invokeAll(writeTaskList);
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
	
	private NavigableSet<Pos> splitSet(int i){
		String floor_chr = convertToChr(i);
		String ceil_chr = convertToChr(i+1);
		return posSet.subSet(new Pos(floor_chr,0), new Pos(ceil_chr,0)); 
	}
	
	class writeToFileAsTPed implements Callable<Integer>{
		private int chr;
		public writeToFileAsTPed(int i){
			this.chr = i;
		}
		@Override
		public Integer call() throws IOException{
			String chrString = convertToChr(chr);
		    String outputFile = outputDir+"/"+chrString;
		    NavigableSet<Pos> subset = splitSet(chr);
		    PrintWriter pw = new PrintWriter( new BufferedWriter(new FileWriter(outputFile)));
		    String chr_str;
		    String ref;
		    int seq;
		    String query;
		    Pattern genotypePattern = Pattern.compile("[\\d]{1}([\\/\\|]{1}[\\d]{1})+");
		    for(Pos pos: subset){
		               chr_str = pos.getChr();
		               seq = pos.getSeq();
		               ref = pos.getRef();
		               StringBuilder query_builder = new StringBuilder();
		               StringBuilder result_builder = new StringBuilder();
		               query = query_builder.append("chr").append(chr).append(":").append(seq).append("-").append(seq).toString();
		               result_builder.append(chr_str).append("\t").append("rs#\t").append("0\t").append(seq);
		              for (TabixReader reader: readerArray){
		            	  String result;
		            	  TabixReader.Iterator queryResultsIter = reader.query(query);
		            	  if(queryResultsIter != null && (result = queryResultsIter.next()) != null)
		            	  {
		            		 String [] fields = result.split("\\s");
		            		 String genotype_field = fields[9].trim();
		            		 String alt = fields[4].trim();
		            		 Matcher matcher = genotypePattern.matcher(genotype_field);
		            		 String genotype = genotype_field.substring(matcher.start(), matcher.end());
		            		 switch(genotype){
		            		 case "0/1" :
		            		 case "1/0" :
		            			 result_builder.append("\t").append(ref+" "+alt);
		            			 break;
		            		 case "0/0":
		            			 result_builder.append("\t").append(ref+" "+ref);
		            			 break;
		            		 case "1/1":
		            			 result_builder.append("\t").append(alt+" "+alt);
		            			 break;
		            		default:
		            			logger.error("Unknown genotype: {}",genotype);
		            			break;
		            		 }
		            	  }else{
		            		  result_builder.append("\t").append(ref+" "+ref);
		            	  }
		              }
		      	pw.println(result_builder.toString());
		    }
		    pw.close();
		    return WRITE_SUCCESS;
		}
		
	}// end of writeToFileAsTPed class


	public static void main(String[] args) {
	
		TabixBasedJoin tbj=new TabixBasedJoin(args[0],args[1]);
	try{	
		tbj.readPosToSet();
		tbj.JoinToTPed();
		}catch(IOException ioe){
			ioe.printStackTrace();
		}catch(InterruptedException ie){
			ie.printStackTrace();
		}
	}//end of main

}
