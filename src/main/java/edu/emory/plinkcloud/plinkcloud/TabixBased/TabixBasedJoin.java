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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TabixBasedJoin {
	private TabixReader[] readerArray;
	private PrintWriter pw=null;
	protected final Logger logger = LoggerFactory.getLogger(getClass());  
	
	public TabixBasedJoin(String input, String OutputFile){
		File dir=new File(input);
		String[] fileNames=dir.list(new FilenameFilter(){
			@Override
			public boolean accept(File dir,String name){
				String regex = ".*vcf$";
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
	
	
	public void join() throws IOException{
		System.out.println(readerArray[0].readLine());
	}
	
	
	
	
	public static void main(String[] args) {
		TabixBasedJoin tbj=new TabixBasedJoin(args[0],args[1]);
	try{	
		tbj.join();
		}catch(IOException ioe){
			ioe.printStackTrace();
		}

	}

}
