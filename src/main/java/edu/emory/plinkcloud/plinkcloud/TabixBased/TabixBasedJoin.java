package edu.emory.plinkcloud.plinkcloud.TabixBased;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;

public class TabixBasedJoin {
	private TabixReader[] readerArray;
	private PrintWriter pw=null;
	
	public TabixBasedJoin(String input, String OutputFile){
		File dir=new File(input);
		String[] fileNames=dir.list();
		ArrayList<String> nameList = new ArrayList<String>(Arrays.asList(fileNames));
		Collections.sort(nameList,new Comparator<String>(){
			public int compare(String name1,String name2){
				int number1 = Integer.parseInt(name1.substring(0,name1.indexOf(".")));
				int number2 = Integer.parseInt(name2.substring(0,name1.indexOf(".")));
				return number1-number2;
			}
		});
		
		try{
			ArrayList<TabixReader> readerList=new ArrayList<TabixReader>();
			for(String name:nameList)
				readerList.add(new TabixReader(name));
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
