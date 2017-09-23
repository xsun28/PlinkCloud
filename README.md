# High-efficient Schemas of Distributed Systems for Sorted Merging of Omics Data
___
Schemas implemented in Java 7 
for merging multiple VCF files into one VCF file or one TPED file using Apache big-data platforms---MapReduce,HBase and Spark respectively. Source codes can be slightly modified to fit into other types of sorted merging of Omics data.

<br>

## Prerequsites
--- 
 
1. ### Platform Installations
	* Separate installation   
    	* [Install Hadoop MapReduce](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/ClusterSetup.html)
    	* [Install HBase](http://hbase.apache.org/book.html#getting_started)
    	* [Install Spark](https://spark.apache.org/docs/latest/spark-standalone.html)
    * Bundled installation
    	* [Install Cloudera Manager](http://www.cloudera.com/documentation/manager/5-1-x/Cloudera-Manager-Installation-Guide/Cloudera-Manager-Installation-Guide.html)
    * Launch an pre-installed AWS Elastic-M cluster
    	* [How to launch an EMR cluster](https://aws.amazon.com/emr/getting-started/)
    	* [Configure EMR cluster](http://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-configure-apps.html)  
    	We also provide an EMR configuration file used in our test. It can be found as Conf/EMRConfiguration.json  under your project home.  
 <br>
 
 2. ### VCFTools installation
 	* [Install VCFTools](http://vcftools.sourceforge.net/examples.html)
 	* [Install Tabix](http://www.danielecook.com/installing-tabix-and-samtools-on-mac/)
 	
 3. ### [Maven Installation](https://maven.apache.org/install.html)

 4. ### [Git Installation](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)

 5. ### Data Preparation  
 We provide a test dataset of 93 VCF files with encrypted genomic locations. Click [here](https://s3.amazonaws.com/xsun316/encrypted/encrypted.tar.gz) to download.  
 A sample merged result data can be downloaded [here]().  
 Type the following command to unzip downloaded files into 93 bzipped VCF files.     
 
 	```
 	tar xzf encrypted.tar.gz  
	
	```
<br>

## Build Project
---
1. Download the project
	
	```
	git clone https://github.com/xsun28/PlinkCloud.git
	```
2. If you do not want to rebuild the project and your Java version is 7 or higher, you can directly using the jar files from the following locations in your project home directory :
		
	Schema	| Location
	---|---
	**Priority-queue** |  _plinkcloud-priorityqueue/target/plinkcloud-priorityqueue.jar_
	**MapReduce** | _plinkcloud-mapreduce/target/plinkcloud-mapreduce.jar_
	**HBase** | _plinkcloud-hbase/target/plinkcloud-hbase.jar_
	**Spark** | _plinkcloud-spark/target/plinkcloud-spark.jar_

3. If step 2 is not chosen, you can compile the project from scratch
	
	```
	cd project_home
	mvn package -Dmaven.test.skip=True
	```
	Then compiled jar files can be found in the same locations as step 2.
	<br>
	

## Usage
---
1.	Merge VCF files into one VCF file  
	1) VCFTools (Benchmark):
	
	```
	
	``` 
	
2.	Merge VCF files into one TPED file
	 	
	 	
	 	
	 	
	 	
	 	
	 	













