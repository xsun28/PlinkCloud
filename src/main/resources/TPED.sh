#! /bin/bash
START=$(date +%s)
temp=
len=
first=
last=
namefile=names
ordered=
parserange ( ) {
temp=${1%-*}
len=${#temp}
first=${chrmrange:0:len}
last=${chrmrange:$((len+1))}
case $first in
x|X) first=23
;;
y|Y) first=24
;;
m|M) first=25
esac

case $last in
x|X) last=23
;;
y|Y) last=24
;;
m|M) last=25
esac

}


read -p "Please enter the libary containing the required hadoop jars: " lib
until [[ -d "$lib" ]]; do
echo "Invalid directory"
ead -p "Please enter the libary containing the required hadoop jars: " lib
done

if [[ ${lib: -1} == '/' ]]; then
hdfsD=${lib%/}
fi

read -p "Are the data ordered by SNP positions? Y(es), N(o): " ordered
until [[ "$ordered" =~ [YyNn] ]]; do
echo "Invalid input"
read -p "Are the data ordered by SNP positions? Y(es), N(o): " ordered
done

read -p "Do you want compress result TPED files? Y(es), N(o): " compression
until [[ "$compression" =~ [YyNn] ]]; do
echo "Invalid input"
read -p "Do you want compress result TPED files? Y(es), N(o): " compression
done

if [[ "$compression" =~ [Yy] ]]; then
compression=true
else compression=false;
fi

currentD=$(pwd)"/"
 echo $currentD
if [[ ! ( -f $namefile ) ]]; then
read -p  "Please enter the input data directory:	" source
until [[ -d "$source"  ]]; do
echo "Invalid directory"
read -p  "Please enter the input data directory:	" source
done
cd $source

for i in *.bz2; do
name1=${i%%.*}
echo "$name1" >> $currentD$namefile
done
cd -
fi 

read -p  "Please enter the path of individual information file:   " infofile
until [[ -f "$infofile"  ]]; do
echo "Invalid info file path"
read -p  "Please enter the path of individual information file:   " infofile
done

read -p "Please enter individual number:	" fileno
until [[  "$fileno" =~ ^[[:digit:]]+$ && $fileno -gt 0  ]]; do
echo "Individual number must great than 0"
read -p "Please enter individual number:	" fileno
done
read -p "Please enter the sampling rate:	" rate
until [[  "$rate" =~ ^0?\.[[:digit:]]+$  ]]; do
echo "Invalid rate input"
read -p "Please enter sampling rate:     " rate
done
read -p "Please enter chromsome number in the form such as 1-3:	" chrmrange
parserange "$chrmrange"
until [[ "$chrmrange" =~ ^[[:digit:]]{1,2}-[[:digit:]]{1,2}$ &&  "$first" -gt 0 && "$last" -lt 26 ]]; do
echo "Invalid chromosome range input"
read -p "Please enter chromsome number such as 1-3:    " chrmrange
parserange "$chrmrange"
done

read -p  "Please enter the HDFS home  directory:    " hdfsD
hdfs dfs -ls $hdfsD
until [[ $? -eq 0  ]]; do
echo "Invalid HDFS home directory"
read -p  "Please enter the HDFS home directory:    " hdfsD
hdfs dfs -ls $hdfsD
done
if [[ ${hdfsD: -1} == '/' ]]; then
hdfsD=${hdfsD%/}
fi
hdfsD=${hdfsD}"/VoTECloud"
hdfsinput=$hdfsD"/input/"
hdfsoutput=$hdfsD"/output/"



rm *.class
if [[ "$ordered" =~ [Yy] ]]; then
method=HDVCF2TPEDOrdered
else 
method=HDVCF2TPEDUnordered
fi

#javac -cp .:commons-cli-1.2.jar:hadoop-common-2.5.0-cdh5.3.0.jar:hadoop-mapreduce-client-core-2.5.0-cdh5.3.0.jar:commons-logging-1.1.3.jar $method.java CFRecordReader.java SequenceFileReadCombiner.java SequenceFileReadCombinerSmall.java JobRunner.java
javac -cp .:$lib/* $method.java CFRecordReader.java SequenceFileReadCombiner.java SequenceFileReadCombinerSmall.java JobRunner.java
jar -cvf HDPLINK.jar *.class


echo "Cluster computing started..."
hadoop jar ./HDPLINK.jar $method $hdfsinput  $hdfsoutput $fileno $rate $chrmrange $compression
echo "Cluster computing ended.."



javac tfam.java Info.java
java -cp . tfam $infofile $namefile $chrmrange

echo "Copying tfam files from local to cluster started..."
for (( i=$first; i<=$last; i++ )); do
case $i in 
23) j="X"
;;
24) j="Y"
;;
25) j="M"
;;
*) j=$i
;;
esac

tfamf="chr"$j".tfam"
mv "chr"$i".tfam" $tfamf
hdfsdest=${hdfsoutput}"chr"$j"_result/"

#echo $hdfssource
#echo $interfile
#echo $hdfssource1
#echo $interfile1

hdfs dfs -copyFromLocal $tfamf $hdfsdest


done
echo "Copying tfam files ended..."

#echo $method
#echo $infofile
#echo $namefile
#echo $inter
#echo $output
#echo $chrmrange
#echo $fileno

rm $namefile
END=$(date +%s)
DIFF=$(( $END - $START )) 
echo "Total execution time is: $DIFF"

exit
