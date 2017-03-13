#! /bin/bash
START=$(date +%s)
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
read -p "Do you want result TPED files to be compressed? Y(es), N(o): " compression
until [[ "$compression" =~ [YyNn] ]]; do
echo "Invalid input"
read -p "Do you want result TPED files to be compressed? Y(es), N(o): " compression
done



read -p "Please enter chromsome range such as 1-3:	" chrmrange
parserange "$chrmrange"
until [[ "$chrmrange" =~ ^[[:digit:]]{1,2}-[[:digit:]]{1,2}$ &&  "$first" -gt 0 && "$last" -lt 26 ]]; do
echo "Invalid chromosome range input"
read -p "Please enter chromsome range such as 1-3:    " chrmrange
parserange "$chrmrange"
done

read -p  "Please enter the HDFS home  directory:    " hdfsD
until [[ $(hdfs dfs -ls $hdfsD)  ]]; do
echo "Invalid HDFS home directory"
read -p  "Please enter the HDFS home directory:    " hdfsD
done
if [[ ${hdfsD: -1} == '/' ]]; then
hdfsD=${hdfsD%/}
fi
hdfsD=${hdfsD}"/VoTECloud"
hdfsoutput=$hdfsD"/output/"

echo "Copying result TPED files from cluster to local started..."
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
if [[ "$compression" =~ [Yy] ]]; then
resultfile="chr"$j".tped.bz2"
else resultfile="chr"$j".tped"
fi

hdfssource=${hdfsoutput}"chr"$j"_result/chr"$j".tped/part*"

#resultfileU="chr"$j".tped"

#hdfssource1=${hdfsoutput}"chr"$j"_result/chr"$j".freq/part*"
#resultfile1="chr"$j".freq.bz2"
#resultfile1U="chr"$j".freq"

#echo $hdfssource
#echo $interfile
#echo $hdfssource1
#echo $interfile1
if [[ ! ( -f "$resultfile" )]]; then
hdfs dfs -getmerge $hdfssource $resultfile

#bunzip2 $resultfile
  
fi

#if [[ ! ( -f "$resultfile1" )]]; then

#hdfs dfs -getmerge $hdfssource1 $resultfile1

#bunzip2 $resultfile1

#fi

done
echo "Copying results ended..."
END=$(date +%s)
DIFF=$((END-START))
echo "Elapsed time is $DIFF seconds"
exit
