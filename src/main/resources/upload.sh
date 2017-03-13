#! /bin/bash

currentD=$(pwd)"/"
 echo $currentD

read -p  "Please enter the input data directory:	" source
until [[ -d "$source"  ]]; do
echo "Invalid directory"
read -p  "Please enter the input data directory:	" source
done


namefile=names
read -p  "Please enter the HDFS home  directory:    " hdfsD
hdfs dfs -ls $hdfsD
until [[ $? -eq 0 ]]; do
echo "Invalid HDFS home directory"
read -p  "Please enter the HDFS home directory:    " hdfsD
hdfs dfs -ls $hdfsD
done
if [[ ${hdfsD: -1} == '/' ]]; then
hdfsD=${hdfsD%/}
fi
hdfsD=${hdfsD}"/VoTECloud"
hdfs dfs -mkdir $hdfsD
hdfsinput=$hdfsD"/input/"

hdfs dfs -mkdir $hdfsinput

if [[ -e $namefile ]]; then
  rm $namefile
fi

cd $source
j=1
echo "Copying files to HDFS started..."
$(rm -r temp)
mkdir temp
for i in *.bz2; do
name1=${i%%.*}

if [[ ! $( hdfs dfs -ls ${hdfsinput}${j}.bz2 ) ]]; then 
echo "Copying ${j}.bz2 to HDFS"
if [[ ! ( -f temp/${j}.bz2 ) ]]; then
#echo "$j"
  cp -u  $i temp/${j}.bz2
fi
echo "$name1" >> $currentD$namefile
hdfs dfs -copyFromLocal temp/${j}.bz2 $hdfsinput 
else
echo "${j}.bz2 arealdy existed in HDFS"
echo "$name1" >> $currentD$namefile

fi

j=$((j+1))

done
rm -r temp
echo "Copying files to HDFS ended"
cd -

