#! /bin/bash
START=$(date +%s)
hadoop jar plinkcloud-mapreduce.jar org.plinkcloud.mapreduce.MRVCF2TPED /user/hadoop/plinkcloud/input/ /user/hadoop/mapreduce/output/ $1 0.0001 1-26 true PASS
END=$(date +%s)
DIFF=$(( $END - $START ))
echo "Total execution time is: $DIFF"

exit