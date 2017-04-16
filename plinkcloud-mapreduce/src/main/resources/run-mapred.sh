#! /bin/bash
START=$(date +%s)
hadoop jar plinkcloud-mapreduce.jar org.plinkcloud.mapreduce.MRVCF2TPED -D mapreduce.task.io.sort.mb=600 -D mapreduce.reduce.merge.inmem.threshold=0 -D mapreduce.reduce.input.buffer.percent=1 /user/hadoop/plinkcloud/input/ /user/hadoop/mapreduce/output/ $1 0.0001 1-26 false PASS
END=$(date +%s)
DIFF=$(( $END - $START ))
echo "Total execution time is: $DIFF"

exit