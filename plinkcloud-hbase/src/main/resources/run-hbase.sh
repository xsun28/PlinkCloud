#! /bin/bash
START=$(date +%s)
hadoop jar plinkcloud-hbase.jar org.plinkcloud.hbase.HBaseVCF2TPEDSelectedChr plinkcloud/input/  HBase 0.0001 $1 PASS 1-26 true
END=$(date +%s)
DIFF=$(( $END - $START ))
echo "Total execution time is: $DIFF"

exit