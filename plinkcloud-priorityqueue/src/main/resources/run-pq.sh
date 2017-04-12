#! /bin/bash
START=$(date +%s)
java -jar plinkcloud-priorityqueue.jar $1 Result.tped 1-26 PASS true
END=$(date +%s)
DIFF=$(( $END - $START ))
echo "Total execution time is: $DIFF"

exit