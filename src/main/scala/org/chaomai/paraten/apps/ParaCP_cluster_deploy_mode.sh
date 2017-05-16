#!/usr/bin/env bash

which hadoop > /dev/null
status=$?

if test $status -ne 0 ; then
	echo ""
	echo "hadoop is not accessible."
	echo "Please install Hadoop and make sure the hadoop binary is accessible."
	exit 127
fi

which spark-submit > /dev/null
status=$?

if test $status -ne 0 ; then
	echo ""
	echo "spark-submit is not accessible."
	echo "Please install Spark and make sure the hadoop spark-submit is accessible."
	exit 127
fi

if [ $# -ne 10 ]; then
	 echo 1>&2 "Usage: $0 [dim_1,..,dim_N (tensor)] [rank] [tensor file path] [output path] \
[max iteration] [tolerance] [tries] [master of Spark] \
[spark.cores.max] [spark.executor.memory]"
	 exit 127
fi

hadoop fs -rm -r $4
hadoop fs -mkdir $4

spark-submit \
--class org.chaomai.paraten.apps.ParaCP \
--master $8 \
--deploy-mode cluster \
--executor-cores=$9 \
--executor-memory=${10} \
target/scala-2.11/ParaTen-assembly-1.0.jar \
-s $1 -r $2 -i $3 -o $4 --maxIter $5 --tol $6 --tries $7
