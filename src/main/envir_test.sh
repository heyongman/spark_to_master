#!/usr/bin/env bash
#对应组件Home路径
SPARK_HOME=/opt/software/spark-2.4.4-bin-hadoop2.7
AZKABAN_HOME=/opt/software/azkaban-3.79.0

#已免密的待检主机名或IP,空格分隔
HOSTS=(10.197.236.216 hadoop02)

function foo() {
  host=$1

  echo -n -e "JAVA检测:\t"
  ssh root@$host 'source /etc/profile;java -version 2>&1 | grep version'

  echo -n -e "PYTHON检测:\t"
  ssh root@$host '/usr/bin/env python -V'

  echo -n -e "SPARK检测:\tSpark"
  ssh root@$host "$SPARK_HOME/bin/spark-shell --version 2>&1 | grep version | grep -v Scala | cut -d '\' -f 4"

  echo -n -e "AZKABAN检测:\t"
  ssh root@$host "cd $AZKABAN_HOME"
  echo "进程：`ssh root@$host 'source /etc/profile;jps | grep AzkabanExecutorServer'`"
}

for host in ${HOSTS[@]}; do
  echo -e "\n>>>>>>>>当前检测主机:$host<<<<<<<<"
  foo $host
done


