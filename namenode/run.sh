#!/bin/bash

JAVA_HOME=/usr/lib/jvm/java-1.21.0-openjdk-amd64 /home/bigd/nifi-2.0.0-M2/bin/nifi.sh start
JAVA_HOME=/usr/lib/jvm/java-1.21.0-openjdk-amd64 /home/bigd/nifi-2.0.0-M2/bin/nifi.sh set-single-user-credentials bigd bigdbigdbigd
JAVA_HOME=/usr/lib/jvm/java-1.21.0-openjdk-amd64 /home/bigd/nifi-2.0.0-M2/bin/nifi.sh restart
/opt/spark/sbin/start-master.sh

namedir=`echo $HDFS_CONF_dfs_namenode_name_dir | perl -pe 's#file://##'`
if [ ! -d $namedir ]; then
  echo "Namenode name directory not found: $namedir"
  exit 2
fi

if [ -z "$CLUSTER_NAME" ]; then
  echo "Cluster name not specified"
  exit 2
fi

echo "remove lost+found from $namedir"
rm -r $namedir/lost+found

if [ "`ls -A $namedir`" == "" ]; then
  echo "Formatting namenode name directory: $namedir"
  $HADOOP_HOME/bin/hdfs --config $HADOOP_CONF_DIR namenode -format $CLUSTER_NAME
fi

$HADOOP_HOME/bin/hdfs --config $HADOOP_CONF_DIR namenode
