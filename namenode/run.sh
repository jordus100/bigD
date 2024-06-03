#!/bin/bash

export JAVA_HOME=/usr/lib/jvm/java-1.21.0-openjdk-amd64

# Dump all environment variables for interactive users
env | egrep -v "^(HOME=|USER=|MAIL=|LC_ALL=|LS_COLORS=|LANG=|HOSTNAME=|PWD=|TERM=|SHLVL=|LANGUAGE=|_=)" >> /etc/environment

# Create Python venv for Apache Spark
python3 -m venv /home/bigd/sparkvenv
source /home/bigd/sparkvenv/bin/activate
pip install pyspark pyspark[sql] matplotlib pillow venv-pack
venv-pack -o /home/bigd/sparkvenv.tar.gz
chown bigd /home/bigd/sparkvenv.tar.gz

/home/bigd/nifi-1.26.0/bin/nifi.sh start
/home/bigd/nifi-1.26.0/bin/nifi.sh set-single-user-credentials bigd bigdbigdbigd
/home/bigd/nifi-1.26.0/bin/nifi.sh restart

# Launch Spark
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
