FROM hadoop-base:latest

MAINTAINER Ivan Ermilov <ivan.s.ermilov@gmail.com>

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      openjdk-21-jdk
ENV JAVA_HOME=/usr/lib/jvm/java-1.21.0-openjdk-amd64/

HEALTHCHECK CMD curl -f http://localhost:9870/ || exit 1
ENV HDFS_CONF_dfs_namenode_name_dir=file:///hadoop/dfs/name
RUN mkdir -p /hadoop/dfs/name
VOLUME /hadoop/dfs/name

ADD run.sh /run.sh
RUN chmod a+x /run.sh

EXPOSE 9870
EXPOSE 8443
EXPOSE 8080


# nifi
RUN curl https://dlcdn.apache.org/nifi/2.0.0-M2/nifi-2.0.0-M2-bin.zip --output /home/bigd/nifi.zip
RUN unzip /home/bigd/nifi.zip -d /home/bigd

ADD nifi.properties /home/bigd/nifi.properties
RUN dos2unix /home/bigd/nifi.properties
RUN mv /home/bigd/nifi.properties /home/bigd/nifi-2.0.0-M2/conf/nifi.properties

# spark
RUN curl https://dlcdn.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz --output spark.tgz
RUN sudo tar xvf spark.tgz

RUN mv spark-3.5.1-bin-hadoop3 /opt
RUN mv /opt/spark-3.5.1-bin-hadoop3 /opt/spark

RUN chmod -R 777 /opt/spark
RUN echo "export SPARK_HOME=/opt/spark" >> ~/.bashrc
RUN echo "export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin" >> ~/.bashrc

RUN dos2unix /run.sh

CMD ["/run.sh"]
