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

RUN curl https://dlcdn.apache.org/nifi/2.0.0-M2/nifi-2.0.0-M2-bin.zip --output /home/bigd/nifi.zip
RUN unzip /home/bigd/nifi.zip -d /home/bigd

ADD nifi.properties /home/bigd/nifi.properties
RUN dos2unix /home/bigd/nifi.properties
RUN mv /home/bigd/nifi.properties /home/bigd/nifi-2.0.0-M2/conf/nifi.properties

RUN dos2unix /run.sh

CMD ["/run.sh"]
