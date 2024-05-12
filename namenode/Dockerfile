FROM hadoop-base:latest

MAINTAINER Ivan Ermilov <ivan.s.ermilov@gmail.com>

HEALTHCHECK CMD curl -f http://localhost:9870/ || exit 1

ENV HDFS_CONF_dfs_namenode_name_dir=file:///hadoop/dfs/name
RUN mkdir -p /hadoop/dfs/name
VOLUME /hadoop/dfs/name

ADD run.sh /run.sh
RUN chmod a+x /run.sh

EXPOSE 9870
EXPOSE 8443

RUN curl https://dlcdn.apache.org/nifi/2.0.0-M2/nifi-2.0.0-M2-bin.zip --output nifi.zip
RUN unzip nifi.zip

RUN dos2unix /run.sh

CMD ["/run.sh"]
