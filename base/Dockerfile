FROM ubuntu:latest

MAINTAINER Ivan Ermilov <ivan.s.ermilov@gmail.com>
MAINTAINER Giannis Mouchakis <gmouchakis@iit.demokritos.gr>

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      vim \
      sudo \
      unzip \
      openjdk-21-jdk \
      net-tools \
      curl \
      netcat-openbsd \
      gnupg \
      libsnappy-dev \
      openssh-server \
      dos2unix \
    && rm -rf /var/lib/apt/lists/*

RUN echo "JAVA_HOME=/usr/lib/jvm/java-1.21.0-openjdk-amd64" >> ~/.bashrc

RUN echo 'root:root' | chpasswd
RUN useradd -rm -d /home/bigd -s /bin/bash -g root -G sudo -u 1000 bigd
RUN echo 'bigd:bigd' | chpasswd

ENV JAVA_HOME=/usr/lib/jvm/java-1.21.0-openjdk-amd64/

USER bigd
RUN echo "JAVA_HOME=/usr/lib/jvm/java-1.21.0-openjdk-amd64" >> ~/.bashrc
ENV JAVA_HOME=/usr/lib/jvm/java-1.21.0-openjdk-amd64/
USER root

RUN curl -O https://dist.apache.org/repos/dist/release/hadoop/common/KEYS

RUN gpg --import KEYS

ENV HADOOP_VERSION 3.3.6
ENV HADOOP_URL https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz

RUN set -x \
    && curl -fSL "$HADOOP_URL" -o /tmp/hadoop.tar.gz \
    && curl -fSL "$HADOOP_URL.asc" -o /tmp/hadoop.tar.gz.asc \
    && gpg --verify /tmp/hadoop.tar.gz.asc \
    && tar -xvf /tmp/hadoop.tar.gz -C /opt/ \
    && rm /tmp/hadoop.tar.gz*

RUN ln -s /opt/hadoop-$HADOOP_VERSION/etc/hadoop /etc/hadoop

RUN mkdir /opt/hadoop-$HADOOP_VERSION/logs

RUN mkdir /hadoop-data

ENV HADOOP_HOME=/opt/hadoop-$HADOOP_VERSION
ENV HADOOP_CONF_DIR=/etc/hadoop
ENV MULTIHOMED_NETWORK=1
ENV USER=root
ENV PATH $HADOOP_HOME/bin/:$PATH

EXPOSE 22
RUN service ssh start

ADD entrypoint.sh /entrypoint.sh
RUN chmod a+x /entrypoint.sh
RUN dos2unix /entrypoint.sh

# spark
RUN curl https://dlcdn.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz --output spark.tgz
RUN sudo tar xvf spark.tgz

RUN mv spark-3.5.1-bin-hadoop3 /opt
RUN mv /opt/spark-3.5.1-bin-hadoop3 /opt/spark

RUN chmod -R 777 /opt/spark
RUN echo "export SPARK_HOME=/opt/spark" >> ~/.bashrc
RUN echo "export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin" >> ~/.bashrc

ENTRYPOINT ["/entrypoint.sh"]
