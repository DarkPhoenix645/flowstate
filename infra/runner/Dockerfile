FROM bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8 AS hadoop
FROM apache/spark:3.5.6

USER root
COPY --from=hadoop /opt/hadoop-3.2.1 /opt/hadoop-3.2.1
COPY server.py /opt/runner/server.py

ENV HADOOP_HOME=/opt/hadoop-3.2.1
ENV HADOOP_CONF_DIR=/etc/hadoop
ENV PATH="/opt/hadoop-3.2.1/bin:${PATH}"

EXPOSE 8091
CMD ["python3", "/opt/runner/server.py"]
