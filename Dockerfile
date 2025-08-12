FROM apache/airflow:2.9.1-python3.10

USER root

# Install OpenJDK 17 for PySpark
RUN rm -f /etc/apt/sources.list.d/*mariadb* && \
    apt-get update && \
    apt-get install -y openjdk-17-jdk wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"
RUN java -version

# Create jars folder
RUN mkdir -p /opt/jars
COPY jars/mysql-connector-java.jar /opt/jars/

ENV SPARK_CLASSPATH=/opt/jars/mysql-connector-java.jar

USER airflow

COPY requirements.txt /
RUN pip install --no-cache-dir --upgrade -r /requirements.txt

COPY etl_scripts/ /opt/airflow/etl_scripts/
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/etl_scripts"
