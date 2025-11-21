FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

# 1. Instalar dependencias
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk \
    python3.10 \
    python3-pip \
    python3.10-venv \
    curl \
    wget \
    ca-certificates \
    tar \
 && ln -s /usr/bin/python3.10 /usr/bin/python \
 && python3 -m pip install --upgrade pip \
 && rm -rf /var/lib/apt/lists/*

# 2. Instalar Spark real (con Hadoop)
RUN wget https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz \
 && tar -xzf spark-3.5.1-bin-hadoop3.tgz -C /opt \
 && rm spark-3.5.1-bin-hadoop3.tgz

# 3. Variables de entorno correctas
ENV SPARK_HOME=/opt/spark-3.5.1-bin-hadoop3
ENV PATH="$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin"
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PYSPARK_PYTHON=python3

# 4. Instalar PySpark (cliente Python)
RUN python3 -m pip install pyspark==3.5.1

# 5. Workdir
WORKDIR /app

CMD ["bash"]