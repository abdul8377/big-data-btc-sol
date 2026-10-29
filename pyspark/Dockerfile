# Adaptado de 262bigdata/lambda26 commit 973ac3020b573a7c66fd925f7d07a9f2553411ec
FROM python:3.10-slim-trixie
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-21-jre-headless && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"
# Se conserva el entorno del curso y se agrega nbclient para verificacion por CLI.
RUN pip install --no-cache-dir pyspark==4.2.0 notebook==7.6.2 pandas==2.3.3 nbclient==0.10.2
WORKDIR /opt
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]
