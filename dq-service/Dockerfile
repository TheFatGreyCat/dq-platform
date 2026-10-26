# Build context: thư mục gốc repo (docker-compose.yml: build.context = .)
# Stage 1: worker anomaly (thin jar + libs, không có Spark)
FROM maven:3.9-eclipse-temurin-17@sha256:1a352420f7aba21f5ad08df31bab55f74c013fb491f1ae8ab1dd7ff9ed698584 AS worker
WORKDIR /src
COPY jvm/ jvm/
RUN mvn -B -q -f jvm/pom.xml -pl dq-anomaly-worker -am -DskipTests package

# Stage 2: JRE 17 + Python 3.11 trong cùng namespace để worker nghe loopback thật
FROM eclipse-temurin:17-jre@sha256:e04fa72be13b257a59fdec6dc14af78c8f80f035d3c1ed4e03e45552a231d4c8
RUN apt-get update && apt-get install -y --no-install-recommends python3 python3-venv python3-pip \
    && rm -rf /var/lib/apt/lists/* \
    && python3 -m venv /opt/venv
ENV PATH=/opt/venv/bin:$PATH
WORKDIR /app
COPY dq-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY dq-service/*.py dq-service/start.sh ./
COPY --from=worker /src/jvm/dq-anomaly-worker/target/dq-anomaly-worker-0.1.0.jar /opt/dq-worker/
COPY --from=worker /src/jvm/dq-anomaly-worker/target/lib/ /opt/dq-worker/lib/
RUN chmod 0755 /app/start.sh
EXPOSE 8000
CMD ["/app/start.sh"]
