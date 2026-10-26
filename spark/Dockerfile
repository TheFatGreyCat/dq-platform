# Build context: thư mục gốc repo (docker-compose.yml: build.context = .)
# Không dùng BuildKit cache mount: host chỉ có classic builder.
ARG HADOOP_VERSION

# ---------- Stage 1: thin jars (dq-client, dq-jobs, Deequ) + connector jars ----------
FROM maven:3.9-eclipse-temurin-17@sha256:1a352420f7aba21f5ad08df31bab55f74c013fb491f1ae8ab1dd7ff9ed698584 AS build
ARG HADOOP_VERSION
WORKDIR /src
COPY suites/ suites/
COPY jvm/ jvm/
RUN mvn -B -q -f jvm/pom.xml -DskipTests package
# Connector jars: hadoop-aws đúng version Hadoop của Spark image, aws-java-sdk-bundle theo POM của hadoop-aws
RUN set -eu; \
    [ -n "${HADOOP_VERSION}" ] || { echo "HADOOP_VERSION là bắt buộc" >&2; exit 1; }; \
    M=https://repo.maven.apache.org/maven2; mkdir -p /out; \
    curl -fsSL -o /out/hadoop-aws.pom "$M/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.pom"; \
    AWS=$(sed -n '/<artifactId>aws-java-sdk-bundle<\/artifactId>/,/<\/dependency>/s:.*<version>\(.*\)</version>.*:\1:p' /out/hadoop-aws.pom | head -n1); \
    if [ -z "$AWS" ] || [ "${AWS#\$}" != "$AWS" ]; then \
      AWS=$(sed -n 's:.*<aws-java-sdk.version>\(.*\)</aws-java-sdk.version>.*:\1:p' /out/hadoop-aws.pom | head -n1); fi; \
    if [ -z "$AWS" ]; then \
      P=$(sed -n 's:.*<artifactId>hadoop-project</artifactId>.*:x:p' /out/hadoop-aws.pom | head -n1); \
      curl -fsSL -o /out/project.pom "$M/org/apache/hadoop/hadoop-project/${HADOOP_VERSION}/hadoop-project-${HADOOP_VERSION}.pom"; \
      AWS=$(sed -n 's:.*<aws-java-sdk.version>\(.*\)</aws-java-sdk.version>.*:\1:p' /out/project.pom | head -n1); fi; \
    [ -n "$AWS" ] || { echo "không tìm thấy version aws-java-sdk-bundle" >&2; exit 1; }; \
    echo "$AWS" > /out/aws_sdk_version; \
    curl -fsSL -o /out/hadoop-aws-${HADOOP_VERSION}.jar "$M/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar"; \
    curl -fsSL -o /out/aws-java-sdk-bundle-$AWS.jar "$M/com/amazonaws/aws-java-sdk-bundle/$AWS/aws-java-sdk-bundle-$AWS.jar"; \
    curl -fsSL -o /out/delta-spark_2.12-3.2.0.jar "$M/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar"; \
    curl -fsSL -o /out/delta-storage-3.2.0.jar "$M/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar"

# ---------- Stage 2: image dùng chung cho driver + executor ----------
FROM apache/spark:3.5.1-scala2.12-java17-python3-ubuntu@sha256:a89ce4ad8abf112e369a74a930eb017da3bc87131855d185768e02ee2484124e
USER root
COPY --from=build /out/*.jar /opt/spark/jars/
COPY --from=build /out/aws_sdk_version /opt/dq/aws_sdk_version
COPY --from=build /src/jvm/dq-jobs/target/dq-jobs-0.1.0.jar /opt/dq/jars/dq-jobs-0.1.0.jar
COPY --from=build /src/jvm/dq-jobs/target/lib/ /opt/dq/jars/
# Đọc secret từ /run/secrets vào env của đúng tiến trình Spark (không qua conf/CLI/log)
COPY spark/with-s3-env.sh /opt/dq/bin/with-s3-env.sh
# k8s: conf mặc định nằm trong image (compose vẫn mount đè), entrypoint driver + launcher
COPY spark/conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY spark/k8s_submit.py scripts/run_dataset_job.py /opt/dq/bin/
RUN chmod 0755 /opt/dq/bin/with-s3-env.sh /opt/dq/bin/k8s_submit.py /opt/dq/bin/run_dataset_job.py \
    && mkdir -p /opt/dq/work && chown spark:spark /opt/dq/work
USER spark
# executor pod (k8s) chép jar/file vào thư mục làm việc: phải ghi được bởi user spark
WORKDIR /opt/dq/work
