# Builder container
FROM registry.cn-hangzhou.aliyuncs.com/aliware2018/services AS builder

COPY . /root/workspace/agent
WORKDIR /root/workspace/agent
RUN set -ex && mvn install:install-file -DgroupId=com.generallycloud -DartifactId=baseio-all -Dversion=3.2.5 -Dpackaging=jar -Dfile=lib/baseio-all-3.2.5.jar
RUN set -ex && mvn clean package -DskipTests

# Runner container
FROM registry.cn-hangzhou.aliyuncs.com/aliware2018/debian-jdk8

COPY --from=builder /root/workspace/services/mesh-provider/target/mesh-provider-1.0-SNAPSHOT.jar /root/dists/mesh-provider.jar
COPY --from=builder /root/workspace/services/mesh-consumer/target/mesh-consumer-1.0-SNAPSHOT.jar /root/dists/mesh-consumer.jar
COPY --from=builder /root/workspace/agent/target/mesh-agent-1.0-SNAPSHOT.jar /root/dists/mesh-agent.jar

COPY --from=builder /usr/local/bin/docker-entrypoint.sh /usr/local/bin
COPY start-agent.sh /usr/local/bin

RUN set -ex \
 && chmod a+x /usr/local/bin/start-agent.sh \
 && mkdir -p /root/logs

EXPOSE 8087

ENTRYPOINT ["docker-entrypoint.sh"]
