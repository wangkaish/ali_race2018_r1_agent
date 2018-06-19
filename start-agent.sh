#!/bin/bash

ETCD_HOST=etcd
ETCD_PORT=2379
ETCD_URL=http://$ETCD_HOST:$ETCD_PORT

echo ETCD_URL = $ETCD_URL

if [[ "$1" == "consumer" ]]; then
  echo "Starting consumer agent..."
  java -jar \
       -Xms1536M \
       -Xmx1536M \
	   -XX:+UseConcMarkSweepGC \
       -XX:+UseParNewGC \
	   -XX:NewRatio=1 \
       -XX:+CMSParallelRemarkEnabled \
       -XX:CMSInitiatingOccupancyFraction=75 \
       -XX:+UseCMSInitiatingOccupancyOnly \
       -Dtype=consumer \
	   -Dthreads=2 \
	   -Dserver.host=consumer \
       -Dserver.port=20000 \
       -Detcd.url=$ETCD_URL \
       -Dlogs.dir=/root/logs \
       /root/dists/mesh-agent.jar
elif [[ "$1" == "provider-small" ]]; then
  echo "Starting small provider agent..."
  java -jar \
       -Xms768M \
       -Xmx768M \
	   -XX:+UseConcMarkSweepGC \
       -XX:+UseParNewGC \
	   -XX:NewRatio=1 \
       -XX:+CMSParallelRemarkEnabled \
       -XX:CMSInitiatingOccupancyFraction=75 \
       -XX:+UseCMSInitiatingOccupancyOnly \
       -Dtype=provider \
	   -Dscale.type=3 \
	   -Dthreads=200 \
	   -Dserver.host=provider-small \
       -Dserver.port=30000 \
       -Ddubbo.protocol.port=20880 \
       -Detcd.url=$ETCD_URL \
       -Dlogs.dir=/root/logs \
       /root/dists/mesh-agent.jar
elif [[ "$1" == "provider-medium" ]]; then
  echo "Starting medium provider agent..."
  java -jar \
       -Xms1024M \
       -Xmx1024M \
	   -XX:+UseConcMarkSweepGC \
       -XX:+UseParNewGC \
	   -XX:NewRatio=1 \
       -XX:+CMSParallelRemarkEnabled \
       -XX:CMSInitiatingOccupancyFraction=75 \
       -XX:+UseCMSInitiatingOccupancyOnly \
       -Dtype=provider \
	   -Dscale.type=2 \
	   -Dthreads=200 \
	   -Dserver.host=provider-medium \
       -Dserver.port=30000 \
       -Ddubbo.protocol.port=20880 \
       -Detcd.url=$ETCD_URL \
       -Dlogs.dir=/root/logs \
       /root/dists/mesh-agent.jar
elif [[ "$1" == "provider-large" ]]; then
  echo "Starting large provider agent..."
  java -jar \
       -Xms1536M \
       -Xmx1536M \
	   -XX:+UseConcMarkSweepGC \
       -XX:+UseParNewGC \
	   -XX:NewRatio=1 \
       -XX:+CMSParallelRemarkEnabled \
       -XX:CMSInitiatingOccupancyFraction=75 \
       -XX:+UseCMSInitiatingOccupancyOnly \
       -Dtype=provider \
	   -Dscale.type=1 \
	   -Dthreads=200 \
	   -Dserver.host=provider-large \
       -Dserver.port=30000 \
       -Ddubbo.protocol.port=20880 \
       -Detcd.url=$ETCD_URL \
       -Dlogs.dir=/root/logs \
       /root/dists/mesh-agent.jar
else
  echo "Unrecognized arguments, exit."
  exit 1
fi
