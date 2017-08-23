#!/bin/sh
DUMP_PATH="/tmp/cubedumps"
LOG_PROPERTIES="log4j.properties"
PORT=80
LOG_OPTS="-Dlog4j.configuration=$LOG_PROPERTIES"

/opt/jdk/bin/java -XX:MaxDirectMemorySize=10G -Xmx2000M  $LOG_OPTS -jar /opt/cubedb/cubedb.jar $PORT $DUMP_PATH
