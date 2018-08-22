#!/bin/sh
# env vars defined on Dockerfile

java -XX:MaxDirectMemorySize=10G -Xmx2000M  $LOG_OPTS -jar $TARGET_DIR/cubedb.jar $PORT $DUMP_PATH
