# An image based on Alpine Linux and an unofficial Oracle JRE Image
FROM anapsix/alpine-java:8_server-jre

ENV TARGET_DIR=/opt/cubedb/

WORKDIR $TARGET_DIR

ADD /target/cubedb-*-SNAPSHOT.jar cubedb.jar
ADD /docker/run.sh run.sh

ENTRYPOINT ./run.sh

EXPOSE 80
