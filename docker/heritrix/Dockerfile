FROM java@sha256:ea18d1cf75d819b8e1b2b9907d671703b8139598829492c315b0305205a2d0f1
#openjdk-7
MAINTAINER Justin Littman <justinlittman@gwu.edu>

RUN apt-get update && apt-get install -y \
    zip=3.0-8 \
    maven=3.0.5-3
WORKDIR /tmp
ADD https://github.com/Landsbokasafn/heritrix3/archive/LBS-2015-01.zip /tmp/
RUN unzip LBS-2015-01.zip
WORKDIR /tmp/heritrix3-LBS-2015-01
RUN mvn package -Dmaven.test.skip=true
RUN unzip dist/target/heritrix-3.3.0-LBS-2015-01-dist.zip -d /tmp
RUN mv /tmp/heritrix-3.3.0-LBS-2015-01 /opt/heritrix
WORKDIR /opt/heritrix
VOLUME /heritrix-data
EXPOSE 8443
ENV FOREGROUND true
CMD /opt/heritrix/bin/heritrix --web-admin ${HERITRIX_USER}:${HERITRIX_PASSWORD} --web-bind-hosts 0.0.0.0 --jobs-dir /heritrix-data/jobs
