FROM java@sha256:ea18d1cf75d819b8e1b2b9907d671703b8139598829492c315b0305205a2d0f1
#openjdk-7
MAINTAINER Justin Littman <justinlittman@gwu.edu>

ARG DEBIAN_FRONTEND=noninteractive
ENV lbs_version LBS-2016-02
RUN echo "deb http://ftp.debian.org/debian jessie-backports main" >> /etc/apt/sources.list
RUN apt-get update && apt-get install -y \
    zip=3.0-8 && \
    apt-get install -y -t jessie-backports maven=3.3.*
WORKDIR /tmp
ADD https://github.com/Landsbokasafn/heritrix3/archive/${lbs_version}.zip /tmp/
RUN unzip ${lbs_version}.zip
WORKDIR /tmp/heritrix3-${lbs_version}
RUN mvn package -Dmaven.test.skip=true
RUN unzip dist/target/heritrix-3.3.0-${lbs_version}-dist.zip -d /tmp
RUN mv /tmp/heritrix-3.3.0-${lbs_version} /opt/heritrix
WORKDIR /opt/heritrix
EXPOSE 8443
ENV FOREGROUND true
CMD /opt/heritrix/bin/heritrix --web-admin ${HERITRIX_USER}:${HERITRIX_PASSWORD} --web-bind-hosts 0.0.0.0 --jobs-dir /sfm-data/containers/$HOSTNAME/jobs
