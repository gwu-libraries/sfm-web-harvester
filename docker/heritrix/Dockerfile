FROM java@sha256:ea18d1cf75d819b8e1b2b9907d671703b8139598829492c315b0305205a2d0f1
#openjdk-7
MAINTAINER Justin Littman <justinlittman@gwu.edu>

#IA build server is unreliable, so adding instead of downloading.
ADD heritrix-3.2.0-dist.tar.gz /tmp/
RUN ls /tmp
RUN mv /tmp/heritrix-3.2.0 /opt/heritrix
#RUN tar xvfz /tmp/heritrix-3.2.0-dist.tar.gz -C /opt/heritrix
WORKDIR /opt/heritrix
VOLUME /heritrix-data
EXPOSE 8443
ENV FOREGROUND true
CMD /opt/heritrix/bin/heritrix --web-admin ${HERITRIX_USER}:${HERITRIX_PASSWORD} --web-bind-hosts 0.0.0.0 --jobs-dir /heritrix-data/jobs