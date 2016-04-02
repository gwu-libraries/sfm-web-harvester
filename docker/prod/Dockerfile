FROM python@sha256:ad39551743b356efda7c61f46019b97d49d1aab01b97f0e6d87c9b34326f3bfe
#Python 2.7
MAINTAINER Justin Littman <justinlittman@gwu.edu>

ENV sfm_harvester_version 0.0.0
RUN apt-get update && apt-get install -y \
    zip=3.0-8
#Upgrade pip
RUN pip install pip==7.1.2
WORKDIR /tmp
ADD https://github.com/gwu-libraries/sfm-web-harvester/archive/${sfm_harvester_version}.zip /tmp/
RUN unzip ${sfm_harvester_version}.zip
RUN mv sfm-web-harvester-${sfm_harvester_version} /opt/sfm-web-harvester
WORKDIR /opt/sfm-web-harvester
RUN pip install -r requirements/requirements.txt
RUN pip install appdeps
CMD appdeps.py --port-wait mq:5672 --port-wait heritrix:8443 \
    && python web_harvester.py --debug=$DEBUG service mq $MQ_ENV_RABBITMQ_DEFAULT_USER $MQ_ENV_RABBITMQ_DEFAULT_PASS https://heritrix:8443 $HERITRIX_ENV_HERITRIX_USER HERITRIX_ENV_HERITRIX_PASSWORD $HERITRIX_CONTACT_URL
