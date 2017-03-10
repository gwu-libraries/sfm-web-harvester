#!/bin/bash

# The late copying and chmoding below is dues to https://github.com/docker/docker/issues/1295
groupadd -r sfm --gid=$SFM_GID && useradd -r -g sfm --uid=$SFM_UID sfm

if [ ! -d "/opt/heritrix" ]; then
    echo "Moving heritrix"
    cp -r /tmp/heritrix /opt/heritrix
    chown -R sfm:sfm /opt/heritrix
    chmod -R u+x /opt/heritrix/bin
fi

gosu sfm /opt/heritrix/bin/heritrix --web-admin ${HERITRIX_USER}:${HERITRIX_PASSWORD} --web-bind-hosts 0.0.0.0 --jobs-dir /sfm-data/containers/$HOSTNAME/jobs
