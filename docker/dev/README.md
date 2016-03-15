# sfm-flickr-harvester dev docker container

A docker container for running sfm-web-harvester as a service.

In this container, the code is external to the container and shared with 
the container as a volume.

The code must be mounted as `/opt/sfm-web-harvester`. For example, 
`-v=~/my_code/sfm-web-harvester:/opt/sfm-web-harvester`.

This container also requires a link to a container running the queue. This
must be linked with the alias `mq`.  For example, `--link rabbitmq:mq`.

It also requires a link to a container running Heritrix. This must
be linked with the alias `heritrix`.