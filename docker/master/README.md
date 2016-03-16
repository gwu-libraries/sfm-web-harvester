# sfm-web-harvester master docker container

A docker container for running sfm-web-harvester as a service.

This container also requires a link to a container running the queue. This
must be linked with the alias `mq`.  For example, `--link rabbitmq:mq`.

It also requires a link to a container running Heritrix. This must
be linked with the alias `heritrix`.