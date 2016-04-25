# sfm-web-harvester
A wrapper around Heritrix for harvesting web content as part of [Social Feed Manager](https://gwu-libraries.github.io/sfm-ui).

[![Build Status](https://travis-ci.org/gwu-libraries/sfm-web-harvester.svg?branch=master)](https://travis-ci.org/gwu-libraries/sfm-web-harvester)

## Installing
    git clone https://github.com/gwu-libraries/sfm-web-harvester.git
    cd sfm-web-harvester
    pip install -r requirements/requirements.txt

Note that `requirements/requirements.txt` references the latest release of sfm-utils.
If you are doing development on the interaction between sfm-utils and sfm-web-harvester,
use `requirements/dev.txt`. This uses a local copy of sfm-utils (`../sfm-utils`)
in editable mode.


## Running as a service
Web harvester will act on harvest start messages received from a queue. To run as a service:

    python web_harvester.py service <mq host> <mq username> <mq password> <heritrix url> <heritrix username> <heritrix password> <contact url>
    
## Process harvest start files
Web harvester can process harvest start files. The format of a harvest start file is the same as a harvest start message.  To run:

    python flickr_harvester.py seed <path to file> <heritrix url> <heritrix username> <heritrix password> <contact url>

## Integration tests (inside docker containers)
1. Install [Docker](https://docs.docker.com/installation/) and [Docker-Compose](https://docs.docker.com/compose/install/).
2. Start up the containers.

        docker-compose -f docker/dev.docker-compose.yml up -d

3. Run the tests.

        docker exec docker_sfmwebharvester_1 python -m unittest discover

4. Shutdown containers.

        docker-compose -f docker/dev.docker-compose.yml kill
        docker-compose -f docker/dev.docker-compose.yml rm -v --force
        

## Harvest start messages
See the [messaging specification](http://sfm.readthedocs.org/en/latest/messaging_spec.html#web-resource-harvest-start-message) for how to construct a harvest start message.
