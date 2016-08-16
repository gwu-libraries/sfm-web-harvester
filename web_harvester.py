from sfmutils.harvester import BaseHarvester
from sfmutils.consumer import MqConfig, EXCHANGE
import logging
import argparse
import sys
from hapy import Hapy, HapyException
import codecs
import time
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import os

log = logging.getLogger(__name__)

QUEUE = "web_harvester"
ROUTING_KEY = "harvest.start.web"
JOB_NAME = "sfm"

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


class WebHarvester(BaseHarvester):
    def __init__(self, heritrix_url, heritrix_username, heritrix_password, contact_url,
                 heritrix_data_path="/sfm-data/heritrix-data", mq_config=None, debug=False):
        BaseHarvester.__init__(self, mq_config=mq_config, debug=debug,
                               use_warcprox=False)
        with open("crawler-beans.cxml", 'r') as f:
            heritrix_config = f.read()
        self.heritrix_config = heritrix_config.replace("HERITRIX_CONTACT_URL", contact_url).replace(
            "HERITRIX_DATA_PATH", heritrix_data_path)
        log.debug("Heritrix config is: %s", self.heritrix_config)
        self.client = Hapy(heritrix_url, username=heritrix_username, password=heritrix_password)
        # May want to set this for testing purposes.
        self.heritrix_data_path = heritrix_data_path

    def harvest_seeds(self):
        with codecs.open(os.path.join(self.heritrix_data_path, "seeds.txt"), "w", encoding="utf-8") as f:
            for url in [s["token"] for s in self.message["seeds"]]:
                f.write(url)
                f.write("\n")

        log.debug("Tearing down")
        try:
            self.client.teardown_job(JOB_NAME)
        except HapyException:
            # Job doesn't exist
            pass

        log.debug("Creating job")
        self.client.create_job(JOB_NAME)

        log.debug("Submitting configuration")
        self.client.submit_configuration(JOB_NAME, self.heritrix_config)
        wait_for(self.client, JOB_NAME, available_action='build')

        log.debug("Building job")
        self.client.build_job(JOB_NAME)
        wait_for(self.client, JOB_NAME, available_action='launch')

        log.debug("Launching")
        self.client.launch_job(JOB_NAME)
        wait_for(self.client, JOB_NAME, available_action='unpause')

        log.info("Unpausing")
        self.client.unpause_job(JOB_NAME)
        # Wait up to a day
        wait_for(self.client, JOB_NAME, controller_state="FINISHED", retries=60*60*24/30, sleep_secs=30)

        log.info("Terminating")
        self.client.terminate_job(JOB_NAME)

        log.info("Harvesting done")

        job_info = self.client.get_job_info(JOB_NAME)
        self.harvest_result.increment_stats("web resources",
                                            count=int(job_info["job"]["sizeTotalsReport"]["totalCount"]))

    def _create_warc_temp_dir(self):
        return os.path.join(self.heritrix_data_path, "jobs/sfm/latest/warcs")


def wait_for(h, job_name, available_action=None, controller_state=None, retries=60, sleep_secs=1):
    assert available_action or controller_state
    if available_action:
        log.debug("Waiting for available action %s", available_action)
    else:
        log.debug("Waiting for controller state %s", controller_state)
    count = 0
    while count <= retries:
        info = h.get_job_info(job_name)
        log.debug("Crawl controller state: %s. Available actions: %s. Downloaded %s of %s URIs.",
                  info['job'].get('crawlControllerState'),
                  info['job']['availableActions']['value'],
                  (info['job']['uriTotalsReport'] or {}).get('downloadedUriCount', 0),
                  (info['job']['uriTotalsReport'] or {}).get('totalUriCount', 0))
        if available_action and available_action in info['job']['availableActions']['value']:
            break
        elif controller_state and controller_state == info['job'].get('crawlControllerState'):
            break
        count += 1
        time.sleep(sleep_secs)
    if count == retries:
        raise Exception("Timed out waiting")

if __name__ == "__main__":

    # Logging
    logging.basicConfig(format='%(asctime)s: %(name)s --> %(message)s', level=logging.DEBUG)

    # Arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", type=lambda v: v.lower() in ("yes", "true", "t", "1"), nargs="?",
                        default="False", const="True")

    subparsers = parser.add_subparsers(dest="command")

    service_parser = subparsers.add_parser("service", help="Run harvesting service that consumes messages from "
                                                           "messaging queue.")
    service_parser.add_argument("host", help="The messaging queue host")
    service_parser.add_argument("username", help="The messaging queue username")
    service_parser.add_argument("password", help="The messaging queue password")
    service_parser.add_argument("heritrix_url", help="The url for Heritrix")
    service_parser.add_argument("heritrix_username", help="The username for Heritrix")
    service_parser.add_argument("heritrix_password", help="The password for Heritrix")
    service_parser.add_argument("contact_url", help="The contact URL to provide when harvesting")

    seed_parser = subparsers.add_parser("seed", help="Harvest based on a seed file.")
    seed_parser.add_argument("filepath", help="Filepath of the seed file.")
    seed_parser.add_argument("heritrix_url", help="The url for Heritrix")
    seed_parser.add_argument("heritrix_username", help="The username for Heritrix")
    seed_parser.add_argument("heritrix_password", help="The password for Heritrix")
    seed_parser.add_argument("contact_url", help="The contact URL to provide when harvesting")

    seed_parser.add_argument("--host", help="The messaging queue host")
    seed_parser.add_argument("--username", help="The messaging queue username")
    seed_parser.add_argument("--password", help="The messaging queue password")
    seed_parser.add_argument("--routing-key")

    args = parser.parse_args()

    # Logging
    logging.basicConfig(format='%(asctime)s: %(name)s --> %(message)s',
                        level=logging.DEBUG if args.debug else logging.INFO)
    logging.getLogger("requests").setLevel(logging.DEBUG if args.debug else logging.INFO)

    if args.command == "service":
        harvester = WebHarvester(args.heritrix_url, args.heritrix_username, args.heritrix_password, args.contact_url,
                                 mq_config=MqConfig(args.host, args.username, args.password, EXCHANGE,
                                                    {QUEUE: (ROUTING_KEY,)}))
        harvester.run()
    elif args.command == "seed":
        main_mq_config = MqConfig(args.host, args.username, args.password, EXCHANGE, None) \
            if args.host and args.username and args.password else None
        harvester = WebHarvester(args.heritrix_url, args.heritrix_username, args.heritrix_password, args.contact_url,
                                 mq_config=main_mq_config)
        harvester.harvest_from_file(args.filepath, routing_key=args.routing_key)
        if harvester.harvest_result:
            log.info("Result is: %s", harvester.harvest_result)
            sys.exit(0)
        else:
            log.warning("Result is: %s", harvester.harvest_result)
            sys.exit(1)
