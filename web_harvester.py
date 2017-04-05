from sfmutils.harvester import BaseHarvester
from sfmutils.consumer import MqConfig, EXCHANGE
import logging
import argparse
import sys
from hapy import Hapy
import codecs
import time
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import os
import shutil

log = logging.getLogger(__name__)

QUEUE = "web_harvester"
ROUTING_KEY = "harvest.start.web"
JOB_NAME = "sfm"

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


class WebHarvester(BaseHarvester):
    def __init__(self, heritrix_url, heritrix_username, heritrix_password, contact_url,
                 working_path, mq_config=None, debug=False):
        BaseHarvester.__init__(self, working_path, mq_config=mq_config, debug=debug,
                               use_warcprox=False, tries=1)
        # Read crawl configuration file
        with open("crawler-beans.cxml", 'r') as f:
            heritrix_config = f.read()
        # Replace contact url and data path
        self.heritrix_config = heritrix_config.replace("HERITRIX_CONTACT_URL", contact_url).replace(
            "HERITRIX_DATA_PATH", working_path)
        log.debug("Heritrix config is: %s", self.heritrix_config)

        self.client = Hapy(heritrix_url, username=heritrix_username, password=heritrix_password)

        # Create a dummy job to allow looking up the directory that Heritrix is writing to.
        # This directory is in the Heritrix container's working path.
        self.client.create_job(JOB_NAME)
        self.jobs_dir = self._get_jobs_dir(JOB_NAME)
        log.debug("Jobs dir is %s", self.jobs_dir)

        self.job_dir = None
        self.job_state_dir = None
        self.job_name = None
        self.collection_path = None

    def harvest_seeds(self):
        # Write out the seed file
        with codecs.open(os.path.join(self.working_path, "seeds.txt"), "w", encoding="utf-8") as f:
            for url in [s["token"] for s in self.message["seeds"] if s["token"]]:
                f.write(url)
                f.write("\n")

        # The collection id is used as the job name
        self.job_name = self.message["collection"]["id"]

        # The job dir is where Heritrix reads/writes job data
        self.job_dir = os.path.join(self.jobs_dir, self.job_name)
        log.debug("Job dir is %s", self.job_dir)

        # The warc temp dir is where Heritrix writes WARC files.
        # The harvester will scan this dir for the WARC files.
        self.warc_temp_dir = os.path.join(self.job_dir, "latest/warcs")
        log.debug("Warc temp dir is %s", self.warc_temp_dir)

        # The job state dir is where the job data is persisted between harvests.
        # It's moved to the collection path so that it does not depend on the containers.
        # This also means that useful files such as crawl reports are kept.
        self.collection_path = self.message["path"]
        self.job_state_dir = os.path.join(self.collection_path, "heritrix_job")
        log.debug("Job state dir is %s", self.job_state_dir)

        # Copy job state dir if it exists
        if os.path.exists(self.job_state_dir):
            if os.path.exists(self.job_dir):
                shutil.rmtree(self.job_dir)
            log.debug("Copying job from %s to %s", self.job_state_dir, self.job_dir)
            shutil.copytree(self.job_state_dir, self.job_dir, symlinks=True)
        else:
            # Otherwise, create a new job
            log.info("Creating job")
            self.client.create_job(self.job_name)

        log.info("Submitting configuration")
        self.client.submit_configuration(self.job_name, self.heritrix_config)
        wait_for(self.client, self.job_name, available_action='build')

        log.info("Building job")
        self.client.build_job(self.job_name)
        wait_for(self.client, self.job_name, available_action='launch')

        log.info("Launching")
        self.client.launch_job(self.job_name)
        wait_for(self.client, self.job_name, available_action='unpause')

        log.info("Unpausing")
        self.client.unpause_job(self.job_name)

        # Wait up to a day
        wait_for(self.client, self.job_name, controller_state="FINISHED", retries=60*60*24/30, sleep_secs=30)

        log.info("Terminating")
        self.client.terminate_job(self.job_name)
        wait_for(self.client, self.job_name, available_action='teardown')

        # Get stats
        job_info = self.client.get_job_info(self.job_name)
        self.result.increment_stats("web resources", count=int(job_info["job"]["sizeTotalsReport"]["novelCount"]))

        self.client.teardown_job(self.job_name)
        wait_for(self.client, self.job_name, available_action='build')

        log.info("Harvesting done")

    def _get_jobs_dir(self, job_name):
        info = self.client.get_job_info(job_name)
        # "primaryConfig": "/sfm-data/containers/65a88d0e0cda/jobs/sfm/crawler-beans.cxml",
        return os.path.dirname(os.path.dirname(info['job']['primaryConfig']))

    def _finish_processing(self):
        BaseHarvester._finish_processing(self)
        # Move job from job_dir to job_state_dir
        if os.path.exists(self.job_state_dir):
            log.debug("Deleting job state directory")
            shutil.rmtree(self.job_state_dir)
        if not os.path.exists(self.collection_path):
            log.debug("Creating collection path")
            os.makedirs(self.collection_path)
        log.debug("Moving job from %s to %s", self.job_dir, self.job_state_dir)
        shutil.move(self.job_dir, self.job_state_dir)


def wait_for(h, job_name, available_action=None, controller_state=None, retries=60, sleep_secs=1):
    assert available_action or controller_state
    if available_action:
        log.debug("Waiting for available action %s", available_action)
    else:
        log.debug("Waiting for controller state %s", controller_state)
    count = 0
    while count <= retries:
        info = h.get_job_info(job_name)
        msg = "Crawl controller state: {}. Available actions: {}. Downloaded {} of {} URIs.".format(
            info['job'].get('crawlControllerState'),
            info['job']['availableActions']['value'],
            (info['job']['uriTotalsReport'] or {}).get('downloadedUriCount', 0),
            (info['job']['uriTotalsReport'] or {}).get('totalUriCount', 0))
        if not count % 10:
            log.info(msg)
        else:
            log.debug(msg)
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
    service_parser.add_argument("working_path")
    service_parser.add_argument("--skip-resume", action="store_true")

    seed_parser = subparsers.add_parser("seed", help="Harvest based on a seed file.")
    seed_parser.add_argument("filepath", help="Filepath of the seed file.")
    seed_parser.add_argument("heritrix_url", help="The url for Heritrix")
    seed_parser.add_argument("heritrix_username", help="The username for Heritrix")
    seed_parser.add_argument("heritrix_password", help="The password for Heritrix")
    seed_parser.add_argument("contact_url", help="The contact URL to provide when harvesting")
    seed_parser.add_argument("working_path")

    seed_parser.add_argument("--host", help="The messaging queue host")
    seed_parser.add_argument("--username", help="The messaging queue username")
    seed_parser.add_argument("--password", help="The messaging queue password")

    args = parser.parse_args()

    # Logging
    logging.getLogger().setLevel(logging.DEBUG if args.debug else logging.INFO)
    logging.getLogger("requests").setLevel(logging.DEBUG if args.debug else logging.WARNING)

    if args.command == "service":
        harvester = WebHarvester(args.heritrix_url, args.heritrix_username, args.heritrix_password, args.contact_url,
                                 args.working_path,
                                 mq_config=MqConfig(args.host, args.username, args.password, EXCHANGE,
                                                    {QUEUE: (ROUTING_KEY,)}))
        if not args.skip_resume:
            harvester.resume_from_file()
        harvester.run()
    elif args.command == "seed":
        main_mq_config = MqConfig(args.host, args.username, args.password, EXCHANGE, None) \
            if args.host and args.username and args.password else None
        harvester = WebHarvester(args.heritrix_url, args.heritrix_username, args.heritrix_password, args.contact_url,
                                 args.working_path, mq_config=main_mq_config)
        harvester.harvest_from_file(args.filepath)
        if harvester.harvest_result:
            log.info("Result is: %s", harvester.harvest_result)
            sys.exit(0)
        else:
            log.warning("Result is: %s", harvester.harvest_result)
            sys.exit(1)
