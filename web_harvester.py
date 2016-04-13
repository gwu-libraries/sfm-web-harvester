from sfmutils.harvester import HarvestResult, STATUS_SUCCESS, STATUS_FAILURE, STATUS_RUNNING
from sfmutils.consumer import BaseConsumer, MqConfig, EXCHANGE
import datetime
import logging
import json
import argparse
import sys
from hapy import Hapy, HapyException
import codecs
import shutil
import time
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import uuid
import hashlib
import os
import re
import threading

log = logging.getLogger(__name__)

QUEUE = "web_harvester"
ROUTING_KEY = "harvest.start.web"
JOB_NAME = "sfm"

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


class WebHarvester(BaseConsumer):
    def __init__(self, heritrix_url, heritrix_username, heritrix_password, contact_url, mq_config=None,
                 heritrix_data_path="/heritrix-data"):
        BaseConsumer.__init__(self, mq_config=mq_config)
        with open("crawler-beans.cxml", 'r') as f:
            heritrix_config = f.read()
        self.heritrix_config = heritrix_config.replace("HERITRIX_CONTACT_URL", contact_url).replace(
            "HERITRIX_DATA_PATH", heritrix_data_path)
        log.debug("Heritrix config is: %s", self.heritrix_config)
        self.client = Hapy(heritrix_url, username=heritrix_username, password=heritrix_password)
        self.harvest_result = None
        self.harvest_result_lock = None
        # May want to set this for testing purposes.
        self.heritrix_data_path = heritrix_data_path
        self.warc_temp_dir = os.path.join(self.heritrix_data_path, "jobs/sfm/latest/warcs")
        self.routing_key = None
        self.message = None

    def on_message(self):
        assert self.message

        log.info("Harvesting by message")

        self.harvest_result = HarvestResult()
        self.harvest_result_lock = threading.Lock()
        self.harvest_result.started = datetime.datetime.now()

        self.harvest_seeds()

        self.harvest_result.ended = datetime.datetime.now()
        self._process()

    def harvest_seeds(self):
        with codecs.open(os.path.join(self.heritrix_data_path, "seeds.txt"), "w") as f:
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
        self.harvest_result.increment_summary("web resources",
                                              increment=int(job_info["job"]["sizeTotalsReport"]["totalCount"]))

    # TODO: Refactor everything below
    def harvest_from_file(self, filepath, routing_key=None):
        """
        Performs a harvest based on the a harvest start message contained in the
        provided filepath.

        SIGTERM or SIGINT (Ctrl+C) will interrupt.

        :param filepath: filepath of the harvest start message
        :param routing_key: routing key of the harvest start message
        """
        log.debug("Harvesting from file %s", filepath)
        with codecs.open(filepath, "r") as f:
            self.message = json.load(f)

        self.routing_key = routing_key or ""

        self.on_message()
        return self.harvest_result

    def _process(self, done=True):
        harvest_id = self.message["id"]
        collection_id = self. message["collection"]["id"]
        collection_path = self.message["collection"]["path"]

        # Acquire a lock
        with self.harvest_result_lock:
            if self.harvest_result.success:
                # Send web harvest message
                self._send_web_harvest_message(harvest_id, collection_id,
                                               collection_path, self.harvest_result.urls_as_set())
                # Since the urls were sent, clear them
                if not done:
                    self.harvest_result.urls = []

                # Process warc files
                for warc_filename in self._list_warcs(self.warc_temp_dir):
                    # Move the warc
                    dest_warc_filepath = self._move_file(warc_filename,
                                                         self.warc_temp_dir,
                                                         self._path_for_warc(collection_path, warc_filename))
                    self.harvest_result.add_warc(dest_warc_filepath)
                    # Send warc created message
                    self._send_warc_created_message(harvest_id, collection_id, collection_path,
                                                    uuid.uuid4().hex, dest_warc_filepath)

                # TODO: Persist summary so that can resume

                if not self.harvest_result.success:
                    status = STATUS_FAILURE
                elif not done:
                    status = STATUS_RUNNING
                else:
                    status = STATUS_SUCCESS
                self._send_status_message(self.routing_key, harvest_id,
                                          self.harvest_result, status)
                if not done:
                    # Since these were sent, clear them.
                    self.harvest_result.errors = []
                    self.harvest_result.infos = []
                    self.harvest_result.warnings = []
                    self.harvest_result.token_updates = []
                    self.harvest_result.uids = []

    @staticmethod
    def _list_warcs(path):
        return [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f)) and
                (f.endswith(".warc") or f.endswith(".warc.gz"))]

    @staticmethod
    def _path_for_warc(collection_path, filename):
        m = re.search("-(\d{4})(\d{2})(\d{2})(\d{2})\d{7}-", filename)
        assert m
        return "/".join([collection_path, m.group(1), m.group(2), m.group(3), m.group(4)])

    @staticmethod
    def _move_file(filename, src_path, dest_path):
        src_filepath = os.path.join(src_path, filename)
        dest_filepath = os.path.join(dest_path, filename)
        log.debug("Moving %s to %s", src_filepath, dest_filepath)
        if not os.path.exists(dest_path):
            os.makedirs(dest_path)
        shutil.move(src_filepath, dest_filepath)
        return dest_filepath

    def _send_web_harvest_message(self, harvest_id, collection_id, collection_path, urls):
        message = {
            # TODO: Make this unique when multiple web harvest messages are sent.
            # This will be unique
            "id": "{}:{}".format(self.__class__.__name__, harvest_id),
            "parent_id": harvest_id,
            "type": "web",
            "seeds": [],
            "collection": {
                "id": collection_id,
                "path": collection_path
            }
        }
    # ADDED CHECK IF URLS
        if urls:
            for url in urls:
                message["seeds"].append({"token": url})

            self._publish_message("harvest.start.web", message)

    def _send_status_message(self, harvest_routing_key, harvest_id, harvest_result, status):
        # Just add additional info to job message
        message = {
            "id": harvest_id,
            "status": status,
            "infos": [msg.to_map() for msg in harvest_result.infos],
            "warnings": [msg.to_map() for msg in harvest_result.warnings],
            "errors": [msg.to_map() for msg in harvest_result.errors],
            "date_started": harvest_result.started.isoformat(),
            "summary": dict(harvest_result.summary),
            "token_updates": harvest_result.token_updates,
            "uids": harvest_result.uids,
            "warcs": {
                "count": len(harvest_result.warcs),
                "bytes": harvest_result.warc_bytes
            }
        }

        if harvest_result.ended:
            message["date_ended"] = harvest_result.ended.isoformat()

        # Routing key may be none
        status_routing_key = harvest_routing_key.replace("start", "status")
        self._publish_message(status_routing_key, message)

    def _send_warc_created_message(self, harvest_id, collection_id, collection_path, warc_id, warc_path):
        message = {
            "harvest": {
                "id": harvest_id
            },
            "collection": {
                "id": collection_id,
                "path": collection_path

            },
            "warc": {
                "id": warc_id,
                "path": warc_path,
                "date_created": datetime.datetime.fromtimestamp(os.path.getctime(warc_path)).isoformat(),
                "bytes": os.path.getsize(warc_path),
                "sha1": hashlib.sha1(open(warc_path).read()).hexdigest()
            }
        }
        self._publish_message("warc_created", message)

    # TODO: This is in BaseConsumer, but not in the version in master yet.
    def _publish_message(self, routing_key, message):
        message_body = json.dumps(message, indent=4)
        if self.mq_config:
            log.debug("Sending message to %s with routing_key %s. The body is: %s", self.exchange.name, routing_key,
                      json.dumps(message, indent=4))
            self.producer.publish(body=message,
                                  routing_key=routing_key,
                                  retry=True,
                                  exchange=self.exchange)
        else:
            log.debug("Skipping sending message to sfm_exchange with routing_key %s. The body is: %s",
                      routing_key, message_body)


def wait_for(h, job_name, available_action=None, controller_state=None, retries=60, sleep_secs=1):
    assert available_action or controller_state
    if available_action:
        log.debug("Waiting for available action %s", available_action)
    else:
        log.debug("Waiting for controller state %s", controller_state)
    count = 0
    while count <= retries:
        info = h.get_job_info(job_name)
        log.debug("Crawl controller state: %s. Available actions: %s.", info['job'].get('crawlControllerState'),
                  info['job']['availableActions']['value'])
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
