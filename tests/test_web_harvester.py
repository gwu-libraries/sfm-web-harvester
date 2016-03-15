from __future__ import absolute_import
import tests
import unittest
import shutil
import tempfile
import time
from datetime import datetime
from kombu import Connection, Exchange, Queue, Producer
from sfmutils.harvester import HarvestResult, EXCHANGE

@unittest.skipIf(not tests.integration_env_available, "Skipping test since integration env not available.")
class TestTwitterHarvesterIntegration(tests.TestCase):
    def _create_connection(self):
        return Connection(hostname="mq", userid=tests.mq_username, password=tests.mq_password)

    def setUp(self):
        self.exchange = Exchange(EXCHANGE, type="topic")
        self.result_queue = Queue(name="result_queue", routing_key="harvest.status.twitter.*", exchange=self.exchange,
                                  durable=True)
        # self.web_harvest_queue = Queue(name="web_harvest_queue", routing_key="harvest.start.web", exchange=self.exchange)
        self.warc_created_queue = Queue(name="warc_created_queue", routing_key="warc_created", exchange=self.exchange)
        web_harvester_queue = Queue(name="web_harvester", exchange=self.exchange)
        # twitter_rest_harvester_queue = Queue(name="twitter_rest_harvester", exchange=self.exchange)
        with self._create_connection() as connection:
            self.result_queue(connection).declare()
            self.result_queue(connection).purge()
            # self.web_harvest_queue(connection).declare()
            # self.web_harvest_queue(connection).purge()
            self.warc_created_queue(connection).declare()
            self.warc_created_queue(connection).purge()
            # Declaring to avoid race condition with harvester starting.
            web_harvester_queue(connection).declare()
            web_harvester_queue(connection).purge()
            # twitter_rest_harvester_queue(connection).declare()
            # twitter_rest_harvester_queue(connection).purge()

        self.collection_path = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.collection_path, ignore_errors=True)

    def test_harvest(self):
        harvest_msg = {
            "id": "test:1",
            "parent_id": "sfmui:45",
            "type": "web",
            "seeds": [
                {
                    "token": "http://www.gwu.edu/"
                },
                {
                    "token": "http://library.gwu.edu/"
                }
            ],
            "collection": {
                "id": "test_collection",
                "path": self.collection_path

            }
        }

        with self._create_connection() as connection:
            bound_exchange = self.exchange(connection)
            producer = Producer(connection, exchange=bound_exchange)
            producer.publish(harvest_msg, routing_key="harvest.start.web")

            # Now wait for result message.
            counter = 0
            bound_result_queue = self.result_queue(connection)
            message_obj = None
            while counter < 240 and not message_obj:
                time.sleep(.5)
                message_obj = bound_result_queue.get(no_ack=True)
                counter += 1
            self.assertTrue(message_obj, "Timed out waiting for result at {}.".format(datetime.now()))
            result_msg = message_obj.payload
            # Matching ids
            self.assertEqual("test:1", result_msg["id"])
            # Success
            self.assertEqual("completed success", result_msg["status"])
            # Some web resources
            self.assertEqual(2, result_msg["summary"]["web resources"])

            # Warc created message.
            # method_frame, header_frame, warc_created_body = self.channel.basic_get(self.warc_created_queue)
            bound_warc_created_queue = self.warc_created_queue(connection)
            message_obj = bound_warc_created_queue.get(no_ack=True)
            self.assertIsNotNone(message_obj, "No warc created message.")
