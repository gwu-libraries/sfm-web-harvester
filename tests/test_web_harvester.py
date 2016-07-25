from __future__ import absolute_import
import tests
import unittest
import shutil
import tempfile
import time
from datetime import datetime, date
from kombu import Connection, Exchange, Queue, Producer
from sfmutils.harvester import HarvestResult, EXCHANGE
from mock import patch, call, MagicMock
from web_harvester import WebHarvester
import threading
import hapy
import os


class TestWebHarvester(tests.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    @patch("web_harvester.Hapy", autospec=True)
    def test_harvest(self, mock_hapy_cls):
        mock_hapy = MagicMock(spec=hapy.Hapy)
        mock_hapy_cls.side_effect = [mock_hapy]
        mock_hapy.get_job_info.return_value = {
            "job": {"availableActions": {"value": "build, launch, unpause"},
                    "crawlControllerState": "FINISHED",
                    "sizeTotalsReport": {"totalCount": "10"},
                    "uriTotalsReport": {"downloadedUriCount": 125, "totalUriCount": 125}}}

        harvester = WebHarvester("http://test", "test_username", "test_password", "http://library.gwu.edu",
                                 heritrix_data_path=self.temp_dir)
        harvester.harvest_result = HarvestResult()
        harvester.harvest_result_lock = threading.Lock()
        harvester.message = {
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
            "path": "/collections/test_collection_set",
            "collection_set": {
                "id": "test_collection_set"
            },
        }

        harvester.harvest_seeds()

        mock_hapy_cls.assert_called_once_with("http://test", username="test_username", password="test_password")
        # print mock_hapy.mock_calls
        self.assertEqual(call.teardown_job("sfm"), mock_hapy.mock_calls[0])
        self.assertEqual(call.create_job("sfm"), mock_hapy.mock_calls[1])
        self.assertEqual("submit_configuration", mock_hapy.mock_calls[2][0])
        self.assertEqual("sfm", mock_hapy.mock_calls[2][1][0])
        config = mock_hapy.mock_calls[2][1][1]
        self.assertTrue(config.startswith("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!--\n  HERITRIX 3 CRAWL JOB"))
        self.assertTrue("http://library.gwu.edu" in config)
        self.assertTrue(self.temp_dir in config)
        self.assertEqual(call.get_job_info('sfm'), mock_hapy.mock_calls[3])
        self.assertEqual(call.build_job('sfm'), mock_hapy.mock_calls[4])
        self.assertEqual(call.get_job_info('sfm'), mock_hapy.mock_calls[5])
        self.assertEqual(call.launch_job('sfm'), mock_hapy.mock_calls[6])
        self.assertEqual(call.get_job_info('sfm'), mock_hapy.mock_calls[7])
        self.assertEqual(call.unpause_job('sfm'), mock_hapy.mock_calls[8])
        self.assertEqual(call.get_job_info('sfm'), mock_hapy.mock_calls[9])
        self.assertEqual(call.terminate_job('sfm'), mock_hapy.mock_calls[10])
        self.assertEqual(call.get_job_info('sfm'), mock_hapy.mock_calls[11])

        # Check harvest result
        self.assertTrue(harvester.harvest_result.success)
        self.assertEqual(10, harvester.harvest_result.stats_summary()["web resources"])


@unittest.skipIf(not tests.integration_env_available, "Skipping test since integration env not available.")
class TestWebHarvesterIntegration(tests.TestCase):
    def _create_connection(self):
        return Connection(hostname="mq", userid=tests.mq_username, password=tests.mq_password)

    def setUp(self):
        self.exchange = Exchange(EXCHANGE, type="topic")
        self.result_queue = Queue(name="result_queue", routing_key="harvest.status.*", exchange=self.exchange,
                                  durable=True)
        self.warc_created_queue = Queue(name="warc_created_queue", routing_key="warc_created", exchange=self.exchange)
        web_harvester_queue = Queue(name="web_harvester", exchange=self.exchange)
        with self._create_connection() as connection:
            self.result_queue(connection).declare()
            self.result_queue(connection).purge()
            self.warc_created_queue(connection).declare()
            self.warc_created_queue(connection).purge()
            # Declaring to avoid race condition with harvester starting.
            web_harvester_queue(connection).declare()
            web_harvester_queue(connection).purge()

        self.path = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.path, ignore_errors=True)

    def test_harvest(self):
        harvest_msg = {
            "id": "test:1",
            "parent_id": "sfmui:45",
            "type": "web",
            "seeds": [
                {
                    "token": "http://gwu-libraries.github.io/sfm-ui/"
                },
            ],
            "path": self.path,
            "collection_set": {
                "id": "test_collection_set"

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
            self.assertEqual(18, result_msg["stats"][date.today().isoformat()]["web resources"])

            # Warc created message.
            bound_warc_created_queue = self.warc_created_queue(connection)
            message_obj = bound_warc_created_queue.get(no_ack=True)
            print message_obj
            self.assertIsNotNone(message_obj, "No warc created message.")
