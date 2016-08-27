import logging
import unittest
import os
import socket


mq_port_available = True
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
try:
    s.connect(("mq", 5672))
except socket.error:
    mq_port_available = False

heritrix_port_available = True
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
try:
    s.connect(("heritrix", 8443))
except socket.error:
    heritrix_port_available = False


mq_username = os.environ.get("RABBITMQ_USER")
mq_password = os.environ.get("RABBITMQ_PASSWORD")
integration_env_available = mq_port_available and heritrix_port_available


class TestCase(unittest.TestCase):
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger("web_harvester").setLevel(logging.DEBUG)
