import time
import json
import logging
import datetime
import requests

from PyQt5.QtCore import QThread
from confluent_kafka import Consumer

logger = logging.getLogger("SERRANO.Orchestrator.NotificationEngine")


class NotificationEngine(QThread):

    def __init__(self, notification_conf, service_endpoint):

        QThread.__init__(self)

        self.__kafka_conf = {"bootstrap.servers":  notification_conf["server"],
                             "group.id": notification_conf["group_id"],
                             "auto.offset.reset": "smallest"}
        self.__ede_topic = notification_conf["ede_topic"]
        self.__service_endpoint = service_endpoint

    def __del__(self):
        self.wait()

    def run(self):

        logger.info("Service is running ...")

        consumer = Consumer(self.__kafka_conf)
        consumer.subscribe([self.__ede_topic])

        while True:

            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue
            else:
                event_msg = msg.value().decode("utf-8").replace("\n", "")
                event_data = json.loads(event_msg)

                element = datetime.datetime.strptime(event_data["reporttimestamp"].split(".")[0], "%Y-%m-%dT%H:%M:%S")
                timestamp = datetime.datetime.timestamp(element)

                logger.info("Notification event from topic '%s'" % self.__ede_topic)
                logger.debug(event_msg)

                try:

                    requests.post("%s/api/v1/orchestrator/ede_notification" % self.__service_endpoint, json=event_data)

                except Exception as e:
                    logger.error("Unable to forward notification event from topic '%s" % self.__ede_topic)
                    logger.error(str(e))

