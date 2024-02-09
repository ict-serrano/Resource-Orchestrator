import json
import uuid
import etcd3
import os.path
import logging
import requests

from PyQt5.QtCore import QObject
from PyQt5.QtCore import pyqtSignal

logger = logging.getLogger("SERRANO.Orchestrator.OrchestrationDriver")


class OrchestrationDriver(QObject):

    orchestrationManagerDeploymentRequest = pyqtSignal(object)
    orchestrationManagerTerminationRequest = pyqtSignal(object)

    def __init__(self, cluster_uuid, config):
        super(QObject, self).__init__()

        self.__cluster_info = {}

        self.__config = config
        self.__basic_url = "%s/api/v1/orchestrator/clusters" % config["orchestrator"]["orchestrator_service"]
        self.__cluster_uuid = cluster_uuid
        self.__cluster_type = "k8s" if config["driver"] == "driverKubernetes" else "hpc"

        etcd = etcd3.client(host=self.__config["etcd"]["address"], port=self.__config["etcd"]["port"])
        etcd.add_watch_prefix_callback("/serrano/orchestrator/assignments/%s" % self.__cluster_uuid,
                                       self.__etcd_watch_callback)

    def set_cluster_info(self, info):
        self.__cluster_info = info
        self.__put_cluster_info()
        self.heartbeat()

    def __put_cluster_info(self):
        try:
            logger.debug("PUT cluster information at %s" % self.__basic_url)
            requests.put(self.__basic_url, json={"cluster_uuid": self.__cluster_uuid,
                                                 "type": self.__cluster_type,
                                                 "info": self.__cluster_info})
        except Exception as err:
            logger.error("Unable to post cluster information at %s" % self.__basic_url)
            logger.error("Traceback error: %s" % str(err))

    def __etcd_watch_callback(self, etcd_events):
        logger.info("Assignment event(s) ...")
        for event in etcd_events.events:
            if isinstance(event, etcd3.events.PutEvent):
                event_value = json.loads(event.value.decode("utf-8"))
                if event_value["updated_by"] == "Orchestration.Manager":
                    logger.info("Assignment event for key '%s'" % event.key.decode("utf-8"))
                    self.orchestrationManagerDeploymentRequest.emit(event_value)
            else:
                logger.info("Delete event for key '%s'" % event.key.decode("utf-8"))
                self.orchestrationManagerTerminationRequest.emit(event.key.decode("utf-8"))

    def heartbeat(self):
        try:
            logger.debug("POST heartbeat message for cluster '%s'" % self.__cluster_uuid)
            requests.get("%s/health/%s" % (self.__basic_url, self.__cluster_uuid))
        except Exception as err:
            logger.error("Unable to post cluster information at %s" % self.__basic_url)
            logger.error("Traceback error: %s" % str(err))

