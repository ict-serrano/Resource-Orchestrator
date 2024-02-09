import yaml
import time
import json
import logging
import requests

import requestType

from PyQt5.QtCore import QObject
from PyQt5.QtCore import pyqtSignal

from serrano_rot.api import clientInstance, clientEvents


logger = logging.getLogger("SERRANO.Orchestrator.ROTInterface")

logging.getLogger("pika").setLevel(logging.CRITICAL)


class ApplicationDeploymentDescription:

    def __init__(self, deployment_description, deployment_objectives):
        self.__application_description = []
        self.__deployment_objectives = deployment_objectives
        self.__parse_deployment_description(deployment_description)

    def __parse_deployment_description(self, deployment_description):

        microservices = []

        data = yaml.safe_load_all(deployment_description)
        for doc in data:
            if doc["kind"] in ["Deployment"]:
                d = {"kind": doc["kind"], "name": doc["metadata"]["name"], "replicas": doc["spec"]["replicas"]}
                microservices.append(d)

        self.__application_description = microservices

    def to_dict(self):
        return {"application_description": self.__application_description,
                "deployment_objectives": self.__deployment_objectives}


class ROTInterface(QObject):

    rotResponse = pyqtSignal(object)

    def __init__(self):
        super(QObject, self).__init__()

        self.__active_clusters = []
        self.__rot_request_description_per_execution_uuid = {}
        self.__dummy_cluster_index = 0

        self.client = clientInstance.ClientInstance()
        self.client.connect([clientEvents.EventExecutionCompleted, clientEvents.EventExecutionError],
                            self.__handle_rot_response)

    def update_available_clusters_info(self, clusters):
        self.__active_clusters = clusters

    def schedule_deployment(self, deployment):
        print("now rot ....")
        app_desc = ApplicationDeploymentDescription(deployment["deployment_description"],
                                                    deployment["deployment_objectives"])
        request_description = app_desc.to_dict()
        request_description["kind"] = deployment["kind"]
        request_description["active_clusters"] = self.__active_clusters
        res = self.client.post_execution("SimpleMatch", request_description)
        if not res:
            return False
        self.__rot_request_description_per_execution_uuid[res["execution_id"]] = deployment
        return True

    def schedule_storage_policy(self, storage_policy):

        request_params = {"kind": storage_policy["kind"], "policy_parameters": storage_policy["policy_parameters"]}

        res = self.client.post_execution("StoragePolicy", request_params)

        if not res:
            return False
        self.__rot_request_description_per_execution_uuid[res["execution_id"]] = storage_policy
        return True

    def schedule_kernel_deployment(self, description):
	pass

    def schedule_kernel_faas(self, description):


        try:

                rot_request = {"kind": description["kind"],
                               "kernel_name": description["kernel_name"],
                               "request_uuid": description["request_uuid"],
                               "active_clusters": self.__active_clusters,
                               "deployment_objectives": description["deployment_objectives"],
                               "data_description": description["data_description"]}

                if not self.client.post_execution("OnDemandKernel", rot_request):
                    return False
                else:
                    return True
    
        except Exception as e:
            print("Unable to execute the provided request.")


    def __handle_rot_response(self, evt):

        logger.info("Receive ROT response for execution uuid '%s'" % evt.execution_uuid)

  
        if evt.evt_type == "EventExecutionCompleted":
  
            data = json.loads(evt.results)

            if data["kind"] == requestType.SERRANO_DEPLOYMENT:

                data["deployment_request"] = self.__rot_request_description_per_execution_uuid[evt.execution_uuid]
                data["kind"] = data["deployment_request"]["kind"]

                self.rotResponse.emit(data)

                if evt.execution_uuid in self.__rot_request_description_per_execution_uuid:
                    del self.__rot_request_description_per_execution_uuid[evt.execution_uuid]

            elif data["kind"] == requestType.SERRANO_FaaS:
                self.rotResponse.emit(data)

            elif data["kind"] == requestType.SERRANO_STORAGE_POLICY:
                data["deployment_request"] = self.__rot_request_description_per_execution_uuid[evt.execution_uuid]
                data["kind"] = data["deployment_request"]["kind"]

                self.rotResponse.emit(data)

                if evt.execution_uuid in self.__rot_request_description_per_execution_uuid:
                    del self.__rot_request_description_per_execution_uuid[evt.execution_uuid]

        else:
            logger.error("Failure in ROT request for execution uuid '%s'" % evt.execution_uuid)
            logger.error("Status: %s - Reason: %s" % (evt.status, evt.reason))
            del self.__rot_request_description_per_execution_uuid[evt.execution_uuid]
