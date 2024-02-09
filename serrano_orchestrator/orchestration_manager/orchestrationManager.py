import copy
import uuid
import time
import json
import yaml
import logging
import requests

import entities
import requestType
import responseType
import rotInterface


from serrano_orchestrator.utils import status

from PyQt5.QtCore import QObject
from PyQt5.QtCore import pyqtSignal

logger = logging.getLogger("SERRANO.Orchestrator.OrchestrationManager")


class OrchestrationManager(QObject):

    orchestrationManagerUpdate = pyqtSignal(object)
    orchestrationManagerLogInfo = pyqtSignal(object)

    def __init__(self, config):
        super(QObject, self).__init__()
        self.__orchestrator_api_base_url = "%s/api/v1/orchestrator" % config["orchestrator"]["service"]

        self.__storage_gateway_service = config["secure_storage"]["service"]
        self.__storage_gateway_authorization_token = config["secure_storage"]["token"]

        self.__rotInterface = rotInterface.ROTInterface()
        self.__rotInterface.rotResponse.connect(self.__handle_rot_response)

        logger.info("OrchestrationManager service is ready ...")


    def __generate_bundles(self, deployment_uuid, deployment_description_request, rot_assignment_deployments,
                           rot_assignment_cluster_uuid, rot_instructions):
        uuids = []
        bundles = []

        per_group_docs = {}
        group_id_per_deployment = {}

        data = yaml.safe_load_all(deployment_description_request)
        for doc in data:
            group_id = doc["metadata"]["labels"]["group_id"]
            if doc["kind"] == "Deployment":
                group_id_per_deployment[doc["metadata"]["name"]] = group_id
                doc["spec"]["template"]["metadata"]["labels"]["serrano_deployment_uuid"] = deployment_uuid
                doc["spec"]["template"]["metadata"]["labels"]["group_id"] = group_id

                # Step 1: Add in the existing containers description the required environment variables
                containers = doc["spec"]["template"]["spec"]["containers"]

                environment_variables = [{"name": "DEPLOYED_SERRANO_CLUSTER_UUID", "value": rot_assignment_cluster_uuid},
                                         {"name": "SERRANO_DEPLOYMENT_UUID", "value": deployment_uuid }]

                for idx, container in enumerate(containers):
                    if "env" not in container:
                        doc["spec"]["template"]["spec"]["containers"][idx]["env"] = environment_variables
                    else:
                        doc["spec"]["template"]["spec"]["containers"][idx]["env"] += environment_variables

                # Step 2: Declarative scheduling preferences/instructions to k8s scheduler at cluster level
                for instruction in rot_instructions[doc["metadata"]["name"]]:
                    elements = instruction["yaml_element"].split(".")
                    target_element = elements[-1]
                    base_yaml_element = ".".join(elements[:-1])
                    if base_yaml_element == "spec":
                        doc["spec"][target_element] = instruction["value"]
                    elif base_yaml_element == "spec.template":
                        doc["spec"]["template"][target_element] = instruction["value"]
                    elif base_yaml_element == "spec.template.spec":
                        doc["spec"]["template"]["spec"][target_element] = instruction["value"]

            if group_id not in per_group_docs:
                per_group_docs[group_id] = [doc]
            else:
                per_group_docs[group_id].append(doc)

        for target_deployment in rot_assignment_deployments:
            group_id = group_id_per_deployment[target_deployment]
            bundle_description = per_group_docs[group_id]
            b = entities.Bundle(bundle_description)
            bundles.append(b)
            uuids.append(bundles[-1].uuid)

        return bundles, uuids

    def __get_clusters(self):
        clusters = []
        try:
            response = requests.get("%s/clusters?active=10m" % self.__orchestrator_api_base_url, verify=False)
            clusters = response.json()["clusters"]
        except Exception as err:
            logger.error(str(err))

        return clusters

    def __get_telemetry_entities(self):
        telemetry_entities = {}
        try:
            response = requests.get("%s/telemetry_entities" % self.__orchestrator_api_base_url, verify=False)
            telemetry_entities = response.json()
        except Exception as err:
            logger.error(str(err))
        return telemetry_entities

    def __get_cc_policy_id(self, policy_name):
        cc_policy_id = 0
        try:
            res = requests.get("%s/storage_policy" % self.__storage_gateway_service,
                               headers={"Authorization": "Bearer %s" % self.__storage_gateway_authorization_token})
            if res.status_code == 200:
                for policy in res.json():
                    if policy["name"] == policy_name:
                        return policy["id"]

        except Exception as err:
            logger.error(str(err))
        return cc_policy_id

    def __handle_rot_response(self, response):

        if not response:
            logger.error("Invalid ROT response (None), unable to serve the deployment request.")
            return None

        if "kind" not in response:
            logger.error("Invalid ROT response (not include kind parameter), unable to server the deployment request")

        if response["kind"] == requestType.SERRANO_KERNEL:
            self.__kernel_request_response(response)
        elif response["kind"] == requestType.SERRANO_FaaS:
            self.__faas_request_response(response)
        elif response["kind"] == requestType.SERRANO_STORAGE_POLICY:
            self.__storage_policy_request_response(response)
        elif response["kind"] == requestType.SERRANO_DEPLOYMENT:
            self.__deployment_request_response(response)

    def __storage_policy_request_response(self, response):

        storage_policy = responseType.StoragePolicy(response)
        self.orchestrationManagerUpdate.emit(storage_policy.to_dict())

        logs = [{"timestamp": int(time.time()), "event": "Request Storage Policy to Secure Storage Gateway"}]

        try:

            if storage_policy.cc_policy_id > 0:
                res = requests.put("%s/storage_policy" % self.__storage_gateway_service,
                                   headers={"Authorization": "Bearer %s" % self.__storage_gateway_authorization_token},
                                   json=storage_policy.format_secure_service_request(cc_policy_id=storage_policy.cc_policy_id))
            else:

                res = requests.post("%s/storage_policy" % self.__storage_gateway_service,
                                    headers={"Authorization": "Bearer %s" % self.__storage_gateway_authorization_token},
                                    json=storage_policy.format_secure_service_request())

            if res.status_code == 200 or res.status_code == 201:
                logs.append({"timestamp": int(time.time()), "event": "Storage Policy created successfully"})
                storage_policy_status = status.StoragePolicy.CREATED
                if storage_policy.cc_policy_id == 0:
                    storage_policy.cc_policy_id = self.__get_cc_policy_id(storage_policy.name)
                    self.orchestrationManagerUpdate.emit(storage_policy.to_dict())
            else:
                logs.append({"timestamp": int(time.time()), "event": "Unable to create requested Storage Policy"})
                logs.append({"timestamp": int(time.time()), "event": res.text})
                storage_policy_status = status.StoragePolicy.FAILED

        except Exception as e:
            logger.error("Unable to request the Storage Policy creation in Secure Storage Service Gateway")
            print("Unable to request the Storage Policy creation in Secure Storage Service Gateway")
            logger.error(str(e))
            logs.append({"timestamp": int(time.time()), "event": "Unable to create requested Storage Policy"})
            storage_policy_status = status.StoragePolicy.FAILED

        self.orchestrationManagerLogInfo.emit({"uuid": storage_policy.policy_uuid,
                                               "kind": storage_policy.kind,
                                               "status": storage_policy_status,
                                               "logs": logs})

    def __faas_request_response(self, response):

        hpc_kernel_bundle = {"kind": response["kind"], "request_uuid": response["request_uuid"],
                             "kernel_name": response["kernel_name"], "data_description": response["data_description"]}

        kernel_bundle = entities.Bundle(hpc_kernel_bundle)
        kernel_assignment = entities.Assignment(requestType.SERRANO_FaaS,
                                                response["cluster_uuid"],
                                                response["request_uuid"],
                                                [kernel_bundle.uuid])

        self.orchestrationManagerUpdate.emit({"kind": response["kind"],
                                              "request_uuid": response["request_uuid"],
                                              "decision": {"assignment": kernel_assignment, "bundle": kernel_bundle}})

    def __kernel_request_response(self, response):

        assignment = {"uuid": str(uuid.uuid4()), "cluster_uuid": response["cluster_uuid"],
                      "bundle_uuid": response["request_uuid"], "status": 0, "logs": [],
                      "updated_by": "Orchestration.Manager", "created_at": int(time.time()),
                      "updated_at": int(time.time())}

        bundle = {"uuid": response["request_uuid"], "description": [response], "status": 0, "logs": [],
                  "created_at": int(time.time()), "updated_at": int(time.time())}

        self.orchestrationManagerUpdate.emit({"deployment": {}, "assignments": [assignment], "bundles": [bundle]})

    def __deployment_request_response(self, response):

        monitoring_entity = {"clusters": []}

        deployment = response["deployment_request"]

        if len(response["assignments"]) == 0:
            self.orchestrationManagerLogInfo.emit({"uuid": deployment["deployment_uuid"],
                                                   "kind": requestType.SERRANO_DEPLOYMENT,
                                                   "status": status.Deployment.FAILED,
                                                   "logs": [{"timestamp": int(time.time()),
                                                             "event": "ROT provided invalid assignment"}]})
            return

        deployment_bundles = []
        deployment_assignments = []
        assignments_uuids = []

        for rot_assignment in response["assignments"]:
            assignment_bundles, bundles_uuids = self.__generate_bundles(deployment["deployment_uuid"],
                                                                        deployment["deployment_description"],
                                                                        rot_assignment["deployments"],
                                                                        rot_assignment["cluster_uuid"],
                                                                        response["instructions"])
            deployment_assignments.append(entities.Assignment(requestType.SERRANO_DEPLOYMENT,
                                                              rot_assignment["cluster_uuid"],
                                                              deployment["deployment_uuid"],
                                                              bundles_uuids))
            assignments_uuids.append(deployment_assignments[-1].uuid)
            deployment_bundles += assignment_bundles

            monitoring_entity["clusters"].append(rot_assignment["cluster_uuid"])
            monitoring_entity[rot_assignment["cluster_uuid"]] = []

        # Inform ETCD for the new assignments
        self.orchestrationManagerUpdate.emit({"kind": response["kind"],
                                              "deployment_uuid": deployment["deployment_uuid"],
                                              "decision": {"assignments_uuids": assignments_uuids,
                                                           "assignments": deployment_assignments,
                                                           "bundles": deployment_bundles},
                                              "monitoring": monitoring_entity})

    def handle_orchestrator_request(self, request):
        logger.info("Handle deployment request ...")
        logger.debug(json.dumps(request))

        clusters = self.__get_clusters()

        if len(clusters) == 0 and request["kind"] != requestType.SERRANO_STORAGE_POLICY:
            print("No available clusters, skip requests ...")
            logger.info("No available clusters, skip requests ...")
            return None

        self.__rotInterface.update_available_clusters_info(clusters)

        try:

            if request["kind"] == requestType.SERRANO_DEPLOYMENT:
                self.orchestrationManagerLogInfo.emit({"uuid": request["deployment_uuid"],
                                                       "kind": requestType.SERRANO_DEPLOYMENT,
                                                       "status": status.Deployment.PENDING,
                                                       "logs": [{"timestamp": int(time.time()),
                                                                 "event": "Request ROT scheduling"}]})
                if not self.__rotInterface.schedule_deployment(request):
                    self.orchestrationManagerLogInfo.emit({"uuid": request["deployment_uuid"],
                                                           "kind": requestType.SERRANO_DEPLOYMENT,
                                                           "status": status.Deployment.FAILED,
                                                           "logs": [{"timestamp": int(time.time()),
                                                                     "event": "Submission to ROT failed"}]})
            elif request["kind"] == requestType.SERRANO_KERNEL:
                self.__rotInterface.schedule_kernel_deployment(request)
            elif request["kind"] == requestType.SERRANO_FaaS:
                self.orchestrationManagerLogInfo.emit({"uuid": request["request_uuid"],
                                                       "kind": requestType.SERRANO_FaaS,
                                                       "status": status.Kernels.PENDING,
                                                       "logs": [{"timestamp": int(time.time()),
                                                                 "event": "Request ROT scheduling"}]})
                if not self.__rotInterface.schedule_kernel_faas(request):
                    self.orchestrationManagerLogInfo.emit({"uuid": request["request_uuid"],
                                                           "kind": requestType.SERRANO_FaaS,
                                                           "status": status.Kernels.FAILED,
                                                           "logs": [{"timestamp": int(time.time()),
                                                                     "event": "Submission to ROT failed"}]})
            elif request["kind"] == requestType.SERRANO_STORAGE_POLICY:
                self.orchestrationManagerLogInfo.emit({"uuid": request["policy_uuid"],
                                                       "kind": requestType.SERRANO_STORAGE_POLICY,
                                                       "status": status.StoragePolicy.PENDING,
                                                       "logs": [{"timestamp": int(time.time()),
                                                                 "event": "Request ROT decision"}]})
                if not self.__rotInterface.schedule_storage_policy(request):
                    self.orchestrationManagerLogInfo.emit({"uuid": request["policy_uuid"],
                                                           "kind": requestType.SERRANO_STORAGE_POLICY,
                                                           "status": status.StoragePolicy.FAILED,
                                                           "logs": [{"timestamp": int(time.time()),
                                                                     "event": "Submission to ROT failed"}]})
        except Exception as e:
            logger.error("Unable to handle orchestrator request.")
            logger.error(str(e))
