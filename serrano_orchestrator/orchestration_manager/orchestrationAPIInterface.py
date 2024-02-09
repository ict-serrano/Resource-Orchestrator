import time
import json
import etcd3
import logging

import entities
import requestType

from serrano_orchestrator.utils import status

from PyQt5.QtCore import QObject
from PyQt5.QtCore import pyqtSignal

logger = logging.getLogger("SERRANO.Orchestrator.OrchestrationAPIInterface")


class OrchestrationAPIInterface(QObject):
    orchestratorRequest = pyqtSignal(object)

    def __init__(self, config):
        super(QObject, self).__init__()

        self.config = config

        self.__etcdClient = etcd3.client(host=self.config["etcd"]["endpoints"][0], port=self.config["etcd"]["port"])
        self.__etcdClient.add_watch_prefix_callback("/serrano/orchestrator/deployments/deployment/",
                                                    self.__etcd_watch_callback)
        self.__etcdClient.add_watch_prefix_callback("/serrano/orchestrator/kernels/kernel/",
                                                    self.__etcd_watch_kernels_callback)
        self.__etcdClient.add_watch_prefix_callback("/serrano/orchestrator/storage_policies/policy/",
                                                    self.__etcd_watch_storage_policies_callback)
        logger.info("OrchestrationAPIInterface service is ready ...")

    def __etcd_watch_callback(self, etcd_event):
        logger.info("Deployment event(s) ...")
        for event in etcd_event.events:
            value = event.value.decode("utf-8")
            if len(value) == 0: # Delete event
                logger.info("Termination event for key '%s'" % event.value.decode("utf-8"))
                return
            else:
                event_data = json.loads(value)
                if event_data["updated_by"] == "Orchestration.API":
                    logger.info("Deployment event for key '%s'" % event.key.decode("utf-8"))
                    print("Deployment event for key '%s'" % event.key.decode("utf-8"))
                    self.orchestratorRequest.emit(event_data)

    def __etcd_watch_storage_policies_callback(self, etcd_event):
        logger.info("Storage Policy deployment event(s)")
        for event in etcd_event.events:
            value = event.value.decode("utf-8")
            if len(value) == 0: # Delete event, nothing here the OrchestratorAPI handles everything in this case
                return
            event_data = json.loads(event.value.decode("utf-8"))
            if event_data["updated_by"] == "Orchestration.API":
                logger.info("Storage Policy event for key '%s'" % event.key.decode("utf-8"))
                print("Storage policy event for key '%s'" % event.key.decode("utf-8"))
                self.orchestratorRequest.emit(event_data)

    def __etcd_watch_kernels_callback(self, etcd_event):
        logger.info("Kernel deployment event(s)")
        for event in etcd_event.events:
            value = event.value.decode("utf-8")
            if len(value) == 0:  # Delete event
                logger.info("Termination event for Kernel key '%s'" % event.value.decode("utf-8"))
                return
            else:
                event_data = json.loads(value)
                if event_data["updated_by"] == "Orchestration.API":
                    logger.info("Kernel execution event for key '%s'" % event.key.decode("utf-8"))
                    print("Kernel execution event for key '%s'" % event.key.decode("utf-8"))
                    self.orchestratorRequest.emit(event_data)

    def update_storage_policy(self, policy_uuid, **kwargs):
        decision = kwargs.get("decision", None)
        status = kwargs.get("status", None)
        log_evts = kwargs.get("logs", [])
        cc_policy_id = kwargs.get("cc_policy_id", 0)
        result, metadata = self.__etcdClient.get("/serrano/orchestrator/storage_policies/policy/%s" % policy_uuid)
        if result is not None:
            data = json.loads(result.decode("utf-8"))
            if decision is not None:
                data["decision"] = decision
            for l_evt in log_evts:
                data["logs"].append(l_evt)
            if status is not None:
                data["status"] = status
            if cc_policy_id > 0:
                data["cc_policy_id"] = cc_policy_id
            data["updated_by"] = "Orchestration.Manager"
            data["updated_at"] = int(time.time())
            self.__etcdClient.put("/serrano/orchestrator/storage_policies/policy/%s" % policy_uuid, json.dumps(data))

    def update_deployment(self, deployment_uuid, **kwargs):
        assignments = kwargs.get("assignments", None)
        assignments_status = kwargs.get("assignments_status", None)
        status = kwargs.get("status", None)
        logs = kwargs.get("logs", None)
        result, metadata = self.__etcdClient.get("/serrano/orchestrator/deployments/deployment/%s" % deployment_uuid)
        if result:
            data = json.loads(result.decode("utf-8"))
            if assignments:
                data["assignments"] = assignments
            if assignments_status:
                data["assignments_status"] = assignments_status
            if status:
                data["status"] = status
            if logs:
                for l_evt in logs:
                    data["logs"].append(l_evt)
            data["updated_by"] = "Orchestration.Manager"
            data["updated_at"] = int(time.time())
            self.__etcdClient.put("/serrano/orchestrator/deployments/deployment/%s" % deployment_uuid, json.dumps(data))

    def update_faas_kernel(self, request_uuid, **kwargs):
        assignment_uuid = kwargs.get("assignment_uuid", None)
        status = kwargs.get("status", None)
        logs = kwargs.get("logs", None)
        result, metadata = self.__etcdClient.get("/serrano/orchestrator/kernels/kernel/%s" % request_uuid)
        if result:
            data = json.loads(result.decode("utf-8"))
            if assignment_uuid:
                data["assignment_uuid"] = assignment_uuid
            if status:
                data["status"] = status
            if logs:
                for l_evt in logs:
                    data["logs"].append(l_evt)
            data["updated_by"] = "Orchestration.Manager"
            data["updated_at"] = int(time.time())
            self.__etcdClient.put("/serrano/orchestrator/kernels/kernel/%s" % request_uuid, json.dumps(data))

    def handle_orchestrator_manager_cmd(self, cmd):

        logger.debug(cmd)

        if cmd["kind"] == requestType.SERRANO_STORAGE_POLICY:
            self.update_storage_policy(cmd["policy_uuid"],
                                       decision=cmd["decision"],
                                       status=status.StoragePolicy.SCHEDULED,
                                       cc_policy_id=cmd["cc_policy_id"],
                                       logs=[{"timestamp": int(time.time()),
                                              "event": "Get decision response from ROT"}])

        if cmd["kind"] == requestType.SERRANO_DEPLOYMENT:

            logger.debug("Update Deployment '%s' in ETCD" % cmd["deployment_uuid"])

            # Create Monitoring entity
            self.__etcdClient.put("/serrano/orchestrator/monitoring/%s" % cmd["deployment_uuid"],
                                  json.dumps(cmd["monitoring"]))

            # Update Deployment entity
            self.update_deployment(cmd["deployment_uuid"],
                                   assignments=cmd["decision"]["assignments_uuids"],
                                   assignments_status=[status.Assignment.SCHEDULED]*len(cmd["decision"]["assignments_uuids"]),
                                   status=status.Deployment.ASSIGNED,
                                   logs=[{"timestamp": int(time.time()), "event": "Deployment assigned to clusters"}])

            # Create Bundle entities
            for bundle in cmd["decision"]["bundles"]:
                logger.debug("Create Bundle '%s' in ETCD" % bundle.uuid)
                self.__etcdClient.put("/serrano/orchestrator/bundles/bundle/%s" % bundle.uuid,
                                      json.dumps(bundle.to_dict()))

            # Final step create Assignment entities.
            for assignment in cmd["decision"]["assignments"]:
                logger.debug("Create Assignment '%s' for cluster '%s' in ETCD" % (assignment.uuid,
                                                                                  assignment.cluster_uuid))
                self.__etcdClient.put("/serrano/orchestrator/assignments/%s/assignment/%s" % (assignment.cluster_uuid,
                                                                                              assignment.uuid),
                                      json.dumps(assignment.to_dict()))

        if cmd["kind"] == requestType.SERRANO_FaaS:

            logger.debug("Update Kernel '%s' in ETCD" % cmd["request_uuid"])

            assignment = cmd["decision"]["assignment"]
            bundle = cmd["decision"]["bundle"]

            # Update Kernel entity
            self.update_faas_kernel(cmd["request_uuid"],
                                    assignment_uuid=assignment.uuid,
                                    status=status.Kernels.ASSIGNED,
                                    logs=[{"timestamp": int(time.time()),
                                           "event": "Kernel with selected deployment mode '%s' assigned to cluster: %s"
                                                    % (bundle.description["data_description"]["mode"],
                                                       assignment.cluster_uuid)}])

            # Create Bundle entity
            logger.debug("Create Bundle '%s' in ETCD" % bundle.uuid)
            self.__etcdClient.put("/serrano/orchestrator/bundles/bundle/%s" % bundle.uuid, json.dumps(bundle.to_dict()))

            # Final step create Assignment entity. 
            logger.debug("Create Assignment '%s' for cluster '%s' in ETCD" % (assignment.uuid, assignment.cluster_uuid))
            self.__etcdClient.put("/serrano/orchestrator/assignments/%s/assignment/%s" % (assignment.cluster_uuid,
                                                                                          assignment.uuid),
                                  json.dumps(assignment.to_dict()))

    def handle_orchestrator_manager_logs(self, cmd):

        if cmd["kind"] == requestType.SERRANO_STORAGE_POLICY:
            self.update_storage_policy(cmd["uuid"], status=cmd["status"], logs=cmd["logs"])
        if cmd["kind"] == requestType.SERRANO_DEPLOYMENT:
            self.update_deployment(cmd["uuid"], status=cmd["status"], logs=cmd["logs"])
        if cmd["kind"] == requestType.SERRANO_FaaS:
            self.update_faas_kernel(cmd["uuid"], status=cmd["status"], logs=cmd["logs"])
