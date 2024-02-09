import json
import time
import logging
import requests

from kubernetes import client

from serrano_orchestrator.utils import status
from serrano_orchestrator.utils import requestType

import k8s.KernelTemplate as kernelTemplate
import k8s.executionWrapper as ExecutionWrapper

import driverInterface

logger = logging.getLogger("SERRANO.Orchestrator.DriverKubernetes")


class DriverKubernetes(driverInterface.DriverInterface):

    def __init__(self, conf):
        super().__init__(conf)

        self.__cluster_uuid = conf["cluster_uuid"]

        self.__driver_k8s_conf = self.config["driver_k8s_conf"]
        self.__orchestrator_service = self.config["orchestrator"]["orchestrator_service"]
        self.__api_client()
        self.__k8s_deployments = self.__load_k8s_deployments()


    def __load_k8s_deployments(self):
        try:
            res = requests.get("%s/api/v1/orchestrator/deployments_monitoring?cluster_uuid=%s" % (self.__orchestrator_service,
                                                                                                  self.__cluster_uuid))
            return res.json()
        except Exception as e:
            print(str(e))
            logger.error(str(e))
            return {}

    def __api_client(self):
        api_configuration = client.Configuration()
        if self.__driver_k8s_conf.get("api_service", None):
            api_configuration.host = self.__driver_k8s_conf["api_service"]
        else:
            api_configuration.host = "https://%s:%s" % (self.__driver_k8s_conf["api_address"],
                                                        self.__driver_k8s_conf["api_port"])
        api_configuration.verify_ssl = False
        api_configuration.api_key = {"authorization": "Bearer " + self.__driver_k8s_conf["token"]}
        api_client = client.ApiClient(api_configuration)
        self.__api_core_client = client.CoreV1Api(api_client)
        self.__api_apps_client = client.AppsV1Api(api_client)

    def __apply_k8s_configMap(self, bundle_description, namespace):
        name = bundle_description["metadata"]["name"]
        try:
            res = self.__api_core_client.list_namespaced_config_map(namespace=namespace,
                                                                    field_selector=f"metadata.name={name}")
            if len(res.items) > 0:
                logger.debug("Update ConfigMap '%s'" % name)
                self.__api_core_client.replace_namespaced_config_map(body=bundle_description,
                                                                     name=name,
                                                                     namespace=namespace)
            else:
                logger.debug("Create ConfigMap '%s'" % name)
                self.__api_core_client.create_namespaced_config_map(body=bundle_description,
                                                                    namespace=namespace)
            configmap_status = True
        except Exception as e:
            logger.error(str(e))
            configmap_status = False
        return configmap_status

    # Persistent Volumes are not related with namespaces
    def __apply_k8s_persistentVolume(self, bundle_description):
        name = bundle_description["metadata"]["name"]
        try:
            res = self.__api_core_client.list_persistent_volume(field_selector=f"metadata.name={name}")
            if len(res.items) > 0:
                logger.debug("Update Persistent Volume '%s'" % name)
                self.__api_core_client.replace_persistent_volume(body=bundle_description,
                                                                 name=name)
            else:
                logger.debug("Create Persistent Volume '%s'" % name)
                self.__api_core_client.create_persistent_volume(body=bundle_description)
            pv_status = True
        except Exception as e:
            logger.error(str(e))
            pv_status = False

        return pv_status

    def __apply_k8s_persistentVolumeClaim(self, bundle_description, namespace):
        name = bundle_description["metadata"]["name"]
        try:
            res = self.__api_core_client.list_namespaced_persistent_volume_claim(namespace=namespace,
                                                                                 field_selector=f"metadata.name={name}")
           
            if len(res.items) == 0:
                logger.debug("Create Persistent Volume Claim '%s'" % name)
                self.__api_core_client.create_namespaced_persistent_volume_claim(body=bundle_description,
                                                                                 namespace=namespace)

            return True
        except Exception as e:
            logger.error(str(e))
            return False

    def __apply_k8s_deployment(self, bundle_description, namespace):
        name = bundle_description["metadata"]["name"]
        try:
            res = self.__api_apps_client.list_namespaced_deployment(namespace=namespace,
                                                                    field_selector=f"metadata.name={name}")
            data = {"k8s_deployment_name": name, "k8s_deployment_namespace": namespace}

            if len(res.items) > 0:
                logger.debug("Update Deployment '%s'", name)
                r = self.__api_apps_client.replace_namespaced_deployment(body=bundle_description, 
                                                                         name=name,
                                                                         namespace=namespace)
                data["k8s_deployment_uuid"] = r.metadata.uid
            else:
                logger.debug("Create Deployment '%s'", name)
                r = self.__api_apps_client.create_namespaced_deployment(namespace=namespace,
                                                                        body=bundle_description)
                data["k8s_deployment_uuid"] = r.metadata.uid

            return data

        except Exception as e:
            logger.error(str(e))
            return None

    def __post_log_data(self, data):
        logger.debug("__post_log_data")
        logger.debug(json.dumps(data))
        try:
            requests.post("%s/api/v1/orchestrator/logs" % self.__orchestrator_service, json={"logs": data})
        except Exception as e:
            logger.error("Unable to update Orchestrator API - POST log data")
            logger.error(str(e))

    def __post_metric_log_data(self, data):
        logger.debug("__post_metric_log_data")
        logger.debug(json.dumps(data))
        try:
            requests.post("%s/api/v1/orchestrator/metric_logs" % self.__orchestrator_service, json={"logs": data})
        except Exception as e:
            logger.error("Unable to update Orchestrator API - POST log data")
            logger.error(str(e))

    def __handle_update_log_status(self, data):
        try:
            if "metric_logs" in data and len(data["metric_logs"]) > 0:
                self.__post_metric_log_data(data["metric_logs"])
            if "logs" in data and len(data["logs"]) > 0:
                self.__post_log_data(data["logs"])
        except Exception as e:
            logger.error("Unable to update Orchestrator API - Handle update log status")
            logger.error(str(e))

    def __put_assignment_monitoring_data(self, data):
        try:
            requests.put("%s/api/v1/orchestrator/monitoring" % self.__orchestrator_service, json=data)
            logger.debug("POST Assignment monitoring data")
            logger.debug(json.dumps(data))
        except Exception as e:
            logger.error("Unable to update Orchestrator API - POST Assignment monitoring data")
            logger.error(str(e))

    # Main abstract method
    def __application_deployment_request(self, request):

        pending_bundles = len(request["bundles"])

        self.__post_log_data([{"kind": "Deployment", "uuid": request["deployment_uuid"],
                               "status": status.Deployment.IN_DEPLOYMENT, "cluster_uuid": self.__cluster_uuid,
                               "event": "Orchestrator Driver handles Deployments request",
                               "timestamp": int(time.time())}])

        namespace = "integration"
        logs = []
        self.__k8s_deployments[request["uuid"]] = []

        for bundle_uuid in request["bundles"]:
            bundle = self.get_bundle(bundle_uuid)
            bundle_pending_actions = len(bundle["description"])
            for bundle_description in bundle["description"]:
                if bundle_description["kind"] == "Deployment":
                    resp = self.__apply_k8s_deployment(bundle_description, namespace)
                    if resp:
                        logger.debug("Successful Deployment description for bundle '%s'" % bundle_uuid)
                        bundle_pending_actions = bundle_pending_actions - 1
                        resp["assignment_uuid"] = request["uuid"]
                        resp["bundle_uuid"] = bundle_uuid
                        self.__k8s_deployments[request["uuid"]].append(resp)
                    else:
                        logger.error("Unable to apply Deployment description for bundle '%s'" % bundle_uuid)
                elif bundle_description["kind"] == "ConfigMap":
                    if self.__apply_k8s_configMap(bundle_description, namespace):
                        logger.debug("Successful ConfigMap description for bundle '%s'" % bundle_uuid)
                        bundle_pending_actions = bundle_pending_actions - 1
                    else:
                        logger.error("Unable to apply ConfigMap description for bundle '%s'" % bundle_uuid)
                elif bundle_description["kind"] == "PersistentVolume":
                    if self.__apply_k8s_persistentVolume(bundle_description):
                        logger.debug("Successful PersistentVolume description for bundle '%s'" % bundle_uuid)
                        bundle_pending_actions = bundle_pending_actions - 1
                    else:
                        logger.error("Unable to apply PersistentVolume description for bundle '%s'" % bundle_uuid)
                elif bundle_description["kind"] == "PersistentVolumeClaim":
                    if self.__apply_k8s_persistentVolumeClaim(bundle_description, namespace):
                        logger.debug("Successful PersistentVolume description for bundle '%s'" % bundle_uuid)
                        bundle_pending_actions = bundle_pending_actions - 1
                    else:
                        logger.error("Unable to apply PersistentVolume description for bundle '%s'" % bundle_uuid)

            if bundle_pending_actions > 0:
                logs.append({"kind": "Bundle", "uuid": bundle_uuid, "status": status.Bundle.FAILED,
                             "cluster_uuid": self.__cluster_uuid,
                             "event": "Unable to successfully execute all Bundle descriptions",
                             "timestamp": int(time.time())})
            else:
                pending_bundles = pending_bundles - 1
                logs.append({"kind": "Bundle", "uuid": bundle_uuid, "status": status.Bundle.SUCCESSFUL,
                             "cluster_uuid": self.__cluster_uuid,
                             "event": "Successfully execute all Bundle descriptions",
                             "timestamp": int(time.time())})

        if pending_bundles > 0:
            logs.append({"kind": "Assignment", "uuid": request["uuid"], "status": status.Assignment.FAILED,
                         "event": "Assignment not executed successfully", "cluster_uuid": self.__cluster_uuid,
                         "timestamp": int(time.time())})
            logger.info("Unsuccessful deployment for Assignment '%s'" % request["uuid"])
        else:
            logs.append({"kind": "Assignment", "uuid": request["uuid"], "status": status.Assignment.DEPLOYED,
                         "event": "Assignment executed successfully", "cluster_uuid": self.__cluster_uuid,
                         "timestamp": int(time.time())})

            monitoring_data = {"deployment_uuid": request["deployment_uuid"],
                               "cluster_uuid": request["cluster_uuid"],
                               "assignment_uuid": request["uuid"],
                               "k8s_params": self.__k8s_deployments[request["uuid"]]}

            self.__put_assignment_monitoring_data(monitoring_data)

            logger.info("Deployment for assignment '%s' successfully executed" % request["uuid"])

        self.__post_log_data(logs)

    def __faas_kernel_deployment_request(self, request):

        self.__post_log_data([{"kind": "FaaS", "uuid": request["deployment_uuid"],
                               "cluster_uuid": request["cluster_uuid"],
                               "status": status.Kernels.IN_DEPLOYMENT,
                               "event": "Orchestrator Driver handles Kernel request",
                               "timestamp": int(time.time())}])

        for bundle_uuid in request["bundles"]:
            description = self.get_bundle(bundle_uuid)["description"]
            if description["kind"] == "FaaS":
                description["request_uuid"] = request["deployment_uuid"]
                description["cluster_uuid"] = request["cluster_uuid"]
                p = ExecutionWrapper.ExecutionWrapper(self.__driver_k8s_conf, description)
                p.updateLogStatus.connect(self.__handle_update_log_status)
                p.start()

        logger.info("Bundle for Faas kernel assignment '%s' is activated" % request["uuid"])

    # Main abstract method
    def handle_deployment_request(self, request):
        logger.info("Handler deployment request ...")
        logger.debug(json.dumps(request))

        if request["kind"] == requestType.SERRANO_DEPLOYMENT:
            self.__application_deployment_request(request)
        elif request["kind"] == requestType.SERRANO_FaaS:
            self.__faas_kernel_deployment_request(request)

    def handle_termination_request(self, event_key):
        try:
            assignment_uuid = event_key.split("/")[-1]
            if assignment_uuid in self.__k8s_deployments:
                for d in self.__k8s_deployments[assignment_uuid]:
                    del_options = client.V1DeleteOptions(propagation_policy="Foreground",
                                                         grace_period_seconds=5)

                    logger.info("Delete K8s Deployment '%s' in namespace '%s'" % (d["k8s_deployment_name"],
                                                                                  d["k8s_deployment_namespace"]))

                    self.__api_apps_client.delete_namespaced_deployment(name=d["k8s_deployment_name"],
                                                                        namespace=d["k8s_deployment_namespace"],
                                                                        body=del_options)
                del self.__k8s_deployments[assignment_uuid]

            logger.info("Termination request for Assignment '%s' successfully executed" % assignment_uuid)

        except Exception as e:
            logger.error("Termination request for event_key '%s' failed" % event_key)
            logger.error(str(e))

    def get_cluster_info(self):
        info = {"nodes": []}
        for node in self.__api_core_client.list_node().items:
            info["nodes"].append({"name": node.metadata.name, "labels": node.metadata.labels})
        return info
