import json
import time
import etcd3
import logging
import requests
import traceback
from PyQt5.QtCore import QObject

from serrano_orchestrator.utils import status
from serrano_orchestrator.utils import requestType

logger = logging.getLogger("SERRANO.Orchestrator.Dispatcher")


class Dispatcher(QObject):

    def __init__(self, etcd_host, etcd_port, cth_service, ede_conf):

        super(QObject, self).__init__()

        self.__etcdClient = etcd3.client(host=etcd_host, port=etcd_port, grpc_options={
                        'grpc.max_send_message_length': 41943040,
                        'grpc.max_receive_message_length': 41943040,
                    }.items())
        self.__cth_service = cth_service
        self.__ede_service = ede_conf.get("service", "")
        self.__ede_username = ede_conf.get("username", "")
        self.__ede_password = ede_conf.get("password", "")
        self.__shape_value_threshold = ede_conf.get("shape_value_threshold", 0)

    def __extract_root_cause_worker_nodes(self, shape_values):
        affected_worker_nodes = []
        for k, v in shape_values.items():
            worker_node = k.split("_")[-1]
            if v >= self.__shape_value_threshold and worker_node not in affected_worker_nodes:
                affected_worker_nodes.append(worker_node)
        return affected_worker_nodes

    def __trigger_assignment_redeployment(self, affected_deployments, affected_worker_nodes):

        affected_bundles = []
        affected_bundle_descriptions = {}

        current_assignment = self.__get_assignment_by_uuid(affected_deployments[0]["assignment_uuid"])
        deployment_uuid = current_assignment["deployment_uuid"]
        current_deployment = self.get_deployments(deployment_uuid=deployment_uuid)[0]

        for dep in affected_deployments:
            affected_bundles.append(dep["bundle_uuid"])
            affected_bundle_descriptions[dep["bundle_uuid"]] = self.get_bundle(dep["bundle_uuid"])
        try:

            if len(affected_bundles) == len(current_assignment["bundles"]):

                self.delete_deployment(deployment_uuid)

                current_deployment["deployment_objectives"] = [{"affected_cluster_uuid": current_assignment["cluster_uuid"],
                                                                "affected_worker_nodes": affected_worker_nodes,
                                                                "affected_deployments": affected_deployments}]
                current_deployment["assignments"] = []
                current_deployment["assignments_status"] = [1]
                current_deployment["logs"].append({"timestamp": int(time.time()), "event": "Trigger Redeployment"})
                current_deployment["updated_by"] = "Orchestration.API"
                current_deployment["updated_at"] = int(time.time())

                self.__etcdClient.put("/serrano/orchestrator/deployments/deployment/%s" % deployment_uuid,
                                      json.dumps(current_deployment))

        except Exception as e:
            logger.error(str(e))
            return False

    def __get_assignment_cluster_uuid(self, assignment_uuid):
        cluster_uuid = None
        if len(assignment_uuid) == 0:
            return ""
        for result in self.__etcdClient.get_prefix("/serrano/orchestrator/assignments"):
            print(result[1].key.decode("utf-8"))
            print("assignment_uuid = '%s'" % assignment_uuid)
            if result[1].key.decode("utf-8").find(assignment_uuid) != -1:
                return result[1].key.decode("utf-8").split("/")[4]
        return cluster_uuid

    def __get_assignment_by_uuid(self, assignment_uuid):
        assignment = None
        for result in self.__etcdClient.get_prefix("/serrano/orchestrator/assignments"):
            if result[1].key.decode("utf-8").find(assignment_uuid) != -1:
                return json.loads(result[0].decode("utf-8"))
        return assignment

    def get_clusters(self, active):
        data = []
        last_seen_offset = 0
        t_m = {"m": 60, "h": 3600, "d": 86400}

        results = self.__etcdClient.get_prefix("/serrano/orchestrator/clusters")

        if active:
            last_seen_offset = (int(active[:-1])*(t_m[active[-1]]))

        for result in results:
            cluster = json.loads(result[0].decode("utf-8"))
            cluster["last_seen"] = 0
            cluster_id = result[1].key.decode("utf-8").split("cluster/")[1]
            health = self.__etcdClient.get("/serrano/orchestrator/health/clusters/%s" % cluster_id)
            if health[0] is not None:
                cluster["last_seen"] = health[0].decode("utf-8")
            cl = {"cluster_uuid": cluster["cluster_uuid"], "type": cluster["type"], "last_seen": cluster["last_seen"]}
            if not active:
                data.append(cl)
            else:
                if (int(cluster["last_seen"]) + last_seen_offset) >= int(time.time()):
                    data.append(cl)
        return data

    def get_cluster(self, cluster_uuid):
        data = {}
        result, metadata = self.__etcdClient.get("/serrano/orchestrator/clusters/cluster/%s" % cluster_uuid)
        if result is not None:
            data = json.loads(result.decode("utf-8"))
        return data

    def cluster_heartbeat(self, cluster_uuid):
        self.__etcdClient.put("/serrano/orchestrator/health/clusters/%s" % cluster_uuid, str(int(time.time())))

    def set_cluster(self, params):
        self.__etcdClient.put("/serrano/orchestrator/clusters/cluster/%s" % params["cluster_uuid"], json.dumps(params))

    def delete_cluster(self, cluster_uuid):
        self.__etcdClient.delete("/serrano/orchestrator/health/clusters/%s" % cluster_uuid)
        self.__etcdClient.delete("/serrano/orchestrator/clusters/cluster/%s" % cluster_uuid)
        return {}

    def get_deployments(self, **kwargs):
        data = []
        deployment_uuid = kwargs.get("deployment_uuid", None)

        if deployment_uuid:
            result, metadata = self.__etcdClient.get("/serrano/orchestrator/deployments/deployment/%s" % deployment_uuid)
            if result:
                data.append(json.loads(result.decode("utf-8")))
        else:
            results = self.__etcdClient.get_prefix("/serrano/orchestrator/deployments")

            for result in results:
                data.append(json.loads(result[0].decode("utf-8")))

        return data

    def get_deployment_logs(self, deployment_uuid):
        data = {}
        result, metadata = self.__etcdClient.get("/serrano/orchestrator/deployments/deployment/%s" % deployment_uuid)
        if result is not None:
            entity = json.loads(result.decode("utf-8"))
            data["deployment_uuid"] = deployment_uuid
            data["name"] = entity["name"]
            data["status"] = entity["status"]
            data["logs"] = entity["logs"]
            data["created_at"] = entity["created_at"]
            data["updated_at"] = entity["updated_at"]
        return data

    def get_deployment_services(self, deployment_uuid):
        data = {}
        deployment_status_str = ["UNKNOWN", "SUBMITTED", "PENDING", "SCHEDULED", "ASSIGNED", "IN_DEPLOYMENT","DEPLOYED",
                                 "FAILED", "REDEPLOYED", "TERMINATED"]
        bundle_status_str = ["UNKNOWN", "CREATED", "SUCCESSFUL", "FAILED", "TERMINATED"]

        result, metadata = self.__etcdClient.get("/serrano/orchestrator/deployments/deployment/%s" % deployment_uuid)
        if result is not None:
            entity = json.loads(result.decode("utf-8"))
            data["deployment_uuid"] = deployment_uuid
            data["name"] = entity["name"]
            data["status"] = entity["status"]
            data["status_str"] = deployment_status_str[entity["status"]]
            data["services"] = []
            for assignment_uuid in entity["assignments"]:
                assignment = self.__get_assignment_by_uuid(assignment_uuid)
                for bundle_uuid in assignment["bundles"]:
                    service = {"cluster_uuid": assignment["cluster_uuid"] }
                    bundle, metadata = self.__etcdClient.get("/serrano/orchestrator/bundles/bundle/%s" % bundle_uuid)
                    bundle = json.loads(bundle.decode("utf-8"))
                    for b in bundle["description"]:
                        if b["kind"] == "Deployment":
                            service["name"] = b["metadata"]["name"]
                            service["status"] = bundle["status"]
                            service["status_str"] = bundle_status_str[bundle["status"]]
                    data["services"].append(service)
            data["created_at"] = entity["created_at"]
            data["updated_at"] = entity["updated_at"]

        return data

    def get_kernel_logs(self, request_uuid):
        data = {}
        result, metadata = self.__etcdClient.get("/serrano/orchestrator/kernels/kernel/%s" % request_uuid)
        if result is not None:
            kernel = json.loads(result.decode("utf-8"))
            data["request_uuid"] = kernel["request_uuid"]
            data["status"] = kernel["status"]
            data["request_logs"] = kernel["logs"]
            data["assignment_logs"] = []
            data["bundle_logs"] = []
            data["created_at"] = kernel["created_at"]
            data["updated_at"] = kernel["updated_at"]

        assignment = self.__get_assignment_by_uuid(kernel["assignment_uuid"])
        if assignment:
            data["assignment_logs"] = assignment["logs"]
            data["bundle_logs"] = self.get_bundle(assignment["bundles"][0])["logs"]

        return data

    def get_all_kernels(self):
        return {}

    def get_all_faas(self):
        data = []
        try:
            results = self.__etcdClient.get_prefix("/serrano/orchestrator/kernels/kernel")
            for result in results:
                item = json.loads(result[0].decode("utf-8"))
                if item["kind"] == requestType.SERRANO_FaaS:
                    data.append(item["request_uuid"])
        except Exception as e:
            print(str(e))
        return data

    def get_kernel_logs(self, uuid):
        return {}

    def get_faas_logs(self, request_uuid):
        data = {}
        result, metadata = self.__etcdClient.get("/serrano/orchestrator/kernels/kernel/%s" % request_uuid)
        if result is not None:
            d = json.loads(result.decode("utf-8"))
            assignment = self.__get_assignment_by_uuid(d["assignment_uuid"])
            bundle = self.get_bundle(assignment["bundles"][0])
            data["request_uuid"] = d["request_uuid"]
            data["kernel_name"] = d["kernel_name"]
            data["request_logs"] = d["logs"]
            data["bundle_logs"] = bundle["logs"]
            data["cluster_uuid"] = assignment["cluster_uuid"]
            data["status"] = d["status"]
            data["created_at"] = d["created_at"]
            data["updated_at"] = d["updated_at"]
        return data

    def handle_notification_evt(self, event):

        serrano_deployments = {}

        try:

            logger.info("Handle Service Assurance notification event ...")

            for result in self.__etcdClient.get_prefix("/serrano/orchestrator/monitoring"):
                deployment_uuid = result[1].key.decode("utf-8").split("/")[-1]
                d = json.loads(result[0].decode())
               
            for anomaly in event["anomalies"]:
                affected_deployments = []
                affected_worker_nodes = self.__extract_root_cause_worker_nodes(anomaly["analysis"]["shap_values"])
                logger.info("Affected worker nodes: %s" % affected_worker_nodes)
                logger.info("Get details for the affected deployment(s) ... ")
                for wn in affected_worker_nodes:
                    logger.debug("Affected deployments in worker node '%s' => '%s'" % (wn, serrano_deployments[wn]))
                    for s_d in serrano_deployments[wn]:
                        affected_deployments.append(s_d)
                self.__trigger_assignment_redeployment(affected_deployments, affected_worker_nodes)

        except Exception as e:
            logger.error("Error while handling Service Assurance notification event ... ")
            logger.error(str(e))
            print(str(e))

    def create_deployment(self, params):

        params["deployment_description"] = params["deployment_description"].replace("\\r", "")
        params["assignments"] = []
        params["assignments_status"] = []
        params["logs"] = [{"timestamp": int(time.time()), "event": "Deployment description received."}]
        params["status"] = status.Deployment.SUBMITTED
        params["updated_by"] = "Orchestration.API"
        params["created_at"] = int(time.time())
        params["updated_at"] = int(time.time())

        self.__etcdClient.put("/serrano/orchestrator/deployments/deployment/%s" % params["deployment_uuid"],
                              json.dumps(params))

    def update_deployment(self, params):
	self.__etcdClient.put("/serrano/orchestrator/deployments/deployment/%s" % params["deployment_uuid"], json.dumps(params))

    def set_kernel_execution(self, params):

        params["assignment_uuid"] = ""
        params["logs"] = [{"timestamp": int(time.time()), "event": "Kernel description received."}]
        params["status"] = status.Kernels.SUBMITTED
        params["updated_by"] = "Orchestration.API"
        params["created_at"] = int(time.time())
        params["updated_at"] = int(time.time())

        self.__etcdClient.put("/serrano/orchestrator/kernels/kernel/%s" % params["request_uuid"], json.dumps(params))

    def delete_deployment(self, deployment_uuid):

        try:
            deps = self.get_deployments(deployment_uuid=deployment_uuid)
            if len(deps) != 1:
                return False

            for a_uuid in deps[0]["assignments"]:
                assignment = self.__get_assignment_by_uuid(a_uuid)
                for b_uuid in assignment["bundles"]:
                    self.__etcdClient.delete("/serrano/orchestrator/bundles/bundle/%s" % b_uuid)

                self.__etcdClient.delete("/serrano/orchestrator/assignments/%s/assignment/%s" % (assignment["cluster_uuid"],
                                                                                                 a_uuid))

            self.__etcdClient.delete("/serrano/orchestrator/deployments/deployment/%s" % deployment_uuid)
            self.__etcdClient.delete("/serrano/orchestrator/monitoring/%s" % deployment_uuid)
            requests.delete("%s/api/v1/telemetry/central/deployments/%s" % (self.__cth_service, deployment_uuid))

            return True

        except Exception as e:
            logger.error("Unable to delete deployment '%s'" % deployment_uuid)
            logger.error(str(e))
            return False

    def grafana_storage_policies(self, kwargs):
        data = []
        filtering_name = kwargs.get("filter", None)
        for result in self.__etcdClient.get_prefix("/serrano/orchestrator/storage_policies/policy"):
            d = json.loads(result[0].decode())
            if filtering_name and d["name"].find(filtering_name) == -1:
                continue
            data.append({"name": d["name"],
                         "policy_uuid": d["policy_uuid"],
                         "status": d["status"],
                         "decision": d["decision"],
                         "created_at": d["created_at"]})
        return data

    def grafana_storage_policies_logs(self, kwargs):
        data = []
        filtering_name = kwargs.get("filter", None)
        for result in self.__etcdClient.get_prefix("/serrano/orchestrator/storage_policies/policy"):
            d = json.loads(result[0].decode())
            if filtering_name and d["name"].find(filtering_name) == -1:
                continue
            for log in d["logs"]:
                data.append({"name": d["name"],
                             "policy_uuid": d["policy_uuid"],
                             "timestamp": log["timestamp"],
                             "event": log["event"]})
        return data

    def grafana_deployments(self, kwargs):
        data = []
        filtering_name = kwargs.get("filter", None)
        for result in self.__etcdClient.get_prefix("/serrano/orchestrator/deployments/deployment"):
            d = json.loads(result[0].decode())
            if filtering_name and d["name"].find(filtering_name) == -1:
                continue
            clusters = []
            for assignment_uuid in d["assignments"]:
                clusters.append(self.__get_assignment_cluster_uuid(assignment_uuid))
            data.append({"name": d["name"],
                         "deployment_uuid": d["deployment_uuid"],
                         "status": d["status"],
                         "assignments": d["assignments"],
                         "clusters": clusters,
                         "created_at": d["created_at"],
                         "updated_at": d["updated_at"]})
        return data

    def grafana_deployments_logs(self, kwargs):
        data = []
        filtering_name = kwargs.get("filter", None)
        for result in self.__etcdClient.get_prefix("/serrano/orchestrator/deployments/deployment"):
            d = json.loads(result[0].decode())
            if filtering_name and d["name"].find(filtering_name) == -1:
                continue
            for log in d["logs"]:
                data.append({"name": d["name"],
                             "deployment_uuid": d["deployment_uuid"],
                             "timestamp": log["timestamp"],
                             "event": log["event"]})
        return data

    def grafana_faas_kernels(self, kwargs):
        data = []
        for result in self.__etcdClient.get_prefix("/serrano/orchestrator/kernels/kernel"):
            d = json.loads(result[0].decode())
            data.append({"request_uuid": d["request_uuid"],
                         "kernel_name": d["kernel_name"],
                         "status": d["status"],
                         "data_description_uuid": d["data_description"]["uuid"],
                         "data_description_total_size_MB": d["data_description"]["total_size_MB"],
                         "assignment_uuid": d["assignment_uuid"],
                         "cluster_uuid": self.__get_assignment_cluster_uuid(d["assignment_uuid"]),
                         "created_at": d["created_at"],
                         "updated_at": d["updated_at"]})
        return data

    def grafana_faas_kernels_logs(self, kwargs):
        data = []
        for result in self.__etcdClient.get_prefix("/serrano/orchestrator/kernels/kernel"):
            d = json.loads(result[0].decode())
            for log in d["logs"]:
                data.append({"request_uuid": d["request_uuid"],
                             "kernel_name": d["kernel_name"],
                             "timestamp": log["timestamp"],
                             "event": log["event"]})
        return data

    def grafana_deployments_services(self):
        data = []
        for result in self.__etcdClient.get_prefix("/serrano/orchestrator/deployments/deployment"):
            d = json.loads(result[0].decode())
            for d_svc in self.get_deployment_services(d["deployment_uuid"])["services"]:
                entry = {"deployment_name": d["name"],
                         "deployment_uuid": d["deployment_uuid"],
                         "created_at": d["created_at"],
                         "updated_at": d["updated_at"],
                         "service_name": d_svc["name"],
                         "service_cluster_uuid": d_svc["cluster_uuid"],
                         "service_status": d_svc["status"]}
                data.append(entry)
        return data

    def get_storage_policy(self, **kwargs):
        data = []

        policy_uuid = kwargs.get("policy_uuid", None)

        if policy_uuid is not None:
            result, metadata = self.__etcdClient.get("/serrano/orchestrator/storage_policies/policy/%s" % policy_uuid)
            if result is not None:
                data.append(json.loads(result.decode("utf-8")))
        else:
            results = self.__etcdClient.get_prefix("/serrano/orchestrator/storage_policies/policy")
            for result in results:
                data.append(json.loads(result[0].decode("utf-8")))

        return data

    def delete_storage_policy(self, policy_uuid):
        return self.__etcdClient.delete("/serrano/orchestrator/storage_policies/policy/%s" % policy_uuid)

    def create_storage_policy(self, params):
        params["decision"] = {}
        params["cc_policy_id"] = 0
        params["status"] = status.StoragePolicy.SUBMITTED
        params["logs"] = [{"timestamp": int(time.time()), "event": "Storage Policy description received."}]
        params["updated_by"] = "Orchestration.API"
        params["created_at"] = int(time.time())
        params["updated_at"] = int(time.time())

        self.__etcdClient.put("/serrano/orchestrator/storage_policies/policy/%s" % params["policy_uuid"],
                              json.dumps(params))

    def update_storage_policy(self, params):
        result, metadata = self.__etcdClient.get("/serrano/orchestrator/storage_policies/policy/%s" %
                                                 params["policy_uuid"])
        if result is not None:
            entity = json.loads(result.decode("utf-8"))

            params["decision"] = {}
            params["cc_policy_id"] = entity["cc_policy_id"]
            params["status"] = status.StoragePolicy.SUBMITTED
            params["logs"] = entity["logs"]
            params["logs"].append({"timestamp": int(time.time()),
                                   "event": "Updated Storage Policy description received."})
            params["updated_by"] = "Orchestration.API"
            params["created_at"] = entity["created_at"]
            params["updated_at"] = int(time.time())

            self.__etcdClient.put("/serrano/orchestrator/storage_policies/policy/%s" % params["policy_uuid"],
                                  json.dumps(params))
            return True

        return False

    def get_assignment(self, cluster_uuid, assignment_uuid):
        data = {}
        result, metadata = self.__etcdClient.get("/serrano/orchestrator/assignments/%s/assignment/%s" % (cluster_uuid,
                                                                                                         assignment_uuid))
        if result is not None:
            data = json.loads(result.decode("utf-8"))
        return data

    def get_bundle(self, bundle_uuid):
        data = {}
        result, metadata = self.__etcdClient.get("/serrano/orchestrator/bundles/bundle/%s" % bundle_uuid)
        if result is not None:
            data = json.loads(result.decode("utf-8"))
        return data

    def get_kernel(self, request_uuid):
        data = {}
        result, metadata = self.__etcdClient.get("/serrano/orchestrator/kernels/kernel/%s" % request_uuid)
        if result is not None:
            data = json.loads(result.decode("utf-8"))
        return data

    def __enable_deployment_monitoring(self, deployment_uuid):
        try:
            result, metadata = self.__etcdClient.get("/serrano/orchestrator/monitoring/%s" % deployment_uuid)
            if not result:
                return
            entity = json.loads(result.decode("utf-8"))
            entity["deployment_uuid"] = deployment_uuid
            entity["timestamp"] = int(time.time())
            res = requests.post("%s/api/v1/telemetry/central/deployments" % self.__cth_service, json=entity)
        except Exception as e:
            print(str(e))
            logger.error(str(e))

    def __update_ede_with_deployment(self, deployment_uuid):
        try:
            logger.info("Inform EDE for the new deployment '%s' " % deployment_uuid)
            requests.put("%s/v1/config" % self.__ede_service, json=data)
        except Exception as e:
            print(str(e))
            logger.error(str(e))

    def __update_kernel_request_status(self, entity, assignment_status):

        try:

            if assignment_status == status.Assignment.FAILED:
                entity["status"] = status.Kernels.FAILED
                entity["logs"].append({"timestamp": int(time.time()), "event": "Related Assignment failed"})
            elif assignment_status == status.Assignment.DEPLOYED:
                entity["status"] = status.Kernels.FINISHED
                entity["logs"].append({"timestamp": int(time.time()), "event": "Kernel executed successfully"})
            else:
                return

            entity["updated_by"] = "Orchestration.Driver"
            entity["updated_at"] = int(time.time())
            self.__etcdClient.put("/serrano/orchestrator/kernels/kernel/%s" % entity["request_uuid"],
                                  json.dumps(entity))

        except Exception as e:
            print(str(e))
            logger.error(str(e))

    def __update_deployment_status(self, deployment_uuid, assignment_uuid, assignment_status):

        try:
            entity = self.get_deployments(deployment_uuid=deployment_uuid)[0]

            # Update the Assignment status in the Deployment Object
            a_i = entity["assignments"].index(assignment_uuid)
            entity["assignments_status"][a_i] = assignment_status

            if assignment_status == status.Assignment.FAILED:
                entity["status"] = status.Deployment.FAILED
                entity["logs"].append({"timestamp": int(time.time()), "event": "Assignment '%s' failed" % assignment_uuid})
                entity["updated_by"] = "Orchestration.Driver"
                entity["updated_at"] = int(time.time())
                self.__etcdClient.put("/serrano/orchestrator/deployments/deployment/%s" % deployment_uuid,
                                      json.dumps(entity))

            if entity["status"] != status.Deployment.FAILED and assignment_status == status.Assignment.DEPLOYED:

                # If not all Assignments are executed successfully, then just store the object with the updated
                # assignment_status field
                if entity["assignments_status"].count(status.Assignment.DEPLOYED) != len(entity["assignments_status"]):
                    logger.debug("Update deployment status after assignment progress update ..")
                    entity["updated_by"] = "Orchestration.Driver"
                    entity["updated_at"] = int(time.time())
                    self.__etcdClient.put("/serrano/orchestrator/deployments/deployment/%s" % deployment_uuid,
                                          json.dumps(entity))
                # All Assignments are executed successfully, then update the Deployment overall status
                # and trigger its monitoring
                else:
                    entity["status"] = status.Deployment.DEPLOYED
                    entity["logs"].append({"timestamp": int(time.time()), "event": "Deployment executed successfully"})
                    entity["updated_by"] = "Orchestration.Driver"
                    entity["updated_at"] = int(time.time())
                    
                    self.__etcdClient.put("/serrano/orchestrator/deployments/deployment/%s" % deployment_uuid,
                                          json.dumps(entity))
                    logger.debug("All deployment's assignments were successful")
                    logger.debug("Now enable monitoring and service assurance for Deployment %s" % deployment_uuid)
                    
                    self.__enable_deployment_monitoring(deployment_uuid)
                    self.__update_ede_with_deployment(deployment_uuid)
        except Exception as e:
            print(str(e))
            logger.error(str(e))

    def add_entities_logs(self, log_data):

        try:
            for data in log_data["logs"]:
                if data["kind"] == "Deployment":
                    entity = self.get_deployments(deployment_uuid=data["uuid"])[0]
                    entity["status"] = data["status"]
                    entity["logs"].append({"timestamp": data["timestamp"], "event": data["event"]})
                    entity["updated_by"] = "Orchestration.Driver"
                    entity["updated_at"] = int(time.time())
                    self.__etcdClient.put("/serrano/orchestrator/deployments/deployment/%s" % data["uuid"],
                                          json.dumps(entity))
                elif data["kind"] == "Assignment":
                    entity = self.__get_assignment_by_uuid(data["uuid"])
                    entity["status"] = data["status"]
                    entity["logs"].append({"timestamp": data["timestamp"], "event": data["event"]})
                    entity["updated_by"] = "Orchestration.Driver"
                    entity["updated_at"] = int(time.time())
                    self.__etcdClient.put("/serrano/orchestrator/assignments/%s/assignment/%s" % (entity["cluster_uuid"],
                                                                                                  data["uuid"]),
                                          json.dumps(entity))

                    if data["status"] in [status.Assignment.FAILED, status.Assignment.DEPLOYED]:
                        kernel = self.get_kernel(entity["deployment_uuid"])
                        if kernel:
                            self.__update_kernel_request_status(kernel, data["status"])
                        else:
                            self.__update_deployment_status(entity["deployment_uuid"], data["uuid"], data["status"])

                elif data["kind"] == "Bundle":
                    entity = self.get_bundle(data["uuid"])
                    entity["status"] = data["status"]
                    entity["logs"].append({"timestamp": data["timestamp"], "event": data["event"]})
                    entity["updated_by"] = "Orchestration.Driver"
                    entity["updated_at"] = int(time.time())
                    self.__etcdClient.put("/serrano/orchestrator/bundles/bundle/%s" % data["uuid"], json.dumps(entity))

                elif data["kind"] == "FaaS":

                    entity = self.get_kernel(data["uuid"])
                    assignment = self.get_assignment(data["cluster_uuid"], entity["assignment_uuid"])

                    if assignment:
                        description = {"deployment_mode": "FaaS", "cluster_uuid": data["cluster_uuid"]}
                        bundle = self.get_bundle(assignment["bundles"][0])

                        if data["status"] == status.Kernels.IN_DEPLOYMENT:
                            description["counter_diff"] = 1
                        elif data["status"] in [status.Kernels.FINISHED, status.Kernels.FAILED]:
                            description["counter_diff"] = -1
                        else:
                            description["counter_diff"] = 0

                        description["kernel_mode"] = bundle["description"]["data_description"]["mode"]

                        requests.put("%s/api/v1/telemetry/central/serrano_kernel_deployments" % self.__cth_service,
                                     json=description)

                    entity["status"] = data["status"]
                    entity["logs"].append({"timestamp": data["timestamp"], "event": data["event"]})
                    entity["updated_by"] = "Orchestration.Driver"
                    entity["updated_at"] = int(time.time())
                    self.__etcdClient.put("/serrano/orchestrator/kernels/kernel/%s" % data["uuid"], json.dumps(entity))

        except Exception as e:
            print(str(e))
            logger.error(str(e))

    def put_assignment_monitoring_data(self, data):
        try:
            result, metadata = self.__etcdClient.get("/serrano/orchestrator/monitoring/%s" % data["deployment_uuid"])
            if not result:
                return
            entity = json.loads(result.decode("utf-8"))
            if data["cluster_uuid"] not in entity["clusters"]:
                return
            entity[data["cluster_uuid"]] = data["k8s_params"]
            self.__etcdClient.put("/serrano/orchestrator/monitoring/%s" % data["deployment_uuid"], json.dumps(entity))
            return {}
        except Exception as e:
            print(str(e))
            logger.error(str(e))

    def get_deployments_monitoring_data(self, cluster_uuid):
        data = {}
        try:
            for result in self.__etcdClient.get_prefix("/serrano/orchestrator/monitoring"):
                d = json.loads(result[0].decode("utf-8"))
                deployment_uuid = result[1].key.decode("utf-8").split("/")[-1]
                if cluster_uuid in d["clusters"]:
                    data[deployment_uuid] = d[cluster_uuid]
            return data
        except Exception as e:
            print(str(e))
            logger.error(str(e))
            return data

    def get_telemetry_entities(self):
        result = self.__etcdClient.get("/serrano/orchestrator/telemetry_entities")
        return json.loads(result[0].decode("utf-8"))

