import sys
import json
import time
import signal
import requests
import uvicorn
import os.path
import logging

from PyQt5.QtCore import QObject, QCoreApplication

import uuid
from fastapi import FastAPI, Request, APIRouter, Depends, Response, status
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional
from pydantic import BaseModel, UUID4

import dispatcher
import notificationEngine

LOG_LEVEL = {"CRITICAL": 50, "ERROR": 40, "WARNING": 30, "INFO": 20, "DEBUG": 10}


class Cluster(BaseModel):
    cluster_uuid: str
    type: str
    info: dict


class Deployment(BaseModel):
    name: Optional[str] = None
    user_token: Optional[str] = ""
    deployment_description: str
    deployment_objectives: Optional[List[dict]] = None


class UpdateDeployment(Deployment):
    deployment_uuid: str


class Kernel(BaseModel):
    request_uuid: str
    kernel_name: str
    deployment_objectives: Optional[dict] = {}
    data_description: dict


class LogData(BaseModel):
    uuid: str
    kind: str
    cluster_uuid: str
    status: int
    event: str
    timestamp: int


class Logs(BaseModel):
    logs: List[LogData]


class MetricLogs(BaseModel):
    logs: List[dict]


class StoragePolicy(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = ""
    policy_parameters: dict


class UpdateStoragePolicy(StoragePolicy):
    policy_uuid: str


class AssignmentMonitoringData(BaseModel):
    deployment_uuid: str
    cluster_uuid: str
    assignment_uuid: str
    k8s_params: List[dict]


class NotificationEvent(BaseModel):
    method: str
    model: str
    interval: str
    anomalies: List[dict]


class OrchestratorAPI(QObject):

    def __init__(self, app: FastAPI, conf_params):

        super(QObject, self).__init__()

        logging.basicConfig(filename="%s.log" % (int(time.time())), level=LOG_LEVEL[conf_params["log_level"]])

        logger = logging.getLogger("SERRANO.Orchestrator.OrchestratorAPI")
        logger.info("Initialize services ... ")

        etcd_hostname = conf_params["etcd"]["endpoints"][0] if "etcd" in conf_params else "127.0.0.1"
        etcd_port = conf_params["etcd"]["port"] if "etcd" in conf_params else 2379
        ede_conf = conf_params["ede"] if "ede" in conf_params else {}
        self.__cth_service = conf_params["central_telemetry_handler"]["cth_service"]

        self.__dispatcher = dispatcher.Dispatcher(etcd_hostname, etcd_port, self.__cth_service, ede_conf)

        self.__secure_storage_conf = conf_params["secure_storage"]

        logger.info("SERRANO Resource Orchestrator API is ready ...")

        """
            Clusters 
        """
        @app.get("/api/v1/orchestrator/clusters")
        async def get_clusters(active: Optional[str] = None):
            return {"clusters": self.__dispatcher.get_clusters(active)}

        @app.get("/api/v1/orchestrator/clusters/{cluster_uuid}")
        async def get_cluster(cluster_uuid: uuid.UUID):
            return self.__dispatcher.get_cluster(cluster_uuid)

        @app.get("/api/v1/orchestrator/clusters/health/{cluster_uuid}")
        async def cluster_heartbeat(cluster_uuid: uuid.UUID):
            return self.__dispatcher.cluster_heartbeat(cluster_uuid)

        @app.delete("/api/v1/orchestrator/clusters/{cluster_uuid}")
        async def delete_cluster(cluster_uuid: uuid.UUID):
            self.__dispatcher.delete_cluster(cluster_uuid)
            return {}

        @app.post("/api/v1/orchestrator/clusters", status_code=201)
        async def post_cluster(cluster: Cluster):
            self.__dispatcher.set_cluster({"cluster_uuid": cluster.cluster_uuid, "type": cluster.type,
                                           "info": cluster.info})
            return {"cluster_uuid": cluster.cluster_uuid}

        @app.put("/api/v1/orchestrator/clusters", status_code=200)
        async def put_cluster(cluster: Cluster):
            self.__dispatcher.set_cluster({"cluster_uuid": cluster.cluster_uuid, "type": cluster.type,
                                           "info": cluster.info})
            return {"cluster_uuid": cluster.cluster_uuid}

        """
            Deployments
        """

        @app.get("/api/v1/orchestrator/deployments")
        async def get_deployments():
            return {"deployments": self.__dispatcher.get_deployments()}

        @app.get("/api/v1/orchestrator/deployments/{deployment_uuid}")
        async def get_deployment(deployment_uuid: uuid.UUID):
            return {"deployments": self.__dispatcher.get_deployments(deployment_uuid=deployment_uuid)}

        @app.get("/api/v1/orchestrator/deployments/logs/{deployment_uuid}")
        async def get_deployment(deployment_uuid: uuid.UUID):
            return {"deployments": self.__dispatcher.get_deployment_logs(deployment_uuid)}

        @app.get("/api/v1/orchestrator/deployments/services/{deployment_uuid}")
        async def get_deployment(deployment_uuid: uuid.UUID):
            return self.__dispatcher.get_deployment_services(deployment_uuid)

        @app.delete("/api/v1/orchestrator/deployments/{deployment_uuid}")
        async def delete_deployment(deployment_uuid: uuid.UUID, response: Response):
            if self.__dispatcher.delete_deployment(deployment_uuid):
                response.status_code = status.HTTP_200_OK
            else:
                response.status_code = status.HTTP_404_NOT_FOUND
            return {}

        @app.post("/api/v1/orchestrator/deployments", status_code=201)
        async def post_deployment(deployment: Deployment):
            deployment_uuid = str(uuid.uuid4())
            data = deployment.dict()
            if not deployment.name:
                data["name"] = deployment_uuid
            data["deployment_uuid"] = deployment_uuid
            data["kind"] = "Deployment"
            self.__dispatcher.create_deployment(data)
            return {"deployment_uuid": deployment_uuid}

        @app.put("/api/v1/orchestrator/deployments", status_code=200)
        async def put_deployment(deployment: UpdateDeployment):
            data = deployment.dict()
            if not deployment.name:
                data["name"] = deployment.deployment_uuid
            data["kind"] = "Deployment"
            self.__dispatcher.update_deployment(data)
            return {"deployment_uuid": deployment.deployment_uuid}

        """
            Kernels
        """
        @app.post("/api/v1/orchestrator/kernels", status_code=201)
        async def post_serverless_kernel(kernel: Kernel):
            request_uuid = kernel.data_description["bucket_id"]
            self.__dispatcher.set_kernel_execution({"kind": "Kernel",
                                                    "request_uuid": request_uuid,
                                                    "kernel_name": kernel.kernel_name,
                                                    "data_description": kernel.data_description})
            return {"uuid": request_uuid}

        @app.get("/api/v1/orchestrator/kernels/{request_uuid}", status_code=200)
        async def get_serverless_kernel_logs(request_uuid: uuid.UUID):
            return self.__dispatcher.get_kernel_logs(request_uuid)

        @app.get("/api/v1/orchestrator/kernels", status_code=200)
        async def get_serverless_kernel_logs():
            return self.__dispatcher.get_all_kernels()

        @app.post("/api/v1/orchestrator/faas", status_code=201)
        async def post_faas_kernel(kernel: Kernel):
            self.__dispatcher.set_kernel_execution({"kind": "FaaS",
                                                    "request_uuid": kernel.request_uuid,
                                                    "kernel_name": kernel.kernel_name,
                                                    "deployment_objectives": kernel.deployment_objectives,
                                                    "data_description": kernel.data_description})
            return {"request_uuid": kernel.request_uuid}

        @app.get("/api/v1/orchestrator/faas/{request_uuid}", status_code=200)
        async def get_faas_kernel_logs(request_uuid: uuid.UUID):
            return self.__dispatcher.get_faas_logs(request_uuid)

        @app.get("/api/v1/orchestrator/faas", status_code=200)
        async def get_faas_kernel_logs():
            return {"faas": self.__dispatcher.get_all_faas()}

        """
            Storage Policies
        """
        @app.get("/api/v1/orchestrator/storage_policies")
        async def get_storage_policies():
            return {"storage_policies": self.__dispatcher.get_storage_policy()}

        @app.get("/api/v1/orchestrator/storage_policies/{policy_uuid}")
        async def get_storage_policy(policy_uuid: uuid.UUID):
            return {"storage_policies": self.__dispatcher.get_storage_policy(policy_uuid=policy_uuid)}

        @app.delete("/api/v1/orchestrator/storage_policies/{policy_uuid}")
        async def delete_storage_policy(policy_uuid: uuid.UUID, response: Response):

            try:

                policy = self.__dispatcher.get_storage_policy(policy_uuid=policy_uuid)

                if len(policy):
                    policy_name = policy[0]["name"]
                    res = requests.delete("%s/storage_policy/%s" %(self.__secure_storage_conf["service"], policy_name),
                                          headers={"Authorization": "Bearer %s" % self.__secure_storage_conf["token"] })

                    self.__dispatcher.delete_storage_policy(policy_uuid)
                    response.status_code = status.HTTP_200_OK
                else:
                    response.status_code = status.HTTP_404_NOT_FOUND
            except Exception as e:
                response.status_code = status.HTTP_404_NOT_FOUND

            return {}

        @app.post("/api/v1/orchestrator/storage_policies", status_code=201)
        async def post_storage_policy(policy: StoragePolicy):
            policy_uuid = str(uuid.uuid4())
            data = policy.dict()
            if not policy.name:
                data["name"] = policy_uuid
            data["policy_uuid"] = policy_uuid
            data["kind"] = "StoragePolicy"
            self.__dispatcher.create_storage_policy(data)
            return {"policy_uuid": policy_uuid}

        @app.put("/api/v1/orchestrator/storage_policies", status_code=200)
        async def put_storage_policy(policy: UpdateStoragePolicy, response: Response):
            data = policy.dict()
            if not policy.name:
                data["name"] = policy.policy_uuid
            data["kind"] = "StoragePolicy"
            if self.__dispatcher.update_storage_policy(data):
                response.status_code == status.HTTP_200_OK
                return {"policy_uuid": policy.policy_uuid}
            else:
                response.status_code == status.HTTP_404_NOT_FOUND
                return {}

        """
            Assignments
        """

        @app.get("/api/v1/orchestrator/assignments/{cluster_uuid}/assignment/{assignment_uuid}")
        async def get_assignment(cluster_uuid: uuid.UUID, assignment_uuid: uuid.UUID):
            return self.__dispatcher.get_assignment(cluster_uuid, assignment_uuid)

        """
            Bundles
        """

        @app.get("/api/v1/orchestrator/bundles/{bundle_uuid}")
        async def get_bundle(bundle_uuid: uuid.UUID):
            return self.__dispatcher.get_bundle(bundle_uuid)

        """
            Logs
        """
        @app.post("/api/v1/orchestrator/logs", status_code=201)
        async def post_logs(logs: Logs):
            return self.__dispatcher.add_entities_logs(logs.dict())

        @app.post("/api/v1/orchestrator/metric_logs", status_code=201)
        async def post_metric_logs(logs: MetricLogs):
            try:
                print("POST /metric_logs")
                requests.post("%s/api/v1/telemetry/central/kernel_metrics" % self.__cth_service, json=logs.dict())
            except Exception as e:
                print(str(e))
            return {}

        """
            Monitoring
        """
        @app.put("/api/v1/orchestrator/monitoring", status_code=201)
        async def post_logs(data: AssignmentMonitoringData):
            return self.__dispatcher.put_assignment_monitoring_data(data.dict())



        """
            Grafana
        """
        @app.get("/api/v1/orchestrator/grafana/storage_policies")
        async def grafana_storage_policies(request: Request):
            return self.__dispatcher.grafana_storage_policies(request.query_params)

        @app.get("/api/v1/orchestrator/grafana/storage_policies_logs")
        async def grafana_storage_policies_logs(request: Request):
            return self.__dispatcher.grafana_storage_policies_logs(request.query_params)

        @app.get("/api/v1/orchestrator/grafana/deployments")
        async def grafana_deployments(request: Request):
            return self.__dispatcher.grafana_deployments(request.query_params)

        @app.get("/api/v1/orchestrator/grafana/deployments_logs")
        async def grafana_deployments_logs(request: Request):
            return self.__dispatcher.grafana_deployments_logs(request.query_params)

        @app.get("/api/v1/orchestrator/grafana/faas_kernels")
        async def grafana_faas_kernels(request: Request):
            return self.__dispatcher.grafana_faas_kernels(request.query_params)

        @app.get("/api/v1/orchestrator/grafana/faas_kernels_logs")
        async def grafana_faas_kernels_logs(request: Request):
            return self.__dispatcher.grafana_faas_kernels_logs(request.query_params)

        @app.get("/api/v1/orchestrator/grafana/deployments_services")
        async def grafana_faas_kernels_logs(request: Request):
            return self.__dispatcher.grafana_deployments_services()

def startup(params):
    app = FastAPI()

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    OrchestratorAPI(app, params)
    return app


if __name__ == "__main__":

    config_file = "/etc/serrano/orchestration_api.json" 

    with open(config_file) as f:
        config = json.load(f)

    uvicorn.run(startup(config), host=config["rest_interface"]["address"], port=config["rest_interface"]["port"])
    

