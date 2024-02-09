import pika
import json
import time
import logging
import requests

from serrano_orchestrator.utils import status

import hpc.executionWrapper as ExecutionWrapper
import driverInterface

logger = logging.getLogger("SERRANO.Orchestrator.DriverHPC")


class DriverHPC(driverInterface.DriverInterface):

    def __init__(self, config):
        super().__init__(config)
        self.__cluster_uuid = config["cluster_uuid"]
        self.__driver_hpc_conf = config["driver_hpc_conf"]
        self.__hpc_gateway = self.__driver_hpc_conf["gateway_service"]
        self.__hpc_infrastructure = self.__driver_hpc_conf["infrastructure"]
        self.__available_hpc_services = self.__get_available_hpc_services()
        self.__orchestrator_service = config["orchestrator"]["orchestrator_service"]

        self.__databroker_credentials = pika.PlainCredentials(config["driver_hpc_conf"]["databroker_username"],
                                                              config["driver_hpc_conf"]["databroker_password"])

        self.__databroker_connection_params = pika.ConnectionParameters(host=config["driver_hpc_conf"]["databroker_address"],
                                                                        virtual_host=config["driver_hpc_conf"]["databroker_virtual_host"],
                                                                        credentials=self.__databroker_credentials,
                                                                        blocked_connection_timeout=5,
                                                                        socket_timeout=None,
                                                                        heartbeat=0)

        self.__hpc_deployments = {}

    def __get_available_hpc_services(self):
        available_services = {}
        for service in self.get_hpc_services():
            available_services[service["name"]] = 1.0
        return available_services

    def __update_hpc_deployments_mapper(self, data):

        if data["request_uuid"] not in self.__hpc_deployments:
            return

        if not data["hpc_gateway_id"]:
            del self.__hpc_deployments[data["request_uuid"]]
        else:
            self.__hpc_deployments[data["request_uuid"]] = data["hpc_gateway_id"]

    def __post_log_data(self, data):

        try:

            if isinstance(data, list):
                requests.post("%s/api/v1/orchestrator/logs" % self.__orchestrator_service, json={"logs": data})
            else:
                if "kernel_mode" in data:
                    requests.post("%s/api/v1/orchestrator/metric_logs" % self.__orchestrator_service,
                                  json={"logs": [data]})
                else:
                    requests.post("%s/api/v1/orchestrator/logs" % self.__orchestrator_service, json={"logs": [data]})

        except Exception as e:
            logger.error("Unable to update Orchestrator API")
            logger.error(str(e))

    def __handle_results_ready(self, data):
        logger.info("Trigger SDK")
        self.__update_hpc_deployments_mapper(data)
        databroker_connection = pika.BlockingConnection(self.__databroker_connection_params)
        databroker_channel = databroker_connection.channel()
        databroker_channel.queue_declare(queue="watch_kernels_results_%s" % data["request_uuid"], durable=True)
        databroker_channel.basic_publish(exchange="",
                                         routing_key="watch_kernels_results_%s" % data["request_uuid"],
                                         body=json.dumps(data))
        databroker_connection.close()

    # Main abstract method
    def handle_deployment_request(self, request):

        logger.info("Handle deployment request ...")
        logger.debug(json.dumps(request))

        self.__post_log_data({"kind": "FaaS", "uuid": request["deployment_uuid"],
                              "status": status.Kernels.IN_DEPLOYMENT, "cluster_uuid": self.__cluster_uuid,
                              "event": "Orchestrator Driver handles Kernel request",
                              "timestamp": int(time.time())})

        for bundle_uuid in request["bundles"]:
            description = self.get_bundle(bundle_uuid)["description"]
            if description["kind"] == "FaaS":

                description["bundle_uuid"] = bundle_uuid
                description["assignment_uuid"] = request["uuid"]
                description["request_uuid"] = request["deployment_uuid"]

                p = ExecutionWrapper.ExecutionWrapper(self.__driver_hpc_conf, self.__cluster_uuid,  description)
                p.jobSubmitted.connect(self.__update_hpc_deployments_mapper)
                p.updateLogStatus.connect(self.__post_log_data)
                p.resultsReady.connect(self.__handle_results_ready)
                p.start()

        logger.info("Bundle for assignment '%s' is activated" % request["uuid"])

    def handle_termination_request(self, event_key):

        assignment_uuid = event_key.split("/")[-1]

        if assignment_uuid in self.__hpc_deployments:
            hpc_job_id = self.__hpc_deployments[assignment_uuid]
            try:
            	return
            except Exception as err:
                logger.error("Unable to terminate HPC job '%s'" % hpc_job_id)
                logger.error("Traceback error: %s" % str(err))

        logger.info("Termination request for assignment '%s' successfully executed" % assignment_uuid)

    def get_cluster_info(self):

        info = {"services": self.get_hpc_services(), "partitions": []}

        try:
            res = requests.get("%s/infrastructure/%s/telemetry" % (self.__hpc_gateway, self.__hpc_infrastructure))
            data = res.json()
            info["name"] = data["name"]
            info["scheduler"] = data["scheduler"]
            for partition in data["partitions"]:
                info["partitions"].append({"name": partition["name"],
                                           "total_nodes": partition["total_nodes"],
                                           "total_cpus": partition["total_cpus"]})
        except Exception as err:
            logger.error(str(err))

        return info

    def get_hpc_services(self):
        try:
            res = requests.get("%s/services" % self.__hpc_gateway)
            return res.json()
        except Exception as err:
            logger.error(str(err))
            return []
