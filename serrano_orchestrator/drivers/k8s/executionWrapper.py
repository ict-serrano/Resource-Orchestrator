import json
import time
import pika
import logging
import requests

from serrano_orchestrator.utils import status

from PyQt5.QtCore import QThread
from PyQt5.QtCore import pyqtSignal

logger = logging.getLogger("SERRANO.Orchestrator.OrchestrationDriver.ExecutionWrapper")


class ExecutionWrapper(QThread):

    updateLogStatus = pyqtSignal(object)

    def __init__(self, driver_k8s_conf, description):

        QThread.__init__(self)

        self.__kernel_name = description["kernel_name"]

        self.__cluster_uuid = description["cluster_uuid"]
        self.__request_uuid = description["request_uuid"]

        self.__faas_endpoint = description["data_description"]["faas_endpoint"]
        self.__data_description = description["data_description"]

        self.__databroker_address = driver_k8s_conf["databroker_address"]
        self.__databroker_username = driver_k8s_conf["databroker_username"]
        self.__databroker_password = driver_k8s_conf["databroker_password"]
        self.__databroker_virtual_host = driver_k8s_conf["databroker_virtual_host"]

    def __del__(self):
        self.wait()

    def __parse_vaccel_logs(self, message):
        c = 0
        vaccel_kernel_metrics = {}
        keys = ["load_vaccel_libs_ms", "load_model_libs_ms", "read_input_from_backend_ms", "parse_model_ms",
                "parse_input_ms", "setup_vaccel_args_ms", "run_kernel_ms", "output_ms", "push_output_to_backend_ms",
                "total_ms"]

        vaccel_part = message.split("Load vAccel libraries")[1]

        for vaccel_line in vaccel_part.split("\n"):
            if vaccel_line.find(" ms") != -1:
                metric = vaccel_line.split(":")[1][:-2]
                vaccel_kernel_metrics[keys[c]] = int(metric)
                c += 1

        return vaccel_kernel_metrics

    def __reset_results_on_error(self):

        try:
            message = json.dumps({"uuid": self.__request_uuid, "data": None})

            connection_parameters = pika.ConnectionParameters(host=self.__databroker_address,
                                                              virtual_host=self.__databroker_virtual_host,
                                                              credentials=pika.PlainCredentials(self.__databroker_username,
                                                                                                self.__databroker_password),
                                                              blocked_connection_timeout=5,
                                                              socket_timeout=None,
                                                              heartbeat=10)

            connection = pika.BlockingConnection(connection_parameters)
            channel = connection.channel()

            channel.queue_declare(queue="kernels_results_%s" % self.__request_uuid, durable=True)
            channel.queue_declare(queue=self.__request_uuid, durable=True)

            channel.basic_publish(exchange='',
                                  routing_key="kernels_results_%s" % self.__request_uuid,
                                  body=message,
                                  properties=pika.BasicProperties(delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE))
            channel.basic_publish(exchange='',
                                  routing_key=self.__request_uuid,
                                  body=message,
                                  properties=pika.BasicProperties(delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE))
            channel.close()
            connection.close()
        except Exception as e:
            print(str(e))

    def __execute_faas_kernel(self):

        logger.info("Submit execution request to OpenFaaS service for request_uuid '%s'" % self.__request_uuid)

        logs = []
        metrics_logs = []

        try:

            deployed_at = int(time.time())

            logs.append({"uuid": self.__request_uuid, "kind": "FaaS",
                         "cluster_uuid": self.__cluster_uuid, "status": status.Kernels.DEPLOYED,
                         "event": "Submitting execution request to OpenFaas endpoint", "timestamp": deployed_at})

            res = requests.post(self.__faas_endpoint, json=json.dumps(self.__data_description))

            logger.debug(res.text)

            metrics_logs.append({"uuid": self.__request_uuid, "kind": "KernelMetrics", "deployment_mode": "FaaS",
                                 "cluster_uuid": self.__cluster_uuid, "kernel_name": self.__kernel_name,
                                 "input_total_size_MB": self.__data_description["total_size_MB"],
                                 "deployed_at": deployed_at, "completed_at": 0,
                                 "kernel_mode": self.__data_description["mode"], "metrics": {}})

            if res.status_code == 200 or res.status_code == 201:
                metrics_logs[0]["status"] = 1
                metrics_logs[0]["completed_at"] = int(time.time())
                metrics_logs[0]["metrics"] = self.__parse_vaccel_logs(res.text)

                logs.append({"uuid": self.__request_uuid, "kind": "FaaS", "status": status.Kernels.FINISHED,
                             "cluster_uuid": self.__cluster_uuid,
                             "event": "Kernel executed successfully.", "timestamp": int(time.time())})
            else:
                metrics_logs[0]["status"] = 0
                self.__reset_results_on_error()
                logs.append({"uuid": self.__request_uuid, "kind": "FaaS", "status": status.Kernels.FAILED,
                             "cluster_uuid": self.__cluster_uuid,
                             "event": "Error in kernel execution. Request status_code: %s" % res.status_code,
                             "timestamp": int(time.time())})

            self.updateLogStatus.emit({"logs": logs, "metric_logs": metrics_logs})

        except Exception as e:
            logger.error("Unable to execute FaaS kernel deployment for request_uuid '%s'" % self.__request_uuid)
            logger.error(str(e))
            metrics_logs[0]["status"] = 0
            self.updateLogStatus.emit({"logs": [{"uuid": self.__request_uuid, "kind": "FaaS",
                                                 "cluster_uuid": self.__cluster_uuid,
                                                 "event": "Unable to execute FaaS kernel. Error: %s" % str(e),
                                                 "status": status.Kernels.FAILED,
                                                 "timestamp": int(time.time())}], "metric_logs": metrics_logs})

    def run(self):
        self.__execute_faas_kernel()
