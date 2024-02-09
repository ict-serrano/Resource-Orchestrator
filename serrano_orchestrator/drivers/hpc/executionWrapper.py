import time
import logging
import requests

import hpc.serviceTemplate as ServiceTemplate

from serrano_orchestrator.utils import status

from PyQt5.QtCore import QThread
from PyQt5.QtCore import pyqtSignal

logger = logging.getLogger("SERRANO.Orchestrator.OrchestrationDriver.ExecutionWrapper")


class ExecutionWrapper(QThread):

    jobSubmitted = pyqtSignal(object)
    updateLogStatus = pyqtSignal(object)
    resultsReady = pyqtSignal(object)

    def __init__(self, config, cluster_uuid, description):

        QThread.__init__(self)

        self.__cluster_uuid = cluster_uuid

        self.__gateway_service = config["gateway_service"]
        self.__infrastructure = config["infrastructure"]
        self.__s3_endpoint = config["s3_endpoint"]
        self.__s3_access_key = config["s3_access_key"]
        self.__s3_secret_key = config["s3_secret_key"]

        self.__request_uuid = description["request_uuid"]
        self.__bundle_uuid = description["bundle_uuid"]
        self.__assignment_uuid = description["assignment_uuid"]

        self.__kernel_name = description["kernel_name"]
        self.__bucket_id = description["data_description"]["bucket_id"]
        self.__arguments = description["data_description"]["arguments"]
        self.__input_data_total_size_MB = description["data_description"]["total_size_MB"]

        self.__serviceTemplate = None


    def __del__(self):
        self.wait()

    def __notify_sdk_for_results(self, status):
        self.resultsReady.emit({"request_uuid": self.__request_uuid,
                                "bucket_id": self.__bucket_id,
                                "hpc_gateway_id": None,
                                "status": status})
    def __failed_assignment_evt(self):
        return {"uuid": self.__assignment_uuid,
                "cluster_uuid": self.__cluster_uuid,
                "kind": "Assignment",
                "event": "Related bundle failed",
                "status": status.Assignment.FAILED,
                "timestamp": int(time.time())}

    def __check_results_transfer_status(self, id):
        while True:
            res = requests.get("%s/s3_result/%s" % (self.__gateway_service, id))
            if res.status_code == 200 or res.status_code == 201:
                data = res.json()
                if data["status"] == "transferring":
                    time.sleep(1)
                elif data["status"] == "completed":
                    self.updateLogStatus.emit({"uuid": self.__bundle_uuid,
                                               "cluster_uuid": self.__cluster_uuid,
                                               "kind": "Bundle",
                                               "event": "Moving results from HPC Gateway - Completed",
                                               "status": status.Bundle.HPC_RESULTS_TRANSFER_COMPLETED,
                                               "timestamp": int(time.time())})
                    return True
                else:
                    self.updateLogStatus.emit([{"uuid": self.__bundle_uuid,
                                                "cluster_uuid": self.__cluster_uuid,
                                                "kind": "Bundle",
                                                "event": "Moving results from HPC Gateway - Failed (%s)" % data["reason"],
                                                "status": status.Bundle.HPC_RESULTS_TRANSFER_FAILED,
                                                "timestamp": int(time.time())}, self.__failed_assignment_evt()])
                    return False

    def __check_data_transfer_status(self, id):
        while True:
            res = requests.get("%s/s3_data/%s" % (self.__gateway_service, id))
            if res.status_code == 200 or res.status_code == 201:
                data = res.json()
                if data["status"] == "transferring":
                    time.sleep(1)
                elif data["status"] == "completed":
                    self.updateLogStatus.emit({"uuid": self.__bundle_uuid,
                                               "cluster_uuid": self.__cluster_uuid,
                                               "kind": "Bundle",
                                               "event": "Moving data to HPC Gateway - Completed",
                                               "status": status.Bundle.HPC_DATA_TO_GATEWAY_COMPLETED,
                                               "timestamp": int(time.time())})
                    return True
                else:
                    self.updateLogStatus.emit([{"uuid": self.__bundle_uuid,
                                                "cluster_uuid": self.__cluster_uuid,
                                                "kind": "Bundle",
                                                "event": "Moving data to HPC Gateway - Failed (%s)" % data["reason"],
                                                "status": status.Bundle.HPC_DATA_TO_GATEWAY_FAILED,
                                                "timestamp": int(time.time())}, self.__failed_assignment_evt()])
                    return False
            else:
                return False

    def __check_execution_status(self, job_id):
        logger.info("Checking the execution status for request_uuid '%s'" % self.__request_uuid)
        while True:
            res = requests.get("%s/job/%s" % (self.__gateway_service, job_id))
            if res.status_code == 200 or res.status_code == 201:
                data = res.json()
                if data["status"] == "queued" or data["status"] == "running":
                    time.sleep(1)
                elif data["status"] == "completed":
                    self.updateLogStatus.emit({"uuid": self.__bundle_uuid,
                                               "cluster_uuid": self.__cluster_uuid,
                                               "kind": "Bundle",
                                               "event": "Execution in HPC is completed",
                                               "status": status.Bundle.HPC_EXECUTION_COMPLETED,
                                               "timestamp": int(time.time())})
                    return True
            else:
                return False

    def __move_results_from_hpc(self):

        logger.info("Move results from HPC Gateway for request_uuid '%s'" % self.__request_uuid)

        post_data = {"endpoint": self.__s3_endpoint,
                     "access_key": self.__s3_access_key,
                     "secret_key": self.__s3_secret_key,
                     "region": "local",
                     "infrastructure": self.__infrastructure,
                     "bucket": self.__bucket_id,
                     "object": "results_req_%s" % self.__request_uuid,
                     "src": self.__serviceTemplate.get_kernel_results_filename()}

        try:
            res = requests.post("%s/s3_result" % self.__gateway_service, json=post_data)
            if res.status_code == 200 or res.status_code == 201:
                if not self.__check_results_transfer_status(res.json()["id"]):
                    return False
                else:
                    return True
            else:
                logger.error("Unable to move results from HPC Gateway for request_uuid '%s'" % self.__request_uuid)
                self.updateLogStatus.emit([{"uuid": self.__bundle_uuid,
                                            "cluster_uuid": self.__cluster_uuid,
                                            "kind": "Bundle",
                                            "event": "Unable to get results from HPC Gateway - Status code: %s" % res.status_code,
                                            "status": status.Bundle.HPC_RESULTS_TRANSFER_FAILED,
                                            "timestamp": int(time.time())}, self.__failed_assignment_evt()])
                return False
        except Exception as e:
            logger.error("Unable to move results from HPC Gateway for request_uuid: %s" % self.__request_uuid)
            self.updateLogStatus.emit([{"uuid": self.__bundle_uuid,
                                        "cluster_uuid": self.__cluster_uuid,
                                        "kind": "Bundle",
                                        "event": "Unable to get results from HPC Gateway - %s" % str(e),
                                        "status": status.Bundle.HPC_RESULTS_TRANSFER_FAILED,
                                        "timestamp": int(time.time())}, self.__failed_assignment_evt()])
            return False

    def __move_data_to_hpc(self):

        logger.info("Move data from bucket '%s' to HPC Gateway Service" % self.__bucket_id)

        self.__serviceTemplate = ServiceTemplate.ServiceTemplate(self.__kernel_name, self.__arguments)

        for object_name in self.__arguments:

            logger.debug("Move object '%s' from bucket '%s' to HPC Gateway Service" % (object_name, self.__bucket_id))

            put_data = {"endpoint": self.__s3_endpoint,
                        "bucket": self.__bucket_id,
                        "object": object_name,
                        "region": "local",
                        "access_key": self.__s3_access_key,
                        "secret_key": self.__s3_secret_key,
                        "dst": "%s/from_s3_%s" % (self.__serviceTemplate.get_data_to_hpc_dst_path(), object_name),
                        "infrastructure": self.__infrastructure}

            try:
                self.updateLogStatus.emit({"uuid": self.__bundle_uuid,
                                           "cluster_uuid": self.__cluster_uuid,
                                           "kind": "Bundle",
                                           "event": "Moving data to HPC Gateway - Requested",
                                           "status": status.Bundle.HPC_DATA_TO_GATEWAY_REQUESTED,
                                           "timestamp": int(time.time())})

                res = requests.post("%s/s3_data" % self.__gateway_service, json=put_data)

                if res.status_code == 200 or res.status_code == 201:
                    hpc_data_id = res.json()["id"]
                    if not self.__check_data_transfer_status(hpc_data_id):
                        return False
                else:
                    logger.error("Unable to move data from bucket '%s' to HPC Gateway Service" % self.__bucket_id)
                    self.updateLogStatus.emit([{"uuid": self.__bundle_uuid,
                                                "cluster_uuid": self.__cluster_uuid,
                                                "kind": "Bundle",
                                                "event": "Unable to move data to HPC Gateway - Status code: %s" % res.status_code,
                                                "status": status.Bundle.HPC_DATA_TO_GATEWAY_FAILED,
                                                "timestamp": int(time.time())}, self.__failed_assignment_evt()])
                    return False

            except Exception as e:
                logger.error("Unable to move data from bucket '%s' to HPC Gateway Service" % self.__bucket_id)
                self.updateLogStatus.emit([{"uuid": self.__bundle_uuid,
                                            "cluster_uuid": self.__cluster_uuid,
                                            "kind": "Bundle",
                                            "event": "Move data to HPC Gateway - Status code: %s" % res.status_code,
                                            "status": status.Bundle.HPC_DATA_TO_GATEWAY_FAILED,
                                            "timestamp": int(time.time())}, self.__failed_assignment_evt()])
                return False

        return True

    def __submit_hpc_job(self):

        logger.info("Submit execution request to HPC service for request_uuid '%s'" % self.__request_uuid)

        job_id = None

        try:
            self.updateLogStatus.emit({"uuid": self.__bundle_uuid,
                                       "cluster_uuid": self.__cluster_uuid,
                                       "kind": "Bundle",
                                       "event": "Submitting execution request to HPC Gateway",
                                       "status": status.Bundle.HPC_EXECUTION_REQUESTING,
                                       "timestamp": int(time.time())})

            res = requests.post("%s/job" % self.__gateway_service, json=self.__serviceTemplate.to_dict())

            if res.status_code == 200 or res.status_code == 201:
                data = res.json()
                job_id = data["id"]
                self.jobSubmitted.emit({"request_uuid": self.__request_uuid, "hpc_gateway_id": data["id"]})
                self.updateLogStatus.emit({"uuid": self.__bundle_uuid,
                                           "cluster_uuid": self.__cluster_uuid,
                                           "kind": "Bundle",
                                           "event": "Execution request is submitted to HPC Gateway",
                                           "status": status.Bundle.HPC_EXECUTION_SUBMITTED,
                                           "timestamp": int(time.time())})
            else:
                logger.error("Unable to submit execution request to HPC Gateway Service for request_uuid '%s'" %
                             self.__request_uuid)
                self.updateLogStatus.emit([{"uuid": self.__bundle_uuid,
                                            "cluster_uuid": self.__cluster_uuid,
                                            "kind": "Bundle",
                                            "event": "Unable to submit execution request to HPC Gateway - Status code: %s"
                                                     % res.status_code,
                                            "status": status.Bundle.HPC_EXECUTION_FAILED,
                                            "timestamp": int(time.time())}, self.__failed_assignment_evt()])

        except Exception as e:
            logger.error("Unable to submit execution request to HPC Gateway Service for request_uuid '%s'"
                         % self.__request_uuid)
            logger.error(str(e))
            self.updateLogStatus.emit([{"uuid": self.__bundle_uuid,
                                        "cluster_uuid": self.__cluster_uuid,
                                        "kind": "Bundle",
                                        "event": "Unable to submit execution request to HPC Gateway - %s" % str(e),
                                        "status": status.Bundle.HPC_EXECUTION_FAILED,
                                        "timestamp": int(time.time())}, self.__failed_assignment_evt()])
        return job_id

    def run(self):

        metrics = {}
        deployed_at = int(time.time())

        tick = int(time.time())

        if not self.__move_data_to_hpc():
            self.__notify_sdk_for_results(0)
            return

        metrics["move_data_to_hpc_secs"] = int(time.time()) - tick

        tick = int(time.time())

        job_id = self.__submit_hpc_job()

        print("HPC job_id = %s" % job_id)

        if not job_id:
            self.__notify_sdk_for_results(0)
            return

        if not self.__check_execution_status(job_id):
            self.__notify_sdk_for_results(0)
            return

        print("HPC job_id = %s finished, move data ..." % job_id)

        metrics["hpc_job_execution_secs"] = int(time.time()) - tick

        tick = int(time.time())

        if self.__move_results_from_hpc():
            self.__notify_sdk_for_results(1)
            self.updateLogStatus.emit({"uuid": self.__assignment_uuid,
                                       "cluster_uuid": self.__cluster_uuid,
                                       "kind": "Assignment",
                                       "event": "Kernel Assignment executed successfully",
                                       "status": status.Assignment.DEPLOYED,
                                       "timestamp": int(time.time())})

            metrics["move_results_from_hpc_secs"] = int(time.time()) - tick

            metrics_logs = {"uuid": self.__request_uuid, "kind": "KernelMetrics", "deployment_mode": "FaaS",
                            "kernel_mode": "HPC", "cluster_uuid": self.__cluster_uuid,
                            "kernel_name": self.__kernel_name,
                            "input_total_size_MB": self.__input_data_total_size_MB,
                            "deployed_at": deployed_at, "completed_at": int(time.time()), "metrics": metrics}

            self.updateLogStatus.emit(metrics_logs)

        else:
            self.__notify_sdk_for_results(0)
