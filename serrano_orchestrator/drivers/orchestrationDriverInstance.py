import sys
import uuid
import json
import time
import signal
import logging
import urllib3
import os.path

from PyQt5.QtCore import QObject, QTimer, QCoreApplication

import orchestrationDriver

LOG_LEVEL = {"CRITICAL": 50, "ERROR": 40, "WARNING": 30, "INFO": 20, "DEBUG": 10}
CONF_FILE = "/etc/serrano/orchestration_driver.json"


class OrchestrationDriverInstance(QObject):

    def __init__(self, conf):

        super(QObject, self).__init__()

        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        self.config = conf

        self.cluster_uuid = None
        self.statusTimer = None
        self.platformInterface = None
        self.orchestrationDriver = None

        logging.basicConfig(filename="%s.log" % (int(time.time())), level=LOG_LEVEL[self.config["log_level"]])

        # Suppress logging messages from pika only to levels: WARNING,ERROR and CRITICAL
        logging.getLogger('pika').setLevel(logging.WARNING)

        self.logger = logging.getLogger("SERRANO.Orchestrator.OrchestrationDriver")

        if "cluster_uuid" not in conf or len(conf["cluster_uuid"]) == 0:
            self.cluster_uuid = str(uuid.uuid4())
        else:
            self.cluster_uuid = conf["cluster_uuid"]

    def boot(self):

        print("Initialize OrchestrationDriver for cluster: %s" % self.cluster_uuid)

        self.logger.info("Initialize services ... ")

        self.orchestrationDriver = orchestrationDriver.OrchestrationDriver(self.cluster_uuid, self.config)

        driver_module = __import__(self.config["driver"])
        if self.config["driver"] == "driverHPC":
            driver_class = getattr(driver_module, "DriverHPC")
        else:
            driver_class = getattr(driver_module, "DriverKubernetes")

        self.platformInterface = driver_class(self.config)
        self.orchestrationDriver.orchestrationManagerDeploymentRequest.connect(
            self.platformInterface.handle_deployment_request)
        self.orchestrationDriver.orchestrationManagerTerminationRequest.connect(
            self.platformInterface.handle_termination_request)

        self.orchestrationDriver.set_cluster_info(self.platformInterface.get_cluster_info())

        self.statusTimer = QTimer(self)
        self.statusTimer.timeout.connect(self.orchestrationDriver.heartbeat)
        self.statusTimer.start(int(self.config["heartbeat"]) * 1000)

        self.logger.info("SERRANO Orchestration Driver is ready ...")


if __name__ == "__main__":

    signal.signal(signal.SIGINT, signal.SIG_DFL)

    target_config_file = CONF_FILE
    config_params = None

    if len(sys.argv) == 2:
        target_config_file = sys.argv[1]

    if os.path.exists(target_config_file):
        with open(target_config_file) as f:
            config_params = json.load(f)

    if config_params is None:
        sys.exit(0)

    app = QCoreApplication(sys.argv)

    instance = OrchestrationDriverInstance(config_params)
    instance.boot()

    sys.exit(app.exec_())
