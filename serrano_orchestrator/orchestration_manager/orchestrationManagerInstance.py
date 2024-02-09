import sys
import json
import time
import signal
import logging
import os.path

from PyQt5.QtCore import QObject, QCoreApplication

import orchestrationManager
import orchestrationAPIInterface

LOG_LEVEL = {"CRITICAL": 50, "ERROR": 40, "WARNING": 30, "INFO": 20, "DEBUG": 10}


class OrchestrationManagerInstance(QObject):

    def __init__(self, config):

        super(QObject, self).__init__()

        self.config = config

        self.orchestrationManager = None
        self.orchestratorAPIInterface = None

        logging.basicConfig(filename="%s.log" % (int(time.time())), level=LOG_LEVEL[self.config["log_level"]])

        self.logger = logging.getLogger("SERRANO.Orchestrator.OrchestrationManager")

    def boot(self):

        self.logger.info("Initialize services ... ")

        self.orchestratorAPIInterface = orchestrationAPIInterface.OrchestrationAPIInterface(self.config)

        self.orchestrationManager = orchestrationManager.OrchestrationManager(self.config)
        self.orchestratorAPIInterface.orchestratorRequest.connect(self.orchestrationManager.handle_orchestrator_request)
        self.orchestrationManager.orchestrationManagerUpdate.connect(self.orchestratorAPIInterface.handle_orchestrator_manager_cmd)
        self.orchestrationManager.orchestrationManagerLogInfo.connect(self.orchestratorAPIInterface.handle_orchestrator_manager_logs)

        self.logger.info("SERRANO Orchestration Manager is ready ...")


if __name__ == "__main__":

    with open("/etc/serrano/orchestration_manager.json") as f:
        config = json.load(f)

    signal.signal(signal.SIGINT, signal.SIG_DFL)

    app = QCoreApplication(sys.argv)

    instance = OrchestrationManagerInstance(config)
    instance.boot()

    sys.exit(app.exec_())
    


