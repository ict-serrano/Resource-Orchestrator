import abc
import logging
import requests

logger = logging.getLogger("SERRANO.Orchestrator.DriverInterface")


class DriverInterface(metaclass=abc.ABCMeta):

    def __init__(self, config):
        self.config = config
        self.__orchestrator_api_base_url = "%s/api/v1/orchestrator" % (config["orchestrator"]["orchestrator_service"])

    def get_bundle(self, bundle_id):
        try:
            res = requests.get("%s/bundles/%s" % (self.__orchestrator_api_base_url, bundle_id), verify=True)
            return res.json()
        except Exception as err:
            logger.error(str(err))
            return None

    def get_assignment(self, cluster_id, assignment_id):
        try:
            res = requests.get("%s/assignments/%s/%s" % (self.__orchestrator_api_base_url, cluster_id, assignment_id), verify=True)
            return res.json()
        except Exception as err:
            logger.error(str(err))
            return None

    @abc.abstractmethod
    def get_cluster_info(self):
        pass

    @abc.abstractmethod
    def handle_deployment_request(self, request):
        pass

    @abc.abstractmethod
    def handle_termination_request(self, request):
        pass
