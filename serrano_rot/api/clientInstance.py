import json
import logging
import os.path
import pathlib

import requests

from serrano_rot.api import clientEvents
from serrano_rot.api import clientContext

logger = logging.getLogger("SERRANO.ROT.API.ClientInstance")


class ClientInstance:

    def __init__(self):

        self.__rest_url = None
        self.__http_auth = None
        self.__databroker = None
        self.__client_uuid = None

        self.__load_configuration("/%s/.rot/api.json" % str(pathlib.Path.home()))
        self.__client_context = clientContext.ClientContext(self.__client_uuid, self.__databroker)

    def __load_configuration(self, config_file):

        try:
            with open(config_file) as f:
                params = json.load(f)

                self.__client_uuid = params["api_client"]["client_uuid"]
                self.__databroker = params["databroker_interface"]
                self.__rest_url = "https://%s:%s" % (params["api_client"]["server_address"], params["api_client"]["server_port"])
                self.__http_auth = (params["api_client"]["username"], params["api_client"]["password"])

        except FileNotFoundError:
            raise clientEvents.ConfigurationError("Invalid configuration - FileNotFoundError")
        except json.JSONDecodeError as s:
            raise clientEvents.ConfigurationError("Invalid configuration - JSONDecodeError: %s" % s.msg)
        except KeyError as s:
            raise clientEvents.ConfigurationError("Invalid configuration - Missing configuration parameter %s" % s)

    def connect(self, events, handler):
        self.__client_context.connect(events, handler)

    def get_engines(self):
        data = {}
        res = requests.get("%s/api/v1/rot/engines" % self.__rest_url, auth=self.__http_auth)
        if res.status_code == 200:
            data = json.loads(res.text)["engines"]
        return data

    def get_engine(self, engine_uuid):
        data = {}
        res = requests.get("%s/api/v1/rot/engine/%s" % (self.__rest_url, engine_uuid), auth=self.__http_auth)
        if res.status_code == 200:
            data = json.loads(res.text)
        return data

    def get_logs(self, execution_uuid):
        data = {}
        res = requests.get("%s/api/v1/rot/logs/%s" % (self.__rest_url, execution_uuid), auth=self.__http_auth)
        if res.status_code == 200:
            data = json.loads(res.text)["log_details"]
        return data

    def get_statistics(self, **kwargs):
        start = kwargs.get('start', None)
        end = kwargs.get('end', None)
        return {}

    def delete_execution(self, execution_uuid):
        res = requests.delete("%s/api/v1/rot/execution/%s" % (self.__rest_url, execution_uuid), auth=self.__http_auth)
        print(res.text)

    def post_execution(self, execution_plugin, parameters):
        data = None
        if type(parameters) is not dict:
            parameters = json.loads(parameters)

        try:
            res = requests.post("%s/api/v1/rot/execution" % self.__rest_url,
                                auth=self.__http_auth,
                                json={"execution_plugin": execution_plugin, "parameters": parameters})
            if res.status_code == 200 or res.status_code == 201:
                data = json.loads(res.text)
        except Exception:
            pass
        return data

    def get_execution(self, execution_uuid):
        data = {}
        try:
            res = requests.get("%s/api/v1/rot/execution/%s" % (self.__rest_url, execution_uuid), auth=self.__http_auth)
            if res.status_code == 200:
                data = json.loads(res.text)
        except Exception:
            pass
        return data

    def get_executions(self):
        data = {}
        res = requests.get("%s/api/v1/rot/executions" % self.__rest_url, auth=self.__http_auth)
        if res.status_code == 200:
            data = json.loads(res.text)["executions"]
        return data
