import os
import yaml


class KernelTemplate:

    def __init__(self, bundle_params):
        self.__description = None

        with open("%s/kernel.tpl" % os.path.dirname(os.path.realpath(__file__))) as yaml_in:
            self.__description = yaml.safe_load(yaml_in)

        self.__description["metadata"]["name"] = "kernel-%s" % bundle_params["request_uuid"][-12:]
        self.__description["spec"]["initContainers"][0]["args"][2] = bundle_params["input_data"]
        self.__description["spec"]["containers"][1]["image"] = "ictserrano/serrano:%s" % bundle_params["name"]

        for inx, e in enumerate(self.__description["spec"]["containers"][1]["env"]):
            if e["name"] == "UUID":
                self.__description["spec"]["containers"][1]["env"][inx]["value"] = bundle_params["request_uuid"]
                break

        print(self.__description)

    def get_k8s_description(self):
        return self.__description


