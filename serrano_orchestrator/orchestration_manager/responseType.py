
class StoragePolicy:

    def __init__(self, response):
        self.kind = response["kind"]
        self.policy_uuid = response["deployment_request"]["policy_uuid"]
        self.name = response["deployment_request"]["name"]
        self.description = response["deployment_request"]["description"]
        self.policy_parameters = response["deployment_request"]["policy_parameters"]
        self.cc_policy_id = response["deployment_request"]["cc_policy_id"]
        self.decision = {"backends": response["backends"], "edge_devices": response["edge_devices"]}

        if "redundant_packets" in response:
            self.decision["redundant_packets"] = response["redundant_packets"]

    def to_dict(self):
        return self.__dict__

    def format_secure_service_request(self, cc_policy_id=None):

        request = {"name": self.name, "description": self.description}

        if cc_policy_id:
            request["id"] = cc_policy_id

        if len(self.decision["backends"]) > 0:
            request["backends"] = self.decision["backends"]

        if len(self.decision["edge_devices"]) > 0:
            request["edge_devices"] = self.decision["edge_devices"]

        if "redundant_packets" in self.decision:
            request["redundancy"] = {"scheme": "RLNC", "redundant_packets": self.decision["redundant_packets"]}

        return request


class Deployment:

    def __init__(self, response):
        self.kind = response["kind"]
        self.request_uuid = ""

    def to_dict(self):
        return self.__dict__
