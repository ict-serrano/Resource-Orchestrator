import uuid
import time

from serrano_orchestrator.utils import status

class Deployment:
    pass


class Bundle:

    def __init__(self, description):
        self.uuid = str(uuid.uuid4())
        self.description = description
        self.status = status.Bundle.CREATED
        self.updated_by = "Orchestration.Manager"
        self.logs = [{"timestamp": int(time.time()), "event": "Bundle created."}]
        self.created_at = int(time.time())
        self.updated_at = int(time.time())

    def to_dict(self):
        return self.__dict__


class Assignment:

    def __init__(self, kind, cluster_uuid, deployment_uuid, bundles_uuids):
        self.uuid = str(uuid.uuid4())
        self.kind = kind
        self.cluster_uuid = cluster_uuid
        self.deployment_uuid = deployment_uuid
        self.bundles = bundles_uuids
        self.status = status.Assignment.CREATED
        self.updated_by = "Orchestration.Manager"
        self.logs = [{"timestamp": int(time.time()), "event": "Assignment created."}]
        self.created_at = int(time.time())
        self.updated_at = int(time.time())

    def set_bundle_uuids(self, uuids):
        self.bundles = uuids

    def to_dict(self):
        return self.__dict__

