class ConfigurationError(Exception):
    pass


class EventBase(object):

    def __init__(self):
        super(EventBase, self).__init__()


class EventEnginesChanged(EventBase):

    def __init__(self, notification_params):
        super(EventEnginesChanged, self).__init__()
        self.engine_uuid = notification_params["engine_id"]
        self.event_id = notification_params["event_id"]
        self.timestamp = notification_params["timestamp"]


class EventExecutionCompleted(EventBase):

    def __init__(self, response_params):
        super(EventExecutionCompleted, self).__init__()
        self.evt_type = "EventExecutionCompleted"
        self.execution_uuid = response_params["uuid"]
        self.status = response_params["status"]
        self.results = response_params["results"]


class EventExecutionError(EventBase):

    def __init__(self, response_params):
        super(EventExecutionError, self).__init__()
        self.evt_type = "EventExecutionError"
        self.execution_uuid = response_params["uuid"]
        self.status = response_params["status"]
        self.reason = response_params["reason"]


class EventExecutionCancelled(EventBase):

    def __init__(self, response_params):
        super(EventExecutionCancelled, self).__init__()
        self.evt_type = "EventExecutionCancelled"
        self.execution_uuid = response_params["uuid"]
        self.status = response_params["status"]
        self.reason = response_params["reason"]

