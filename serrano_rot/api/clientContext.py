from enum import IntEnum

from serrano_rot.api import clientEvents
from serrano_rot.api import asynchInterface


class ResponseStatus(IntEnum):
    ACTIVE = 1
    COMPLETED = 2
    FAILED = 3
    REJECTED = 4
    CANCELLED = 5


class ClientContext:

    def __init__(self, client_uuid, databroker):

        self.__events_handlers = dict()

        self.asynchInterface = asynchInterface.AsynchInterface(client_uuid, databroker)
        self.asynchInterface.rotResponse.connect(self.__handle_rot_response)
        self.asynchInterface.rotNotification.connect(self.__handle_rot_notification)
        self.asynchInterface.start()

    def __handle_rot_notification(self, notification):
        self.__notify_event_handlers(clientEvents.EventEnginesChanged(notification))

    def __handle_rot_response(self, response):
        if response["status"] == ResponseStatus.COMPLETED:
            self.__notify_event_handlers(clientEvents.EventExecutionCompleted(response))
        elif response["status"] == ResponseStatus.FAILED or response["status"] == ResponseStatus.REJECTED:
            self.__notify_event_handlers(clientEvents.EventExecutionError(response))
        elif response["status"] == ResponseStatus.CANCELLED:
            self.__notify_event_handlers(clientEvents.EventExecutionCancelled(response))

    def __notify_event_handlers(self, event):

        event_name = event.__class__.__name__

        if event_name not in self.__events_handlers.keys():
            return

        for handler in self.__events_handlers[event_name]:
            handler(event)

    def connect(self, events, handler):

        for evt in events:
            if evt.__name__ not in self.__events_handlers.keys():
                self.__events_handlers[evt.__name__] = [handler]
            else:
                self.__events_handlers[evt.__name__].append(handler)
