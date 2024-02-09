import pika
import json

from PyQt5.QtCore import QThread
from PyQt5.QtCore import pyqtSignal


RESPONSE_EXCHANGE = "rot_v2_dispatcher_results"
NOTIFICATION_EXCHANGE = "rot_v2_dispatcher_events"


class ResponseInterface(QThread):

    responseMessage = pyqtSignal(object)

    def __init__(self, client_uuid, config):

        QThread.__init__(self)

        connection_parameters = pika.ConnectionParameters(host=config["address"],
                                                          virtual_host=config["virtual_host"],
                                                          credentials=pika.PlainCredentials(
                                                              config["username"],
                                                              config["password"]),
                                                          blocked_connection_timeout=5,
                                                          socket_timeout=None,
                                                          heartbeat=0)

        connection = pika.BlockingConnection(connection_parameters)
        self.channel = connection.channel()

        self.channel.exchange_declare(exchange=RESPONSE_EXCHANGE, exchange_type="direct")
        result = self.channel.queue_declare(queue="", exclusive=True)
        self.queue_name = result.method.queue
        self.channel.queue_bind(exchange=RESPONSE_EXCHANGE, queue=self.queue_name, routing_key=client_uuid)

    def __del__(self):
        self.wait()

    def run(self):

        def callback(ch, method, properties, body):
            self.responseMessage.emit(json.loads(body.decode("utf-8")))

        self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback, auto_ack=True)
        self.channel.start_consuming()


class AsynchInterface(QThread):

    rotResponse = pyqtSignal(object)
    rotNotification = pyqtSignal(object)

    def __init__(self, client_uuid, databroker):
        QThread.__init__(self)

        connection_parameters = pika.ConnectionParameters(host=databroker["address"],
                                                          virtual_host=databroker["virtual_host"],
                                                          credentials=pika.PlainCredentials(
                                                              databroker["username"],
                                                              databroker["password"]),
                                                          blocked_connection_timeout=5,
                                                          socket_timeout=None,
                                                          heartbeat=0)
        connection = pika.BlockingConnection(connection_parameters)
        self.channel = connection.channel()

        self.channel.exchange_declare(exchange=NOTIFICATION_EXCHANGE, exchange_type="fanout")
        result = self.channel.queue_declare(queue="", exclusive=True)
        self.queue_name = result.method.queue

        self.responseInterface = ResponseInterface(client_uuid, databroker)
        self.responseInterface.responseMessage.connect(self.__handle_response)
        self.responseInterface.start()

    def __handle_response(self, data):
        self.rotResponse.emit(data)

    def __del__(self):
        self.wait()

    def run(self):
        self.channel.queue_bind(exchange=NOTIFICATION_EXCHANGE, queue=self.queue_name)

        def callback(ch, method, properties, body):
            self.rotNotification.emit(json.loads(body.decode("utf-8")))

        self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback, auto_ack=True)
        self.channel.start_consuming()

