import json
from json import JSONDecodeError
from typing import Generator, Tuple
import pandas as pd
import pika
import os

class MessageBroker:
    EXCHANGE_NAME = os.environ.get("EXCHANGE_NAME")

    def __init__(self):
        self.conn_params = pika.ConnectionParameters(host=os.environ.get("HOST"))
        self.channel = pika.BlockingConnection(self.conn_params).channel()
        self.channel.exchange_declare(exchange=self.EXCHANGE_NAME, exchange_type=os.environ.get("EXCHANGE_TYPE"))
        self.callback = None

    def subscribe(self) -> Generator[Tuple[int, str], None, None]:
        result = self.channel.queue_declare(queue="", exclusive=True)
        queue_name = result.method.queue
        self.channel.queue_bind(exchange=self.EXCHANGE_NAME, queue=queue_name)
        print(' [*] Waiting for logs. To exit press CTRL+C')
        for method, props, body in self.channel.consume(queue_name):
            body = body.decode()
            # logger.info(f"<<< message received: {body}")
            self.channel.basic_ack(method.delivery_tag)
            id = method.delivery_tag
            try:
                yield id, body
            except (KeyError, JSONDecodeError) as e:
                yield id, body