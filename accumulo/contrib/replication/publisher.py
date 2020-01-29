from typing import Iterable

import pika

from accumulo import Mutation

from accumulo.contrib.replication import accumulo_replication_pb2


class Publisher:

    def __init__(self, amqp_url: str, queue: str = '', exchange: str = ''):
        self._amqp_connection: pika.BlockingConnection = pika.BlockingConnection(pika.URLParameters(amqp_url))
        self._amqp_channel = self._amqp_connection.channel()
        self._exchange = exchange
        self._routing_key = queue

    def close(self):
        self._amqp_connection.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @staticmethod
    def encode_mutation(m: Mutation):
        return accumulo_replication_pb2.Mutation(
                    row=m.row_bytes,
                    cf=m.cf_bytes,
                    cq=m.cq_bytes,
                    visibility=m.visibility_bytes,
                    timestamp=m.timestamp,
                    value=m.value_bytes,
                    delete=m.delete
                )

    @staticmethod
    def encode_mutations(mutations: Iterable[Mutation]):
        return [Publisher.encode_mutation(m) for m in mutations]

    @staticmethod
    def encode_mutations_batch(table: str, mutations: Iterable[Mutation]):
        return accumulo_replication_pb2.MutationBatch(table=table, mutations=Publisher.encode_mutations(mutations))

    def publish(self, table: str, mutations: Iterable[Mutation]):
        message_encoded = Publisher.encode_mutations_batch(table, mutations).SerializeToString()
        self._amqp_channel.basic_publish(
            exchange=self._exchange,
            routing_key=self._routing_key,
            body=message_encoded,
            properties=pika.BasicProperties(
                delivery_mode=2  # persistent
            )
        )
