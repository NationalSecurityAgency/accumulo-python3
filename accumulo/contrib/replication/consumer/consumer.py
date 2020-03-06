"""consumer.py

Worker utility that consumes encoded mutation batches from the AMQP queue and
commits the mutations to Accumulo.
"""
import logging
from typing import Optional

import pika

from google.protobuf.message import DecodeError

from accumulo import AccumuloConnector, Mutation, WriterOptions
from accumulo.contrib.replication import accumulo_replication_pb2
from accumulo.thrift import ttypes


logger = logging.getLogger('accumulo.replication.consumer')


class Consumer:

    def __init__(self, amqp_url: str, queue: str, accumulo_connector: AccumuloConnector,
                 writer_options: Optional[WriterOptions] = None):
        self._amqp_connection: pika.BlockingConnection = pika.BlockingConnection(pika.URLParameters(amqp_url))
        self._amqp_channel = self._amqp_connection.channel()
        self._queue = queue
        self._accumulo_connector = accumulo_connector
        if writer_options is None:
            writer_options = WriterOptions()
        self._writer_options = writer_options

    def close(self):
        logger.info('Stopping')
        self._amqp_connection.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _callback(self, ch, method, _properties, body):
        logger.info('ProcessingMessage channel=%r delivery_tag=%r' % (ch.channel_number, method.delivery_tag))
        mutation_batch = accumulo_replication_pb2.MutationBatch()
        try:
            mutation_batch.ParseFromString(body)
        except DecodeError as e:
            logger.warning('MalformedMessage', exc_info=e)
            # Do not requeue malformed messages
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return
        table = mutation_batch.table
        mutations = [
            Mutation(
                row=m.row,
                cf=m.cf,
                cq=m.cq,
                visibility=m.visibility,
                timestamp=m.timestamp,
                value=m.value,
                delete=m.delete
            ) for m in mutation_batch.mutations
        ]
        err = False
        try:
            with self._accumulo_connector.create_writer(table, self._writer_options) as writer:
                writer.add_mutations(mutations)
        except ttypes.TableNotFoundException:
            err = True
            logger.error('TableNotFound table=%s' % table)
        except Exception as e:
            err = True
            logger.error('ErrorProcessingMutations', exc_info=e)
        if err:
            logger.warning('ProcessingMessageFailed')
            # Do not requeue failed messages
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def start_consuming(self):
        logger.info('Starting')
        self._amqp_channel.basic_qos(prefetch_count=1)
        self._amqp_channel.basic_consume(queue=self._queue, on_message_callback=self._callback)
        return self._amqp_channel.start_consuming()
