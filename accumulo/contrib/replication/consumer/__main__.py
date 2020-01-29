import logging
import os
import time

from accumulo import AccumuloProxyConnection, AccumuloProxyConnectionContext, AccumuloProxyConnectionParams

from .consumer import Consumer


logger = logging.getLogger('accumulo.replication.consumer')
logger.setLevel('INFO')
logger_handler = logging.StreamHandler()
logger.addHandler(logger_handler)
logger_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

startup_delay = int(os.getenv('STARTUP_DELAY', 1))
if startup_delay:
    logger.info(f'Starting in {startup_delay} seconds...')
    time.sleep(startup_delay)

ACCUMULO_PROXY_HOSTNAME = os.getenv('ACCUMULO_PROXY_HOSTNAME', '127.0.0.1')
ACCUMULO_PROXY_PORT = int(os.getenv('ACCUMULO_PROXY_PORT', 42424))
ACCUMULO_USER = os.getenv('ACCUMULO_USER', 'user')
ACCUMULO_PASSWORD = os.getenv('ACCUMULO_PASSWORD', 'secret')
AMQP_URL = os.getenv('AMQP_URL', 'amqp://guest:guest@localhost:5672')
AMQP_QUEUE = os.getenv('AMQP_QUEUE')  # required
proxy_connection_params = AccumuloProxyConnectionParams(hostname=ACCUMULO_PROXY_HOSTNAME, port=ACCUMULO_PROXY_PORT)
proxy_connection = AccumuloProxyConnection(proxy_connection_params)
context = AccumuloProxyConnectionContext(proxy_connection)
connector = context.create_connector(ACCUMULO_USER, ACCUMULO_PASSWORD)
consumer = Consumer(AMQP_URL, AMQP_QUEUE, connector)
try:
    with proxy_connection, consumer:
        consumer.start_consuming()
except KeyboardInterrupt:
    pass
