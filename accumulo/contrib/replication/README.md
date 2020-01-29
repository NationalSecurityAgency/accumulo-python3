# Accumulo Replication Framework

> Under construction.

Use the __Accumulo Replication Framework (ARF)__ to asynchronously publish and
replicate mutations over an AMQP messaging infrastructure such as RabbitMQ.

ARF uses [Protocol Buffers](https://developers.google.com/protocol-buffers) to
encode batches of mutations in AMQP messages. 

ARF includes a Python client for publishing mutations, as well as a Python
consumer application for writing published mutations to Accumulo. ARF must
utilize an existing AMQP queue.

In order to use ARF, the library must be installed with the *replication*
project extra enabled:

```bash
# Install the library with the 'replication' extra enabled
pip install .[replication]
```

### Publisher Example

```python
# publisher.py
import accumulo
from accumulo.contrib.replication import Publisher

if __name__ == '__main__':
    publisher = Publisher('amqp://guest:guest@localhost:5672', exchange='my.replicated.exchange')
    with publisher:
        publisher.publish(
            table='test', 
            mutations=[
                accumulo.Mutation('Row1'),
                accumulo.Mutation('Row2'),
                accumulo.Mutation('Row3', delete=True)
            ]
        )   
```

### Consumer Example

```python
import accumulo
from accumulo.contrib.replication import Consumer

if __name__ == '__main__':
    context = accumulo.AccumuloProxyConnectionContext()
    connector = context.create_connector('user', 'secret')
    consumer = Consumer('amqp://guest:guest@localhost:5672', 'my.replicated.queue', connector)
    with consumer, context.proxy_connection:
        consumer.start_consuming()
```

### Consumer CLI Usage

Run the consumer process directly from the CLI, using environment variables for configuration.

```bash
AMQP_QUEUE=myqueue python -m accumulo.contrib.replication.consumer
```

The CLI supports the following configuration:
- `AMQP_QUEUE`. Required. The name of the AMQP queue from which to consume records.
- `AMQP_URL`. The AMQP URL. Defaults to *amqp://guest:guest@localhost:5672*.
- `ACCUMULO_PROXY_HOSTNAME`. Defaults to *127.0.0.1*.
- `ACCUMULO_PROXY_PORT`. Defaults to *42424*.
- `ACCUMULO_USER`. Defaults to *user*.
- `ACCUMULO_PASSWORD`. Defaults to *secret*.
- `STARTUP_DELAY`. A startup delay in seconds. Defaults to 1 second.