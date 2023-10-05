# accumulo-python3

Use this library to write Python 3 applications that integrate with [Apache Accumulo](https://accumulo.apache.org/).

Library features include:
- Convenience classes for creating Accumulo objects, such as Mutations and Ranges
- A blocking, synchronous client
- A non-blocking, asynchronous client for applications using the [asyncio](https://docs.python.org/3/library/asyncio.html) 
  module.
 
```python
import accumulo
from accumulo import Mutation, RangePrefix, ScanOptions

# The accumulo proxy now authenticates with accumulo by itself.
# This client then authenticates with the proxy using a configured 'sharedSecret',
#    shared between the client and proxy
sharedSecret = "sharedSecret"

connector = accumulo.AccumuloProxyConnectionContext().create_connector(sharedSecret)

# Create the table 'tmp' if it does not already exist.
if not connector.table_exists('tmp'):
    connector.create_table('tmp')

# Commit some mutations
with connector.create_writer('tmp') as writer:
    writer.add_mutations([
        Mutation('User.1', 'loc', 'x', value='34'),
        Mutation('User.1', 'loc', 'y', value='35'),
        Mutation('User.1', 'old_property', delete=True)
    ])

# Scan the table
with connector.create_scanner('tmp', ScanOptions(range=RangePrefix('User.1'))) as scanner:
    for r in scanner:
        print(r.row, r.cf, r.value_bytes)
```

__Note__. This library is a work in progress. It has been tested with Accumulo 2.1.1 and Python 3.8.

## Installation

This library is not yet available on the [Python Package Index](https://pypi.org/). 

Clone the repository and use `pip` to install locally into your environment.

```bash
git clone https://github.com/NationalSecurityAgency/accumulo-python3.git
cd accumulo-python3
pip install .
```

Optionally include the `-e` option with `pip` to install the library in *edit mode*, which is appropriate for local
development.

```
pip install -e .
```

### Docker Image

Alternatively, build a docker image

```
docker build . -t accumulo-proxy-client
```

## Background

Native integration with Accumulo is powered by [Apache Thrift](https://thrift.apache.org/). This library embeds
Thrift-generated Python 3 bindings for Accumulo in the `accumulo.thrift` submodule. The generated bindings are
low-level and inconsistent with idiomatic Python 3 conventions. This library provides higher-level functionality around
the generated bindings in order to support more practical development.

[Accumulo Proxy](https://github.com/apache/accumulo-proxy/) is required to broker communications between Thrift clients
(such as this library) and Accumulo.

## Manual

### Create a proxy connection

A __proxy connection__ represents the connection to the Accumulo Proxy server.

Use the `AccumuloProxyConnection` and `AccumuloProxyConnectionParams` classes to create a proxy connection to Accumulo 
Proxy.

```python
from accumulo import AccumuloProxyConnection, AccumuloProxyConnectionParams

# Note: These are the default settings.
proxy_connection = AccumuloProxyConnection(AccumuloProxyConnectionParams(hostname='127.0.0.1', port=42424))

# Alternatively, create a proxy connection using the default settings.
proxy_connection = AccumuloProxyConnection()
```

Alternatively, use the proxy connection instance as a context manager to automatically close it.

```
with proxy_connection:
    pass
```

Otherwise, use `proxy_connection.close()` to manually close the proxy connection instance.

### Use the proxy connection to call the low-level Accumulo bindings

It may be necessary to use low-level Thrift-generated bindings to perform certain actions that are not supported by the
higher-level functionality in this library. Use the `client` property of an `AccumuloProxyConnection` instance to
access these bindings.

```python
proxy_connection.client.changeUserAuthorizations('sharedSecret', 'user', [b'ADMIN'])
```

### Creating a blocking connector

A __connector__ is an authenticated interface to Accumulo, and is used to perform actions that require authentication, 
such as creating tables or scanners. A __context__ is used to create a connector.

Use the `AccumuloProxyConnectionContext` class to create a blocking connector instance.

```python
from accumulo import AccumuloProxyConnectionContext
sharedSecret = "sharedSecret"
context = AccumuloProxyConnectionContext(proxy_connection)
connector = context.create_connector(sharedSecret)
```

### Perform some basic table operations

In the example below, we create the table *tmp* if it does not already exist.

```python
if not connector.table_exists('tmp'):
    connector.create_table('tmp')
```

### Change user authorizations

In the example below, we add an authorization to the our user's authorizations.

```python
from accumulo import AuthorizationSet

# Get the user's current set of authorizations
current_auths = connector.get_user_authorizations('tmp')
# AuthorizationSet behaves like a frozenset and supports set operators
new_auths = AuthorizationSet({'PRIVATE'}) | current_auths  # set union
connector.change_user_authorizations('user', new_auths)
```

### Add some mutations

In the example below, we add mutations to a table.

```python
from accumulo import Mutation, WriterOptions

# Use the writer as a context manager to automatically close it. The second parameter opts is optional.
with connector.create_writer('tmp', opts=WriterOptions()) as writer:
    writer.add_mutations([
        # Create a mutation with all parameters defined. 
        Mutation('row', b'CF', 'cq', 'visibility', 123, b'binaryvalue', False),
        # Create a mutation with keyword arguments
        Mutation('row', cq='cq', value='value'),
        Mutation('row', cf=b'cf', visibility=b'PRIVATE', delete=True),
        Mutation('row', timestamp=123)
    ])
```

Note that `Mutation` will automatically encode all parameters into binary values.

### Scan the table

In the example below, we perform a full table scan.

```python
from accumulo import ScanOptions

# Use the scanner as a context manager to automatically close it. The second parameter is optional.
with connector.create_scanner('tmp', ScanOptions()) as scanner:
    for r in scanner:
        # The scanner returns a facade that provides binary and non-binary accessors for the record properties.
        print(r.row, r.row_bytes, r.cf, r.cf_bytes, r.cq, r.cq_bytes, r.visibility, r.visibility_bytes, r.timestamp, 
              r.value, r.value_bytes)
```

We may alternatively create a batch scanner:

```python
from accumulo import BatchScanOptions

# The second parameter is optional.
with connector.create_batch_scanner('tmp', BatchScanOptions()) as scanner:
    pass
```

The `create_scanner` and `create_batch_scanner` methods respectively accept a `ScanOptions` or `BatchScanOptions`
object as a second parameter.

### Scan with specific authorizations

`ScanOptions` and `BatchScanOptions` both support an `authorizations` keyword argument, which may be used to configure
a scanner with specific authorizations. 
 
Authorizations must be provided as an iterable of binary values. We may use the `AuthorizationSet` class to create a 
`frozenset` of binary values from binary and non-binary arguments.

```python
from accumulo import AuthorizationSet

with connector.create_scanner(
    table='tmp', 
    opts=ScanOptions(
        authorizations=AuthorizationSet({'PRIVATE', 'PUBLIC'})
    )
) as scanner:
    pass
```

### Scan specific columns

`ScanOptions` and `BatchScanOptions` both support a `columns` keyword argument, which may be used to only retrieve 
specific columns. Use the `ScanColumn` class to define column, which include a column family and an optional column
qualifier.

```python
from accumulo import ScanColumn

with connector.create_scanner(
    'tmp',
    ScanOptions(
        columns=[
            # Column family and column qualifier. Accepts binary and non-binary arguments. 
            ScanColumn(b'cf', 'cq'),
            # Column family only.
            ScanColumn('cf'),
        ]
    )
) as scanner:
    pass
```

### Use scan ranges

`ScanOptions` and `BatchScanOptions` respectively accept an optional `range` or `ranges` keyword argument. Use the 
`Key`, `Range`, `RangeExact`, and `RangePrefix` classes to define ranges.

```python
from accumulo import Key, Range, RangePrefix

with connector.create_scanner(
    'tmp',
    ScanOptions(
        # Binary and non-binary arguments are accepted
        range=Range(start_key=Key('aStartKey', b'cf'))
    )
) as scanner:
    pass

with connector.create_scanner(
    'tmp',
    ScanOptions(
        range=Range(end_key=Key('endKey', b'cf'), is_end_key_inclusive=True)
    )
) as scanner:
    pass

with connector.create_scanner(
    'tmp',
    ScanOptions(
        range=Range(start_key=Key('aStartKey', b'cf'), end_key=Key('endKey', 'cf', 'cq'))
    )
) as scanner:
    pass

with connector.create_batch_scanner(
    'tmp',
    BatchScanOptions(
        # batch scanner accepts multiple ranges
        ranges=[
            Range(start_key=Key(b'\xff')),
            RangePrefix('row', 'cf', b'cq'),
            RangePrefix(b'abc', 'cq')
        ]
    )
) as scanner:
    pass
```

### Use an iterator

`ScanOptions` and `BatchScanOptions` both support an `iterators` keyword argument.

```python
from accumulo import IteratorSetting

with connector.create_scanner(
    'tmp',
    ScanOptions(
        iterators=[
            IteratorSetting(priority=30, name='iter', iterator_class='my.iterator', properties={})
        ]
    )
) as scanner:
    pass
```

### Writing asynchronous, non-blocking applications

The examples above are all examples of blocking code. This may be fine for scripts, but it is disadvantageous for 
applications such as web services that need to service client requests concurrently. Fortunately, this library includes
an asynchronous connector that may be used to call the above methods in a non-blocking fashion using Python's
*async/await* syntax.

#### Creating an asynchronous connector

Earlier, we used the `AccumuloProxyConnectionContext` class to create a blocking connector. To create an  asynchronous 
connector, we will use the `AccumuloProxyConnectionPoolContextAsync` class. 

```python
from accumulo import AccumuloProxyConnectionPoolContextAsync

async_conn = await AccumuloProxyConnectionPoolContextAsync().create_connector('sharedSecret')
```

Unlike the blocking connector, the non-blocking connector uses a pool of proxy connection objects, and uses a 
[thread pool executor](https://docs.python.org/3/library/asyncio-eventloop.html#asyncio.loop.run_in_executor) to 
call the low-level bindings outside of the main event loop.

In the example below, we explore some more specific options for configuring an asynchronous connector.

```python
from accumulo import (
    AccumuloProxyConnectionParams, AccumuloProxyConnectionFactory, AsyncAccumuloConnectorPoolExecutor, 
    AccumuloProxyConnectionPoolContextAsync
)

# The executor will generate new proxy connection instances on-demand, up to a limit.
executor = AsyncAccumuloConnectorPoolExecutor(
    proxy_connection_limit=4, 
    proxy_connection_factory=AccumuloProxyConnectionFactory(
        params=AccumuloProxyConnectionParams(hostname='127.0.0.1', port=42424)
    )
)
# A default executor is created if one is not provided.
context = AccumuloProxyConnectionPoolContextAsync(executor)
async_conn = await AccumuloProxyConnectionPoolContextAsync().create_connector('sharedSecret')
```

#### Using writers

```python
async with await async_conn.create_writer('tmp') as writer:
    # Add mutations must be awaited
    await writer.add_mutations([Mutation('Row')])
```

#### Using scanners

```python
async with await async_conn.create_scanner('tmp') as scanner:
    async for record in scanner:
        pass
```

#### Performing other operations

All other connector operations, such as `create_table` or `table_exists`, must similarly be called using the `await`
syntax.

```python
await async_conn.table_exists('tmp')
```

#### Asynchronously call low-level bindings

Use the executor to asynchronously call a low-level binding function. You must provide a getter function that returns
the binding function from a proxy client instance.

```python
# executor.run(gettern_fn, *args)
await executor.run(lambda c: c.tableExists, 'sharedSecret', 'tmp')
```
