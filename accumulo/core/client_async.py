import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from typing import AsyncIterator, Callable, Iterable, Optional

from accumulo.core.client import AccumuloContextBase, AccumuloConnectorBase, AccumuloScannerBase, AccumuloWriterBase
from accumulo.core.connection import AccumuloProxyConnection, AccumuloProxyConnectionFactory
from accumulo.core.structs import BatchScanOptions, KeyValueFacade, Mutation, ScanOptions, TTypeFactory, \
    WriterOptions, Types, TimeType, AuthorizationSet
from accumulo.thrift import AccumuloProxy, ttypes


class AsyncAccumuloConnectorPool:
    """Manages a pool of proxy connections."""

    def __init__(self):
        self._queue = None

    async def _get_queue(self) -> asyncio.Queue:
        if not self._queue:
            self._queue = asyncio.Queue()
        return self._queue

    async def get(self) -> AccumuloProxyConnection:
        """Get the next available proxy connection from the pool."""
        queue = await self._get_queue()
        proxy_connection = await queue.get()
        return proxy_connection

    async def put(self, proxy_connection: AccumuloProxyConnection):
        """Add a proxy connection to the pool."""
        queue = await self._get_queue()
        await queue.put(proxy_connection)

    @asynccontextmanager
    async def get_proxy_client_context(self):
        """Provides a context manager around the next-available proxy client."""
        proxy_connection = await self.get()
        try:
            yield proxy_connection.client
        finally:
            await self.put(proxy_connection)


class AsyncAccumuloConnectorPoolAutoScaling(AsyncAccumuloConnectorPool):

    def __init__(
            self, *,
            # keyword-only arguments
            proxy_connection_limit: Optional[int] = None,
            proxy_connection_factory: Optional[Callable[[], AccumuloProxyConnection]] = None
    ):
        if proxy_connection_limit is None:
            proxy_connection_limit = 1
        if proxy_connection_factory is None:
            proxy_connection_factory = AccumuloProxyConnectionFactory()
        self._proxy_connections = []
        self._proxy_connection_limit = proxy_connection_limit
        self._proxy_connection_factory = proxy_connection_factory
        super().__init__()

    async def _add_proxy_connection(self):
        """Generates a new proxy connection and adds it to the pool."""
        proxy_connection = self._proxy_connection_factory()
        self._proxy_connections.append(proxy_connection)
        await self.put(proxy_connection)

    async def get(self) -> AccumuloProxyConnection:
        """Returns the next-available proxy connection.

        Adds a new proxy connection to the pool if the pool is empty and the proxy connection limit has not been
        reached.
        """
        queue = await super()._get_queue()
        if queue.empty() and len(self._proxy_connections) < self._proxy_connection_limit:
            await self._add_proxy_connection()
        return await super().get()

    async def close(self):
        """Close all proxy connections in the pool."""
        for proxy_connection in self._proxy_connections:
            proxy_connection.close()


class BaseAsyncAccumuloConnectorPoolExecutor:

    def __init__(self, thread_pool_executor: ThreadPoolExecutor, proxy_client_context_fn):
        self._thread_pool_executor: ThreadPoolExecutor = thread_pool_executor
        self._proxy_client_context_fn = proxy_client_context_fn

    async def run(self, proxy_client_fn_getter: Callable[[AccumuloProxy.Client], Callable], *args):
        """Asynchronously executes a thrift function against the next available proxy client."""
        # Get the next available proxy client instance
        async with self._proxy_client_context_fn() as proxy_client:
            proxy_client: AccumuloProxy.Client = proxy_client
            # Get the proxy client function from the proxy client instance
            proxy_client_fn = proxy_client_fn_getter(proxy_client)
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(self._thread_pool_executor, proxy_client_fn, *args)


class AsyncAccumuloConnectorPoolExecutor(BaseAsyncAccumuloConnectorPoolExecutor):

    def __init__(
            self, *,
            # keyword-only arguments
            proxy_connection_limit: Optional[int] = None,
            proxy_connection_factory: Optional[Callable[[], AccumuloProxyConnection]] = None,
            max_threads: Optional[int] = None
    ):
        if proxy_connection_limit is None:
            proxy_connection_limit = 1
        if proxy_connection_factory is None:
            proxy_connection_factory = AccumuloProxyConnectionFactory()
        if max_threads is None:
            max_threads = proxy_connection_limit
        self._thread_pool_executor = ThreadPoolExecutor(
            max_workers=max_threads
        )
        self._proxy_connection_pool = AsyncAccumuloConnectorPoolAutoScaling(
            proxy_connection_limit=proxy_connection_limit,
            proxy_connection_factory=proxy_connection_factory
        )
        super().__init__(self._thread_pool_executor, self._proxy_connection_pool.get_proxy_client_context)

    def close(self):
        self._thread_pool_executor.shutdown()
        self._proxy_connection_pool.close()

    def context(self):
        return AccumuloProxyConnectionPoolContextAsync(self)


class AsyncAccumuloConnector(AccumuloConnectorBase):

    def __init__(self, proxy_connection_pool_executor: AsyncAccumuloConnectorPoolExecutor, shared_secret: bytes):
        super().__init__(shared_secret)
        self.proxy_connection_pool_executor = proxy_connection_pool_executor

    async def create_scanner(self, table: str, opts: Optional[ScanOptions] = None):
        if opts is None:
            opts = ScanOptions()
        opts = TTypeFactory.scan_options(opts)
        resource_id = await self.proxy_connection_pool_executor.run(AccumuloProxyClientFunctionGetters.create_scanner,
                                                                    self.shared_secret, table, opts)
        return AsyncAccumuloScanner(self.proxy_connection_pool_executor, resource_id)

    async def create_batch_scanner(self, table: str, opts: Optional[BatchScanOptions] = None):
        if opts is None:
            opts = BatchScanOptions()
        opts = TTypeFactory.batch_scan_options(opts)
        resource_id = await self.proxy_connection_pool_executor.run(
            AccumuloProxyClientFunctionGetters.create_batch_scanner, self.shared_secret, table, opts)
        return AsyncAccumuloScanner(self.proxy_connection_pool_executor, resource_id)

    async def create_writer(self, table: str, opts: Optional[WriterOptions] = None):
        if opts is None:
            opts = WriterOptions()
        opts = TTypeFactory.writer_options(opts)
        resource_id = await self.proxy_connection_pool_executor.run(AccumuloProxyClientFunctionGetters.create_writer,
                                                                    self.shared_secret, table, opts)
        return AsyncAccumuloWriter(self.proxy_connection_pool_executor, resource_id)

    async def change_user_authorizations(self, user: str, auths: Types.T_AUTHORIZATION_SET):
        await self.proxy_connection_pool_executor.run(AccumuloProxyClientFunctionGetters.change_user_authorizations,
                                                      self.shared_secret, user, auths)

    async def get_user_authorizations(self, user: str) -> Types.T_AUTHORIZATION_SET:
        auths = await self.proxy_connection_pool_executor.run(
            AccumuloProxyClientFunctionGetters.get_user_authorizations, user)
        return AuthorizationSet(auths)

    async def create_table(self, table: str, version_iter: bool = True, time_type: Types.T_TIME_TYPE = TimeType.MILLIS):
        await self.proxy_connection_pool_executor.run(AccumuloProxyClientFunctionGetters.create_table, self.shared_secret,
                                                      table, version_iter, time_type)

    async def table_exists(self, table: str) -> bool:
        return await self.proxy_connection_pool_executor.run(AccumuloProxyClientFunctionGetters.table_exists,
                                                             self.shared_secret, table)


class AsyncAccumuloConnectorResource:

    def __init__(self, proxy_connection_pool_executor: AsyncAccumuloConnectorPoolExecutor, resource_id: str):
        self.proxy_connection_pool_executor = proxy_connection_pool_executor
        self.resource_id = resource_id

    async def close(self):
        raise NotImplementedError()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


class AsyncAccumuloScanner(AccumuloScannerBase, AsyncAccumuloConnectorResource):

    async def close(self):
        await self.proxy_connection_pool_executor.run(
            AccumuloProxyClientFunctionGetters.close_scanner,
            self.resource_id
        )

    def __aiter__(self) -> AsyncIterator[KeyValueFacade]:
        return self

    async def __anext__(self) -> KeyValueFacade:
        """Asynchronously return the next record."""
        try:
            kv: ttypes.KeyValueAndPeek = await self.proxy_connection_pool_executor.run(
                AccumuloProxyClientFunctionGetters.next_entry, self.resource_id)
            return KeyValueFacade(kv.keyValue.key, kv.keyValue.value)
        except ttypes.NoMoreEntriesException:
            raise StopAsyncIteration()


class AsyncAccumuloWriter(AccumuloWriterBase, AsyncAccumuloConnectorResource):

    async def close(self):
        await self.proxy_connection_pool_executor.run(
            AccumuloProxyClientFunctionGetters.close_writer,
            self.resource_id
        )

    async def add_mutations(self, mutations: Iterable[Mutation]):
        mutation_index = TTypeFactory.mutation_index(mutations)
        await self.proxy_connection_pool_executor.run(AccumuloProxyClientFunctionGetters.update, self.resource_id,
                                                      mutation_index)


class AccumuloProxyConnectionPoolContextAsync(AccumuloContextBase):

    def __init__(self, proxy_connection_pool_executor: Optional[AsyncAccumuloConnectorPoolExecutor] = None):
        if proxy_connection_pool_executor is None:
            proxy_connection_pool_executor = AsyncAccumuloConnectorPoolExecutor()
        self.proxy_connection_pool_executor = proxy_connection_pool_executor

    async def create_connector(self, shared_secret: bytes) -> AsyncAccumuloConnector:
        return AsyncAccumuloConnector(self.proxy_connection_pool_executor, shared_secret)


class AccumuloProxyClientFunctionGetters:

    @staticmethod
    def create_scanner(c: AccumuloProxy.Client):
        return c.createScanner

    @staticmethod
    def create_batch_scanner(c: AccumuloProxy.Client):
        return c.createBatchScanner

    @staticmethod
    def create_writer(c: AccumuloProxy.Client):
        return c.createWriter

    @staticmethod
    def next_entry(c: AccumuloProxy.Client):
        return c.nextEntry

    @staticmethod
    def close_scanner(c: AccumuloProxy.Client):
        return c.closeScanner

    @staticmethod
    def close_writer(c: AccumuloProxy.Client):
        return c.closeWriter

    @staticmethod
    def update(c: AccumuloProxy.Client):
        # i.e. add mutations
        return c.update

    @staticmethod
    def change_user_authorizations(c: AccumuloProxy.Client):
        return c.changeUserAuthorizations

    @staticmethod
    def get_user_authorizations(c: AccumuloProxy.Client):
        return c.getUserAuthorizations

    @staticmethod
    def create_table(c: AccumuloProxy.Client):
        return c.createTable

    @staticmethod
    def table_exists(c: AccumuloProxy.Client):
        return c.tableExists
