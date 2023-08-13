from typing import Iterable, Iterator, Optional

from accumulo.core.client import AccumuloContextBase, AccumuloConnectorBase, AccumuloScannerBase, AccumuloWriterBase
from accumulo.core.connection import AccumuloProxyConnection
from accumulo.core.structs import (BatchScanOptions, KeyValueFacade, Mutation, ScanOptions, TimeType, TTypeFactory,
                                   Types, WriterOptions, AuthorizationSet)
from accumulo.thrift import AccumuloProxy, ttypes


class AccumuloProxyConnectionContext(AccumuloContextBase):

    def __init__(self, proxy_connection: Optional[AccumuloProxyConnection] = None):
        # Create a default proxy connection if one is not provided.
        if proxy_connection is None:
            proxy_connection = AccumuloProxyConnection()
        self.proxy_connection = proxy_connection

    def create_connector(self, shared_secret: bytes):
        return AccumuloConnector(self.proxy_connection.client, shared_secret)


class AccumuloConnector(AccumuloConnectorBase):

    def __init__(self, proxy_client: AccumuloProxy.Client, shared_secret: bytes):
        super().__init__(shared_secret)
        self.proxy_client = proxy_client

    def create_scanner(self, table: str, opts: Optional[ScanOptions] = None):
        if opts is None:
            opts = ScanOptions()
        opts = TTypeFactory.scan_options(opts)
        return AccumuloScanner(self.proxy_client, self.shared_secret, self.proxy_client.createScanner(self.shared_secret, table, opts))

    def create_batch_scanner(self, table: str, opts: Optional[BatchScanOptions] = None):
        if opts is None:
            opts = BatchScanOptions()
        opts = TTypeFactory.batch_scan_options(opts)
        return AccumuloScanner(self.proxy_client, self.shared_secret, self.proxy_client.createBatchScanner(self.shared_secret, table,
                                                                                                   opts))

    def create_writer(self, table: str, opts: Optional[WriterOptions] = None):
        if opts is None:
            opts = WriterOptions()
        opts = TTypeFactory.writer_options(opts)
        return AccumuloWriter(self.proxy_client, self.shared_secret, self.proxy_client.createWriter(self.shared_secret, table, opts))

    def change_user_authorizations(self, user: str, auths: Types.T_AUTHORIZATION_SET):
        self.proxy_client.changeUserAuthorizations(self.shared_secret, user, auths)

    def get_user_authorizations(self, user: str) -> Types.T_AUTHORIZATION_SET:
        return AuthorizationSet(self.proxy_client.getUserAuthorizations(self.shared_secret, user))

    def create_table(self, table: str, version_iter: bool = True, time_type: Types.T_TIME_TYPE = TimeType.MILLIS):
        self.proxy_client.createTable(self.shared_secret, table, version_iter, time_type)

    def table_exists(self, table: str) -> bool:
        return self.proxy_client.tableExists(self.shared_secret, table)


class AccumuloConnectorResource:

    def __init__(self, client: AccumuloProxy.Client, shared_secret: bytes, resource_id: str):
        self.client = client
        self.shared_secret = shared_secret
        self.resource_id = resource_id

    def close(self):
        raise NotImplementedError()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class AccumuloScanner(AccumuloScannerBase, AccumuloConnectorResource):

    def close(self):
        self.client.closeScanner(self.resource_id)

    def __iter__(self) -> Iterator[KeyValueFacade]:
        return self

    def __next__(self) -> KeyValueFacade:
        try:
            kv: ttypes.KeyValueAndPeek = self.client.nextEntry(self.resource_id)
            return KeyValueFacade(kv.keyValue.key, kv.keyValue.value)
        except ttypes.NoMoreEntriesException:
            raise StopIteration()


class AccumuloWriter(AccumuloWriterBase, AccumuloConnectorResource):

    def close(self):
        self.client.closeWriter(self.resource_id)

    def add_mutations(self, mutations: Iterable[Mutation]):
        mutation_index = TTypeFactory.mutation_index(mutations)
        self.client.update(self.resource_id, mutation_index)
