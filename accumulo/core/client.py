from typing import Iterable

from accumulo.core.structs import BatchScanOptions, Mutation, ScanOptions, TimeType, Types, WriterOptions


class AccumuloContextBase:

    def create_connector(self, shared_secret: bytes):
        raise NotImplementedError


class AccumuloConnectorBase:

    def __init__(self, shared_secret: bytes):
        self.shared_secret = shared_secret

    def create_scanner(self, table: str, options: ScanOptions):
        raise NotImplementedError

    def create_batch_scanner(self, table: str, options: BatchScanOptions):
        raise NotImplementedError

    def create_writer(self, table: str, options: WriterOptions):
        raise NotImplementedError

    def change_user_authorizations(self, user: str, auths: Types.T_AUTHORIZATION_SET):
        raise NotImplementedError

    def get_user_authorizations(self, user: str) -> Types.T_AUTHORIZATION_SET:
        raise NotImplementedError

    def create_table(self, table: str, version_iter: bool = True, time_type: Types.T_TIME_TYPE = TimeType.MILLIS):
        raise NotImplementedError

    def table_exists(self, table: str) -> bool:
        raise NotImplementedError


class AccumuloScannerBase:

    def close(self):
        raise NotImplementedError


class AccumuloWriterBase:

    def close(self):
        raise NotImplementedError

    def add_mutations(self, mutations: Iterable[Mutation]):
        raise NotImplementedError
