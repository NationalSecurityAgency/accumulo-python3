from io import BytesIO
from typing import Tuple

from accumulo.core.structs import KeyValueFacade

TYPE_WHOLE_ROW_COLUMN = Tuple[bytes, bytes, bytes, int, bytes]


class WholeRowIterator:

    @staticmethod
    def _read_int(stream: BytesIO, size: int):
        data = stream.read(size)
        return int.from_bytes(data, byteorder='big', signed=True)

    @staticmethod
    def _read_java_long(stream: BytesIO) -> int:
        return WholeRowIterator._read_int(stream, 8)

    @staticmethod
    def _read_java_int(stream: BytesIO) -> int:
        return WholeRowIterator._read_int(stream, 4)

    @staticmethod
    def _read_field(stream: BytesIO) -> bytes:
        size = WholeRowIterator._read_java_int(stream)
        return stream.read(size)

    @staticmethod
    def decode_columns(row_data: bytes):
        return WholeRowIterator._decode_columns_from_stream(BytesIO(row_data))

    @staticmethod
    def _read_column(stream: BytesIO) -> TYPE_WHOLE_ROW_COLUMN:
        cf_bytes = WholeRowIterator._read_field(stream)
        cq_bytes = WholeRowIterator._read_field(stream)
        visibility_bytes = WholeRowIterator._read_field(stream)
        timestamp = WholeRowIterator._read_java_long(stream)
        value_bytes = WholeRowIterator._read_field(stream)
        return cf_bytes, cq_bytes, visibility_bytes, timestamp, value_bytes

    @staticmethod
    def _decode_columns_from_stream(stream: BytesIO):
        num_columns = WholeRowIterator._read_java_int(stream)
        columns = []
        for i in range(0, num_columns):
            columns.append(WholeRowIterator._read_column(stream))
        return columns


class WholeRowKeyValueColumnFacade:

    def __init__(self, column: TYPE_WHOLE_ROW_COLUMN):
        self.column = column

    @property
    def cf_bytes(self) -> bytes:
        return self.column[0]

    @property
    def cq_bytes(self) -> bytes:
        return self.column[1]

    @property
    def visibility_bytes(self) -> bytes:
        return self.column[2]

    @property
    def timestamp(self) -> int:
        return self.column[3]

    @property
    def value_bytes(self) -> bytes:
        return self.column[4]

    @property
    def cf(self) -> str:
        return self.cf_bytes.decode()

    @property
    def cq(self) -> str:
        return self.cq_bytes.decode()

    @property
    def visibility(self) -> str:
        return self.visibility_bytes.decode()

    @property
    def value(self) -> str:
        return self.value_bytes.decode()


class WholeRowKeyValueFacade:

    def __init__(self, key_value_facade: KeyValueFacade):
        self.row_bytes = key_value_facade.row_bytes
        self.columns = [
            WholeRowKeyValueColumnFacade(col) for col in WholeRowIterator.decode_columns(key_value_facade.value_bytes)
        ]

    @property
    def row(self):
        return self.row_bytes.decode()
