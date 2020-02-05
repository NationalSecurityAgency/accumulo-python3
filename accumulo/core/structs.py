from collections import defaultdict, namedtuple
from dataclasses import dataclass, field
from typing import Dict, FrozenSet, Iterable, NewType, Optional, Tuple, Union

from accumulo.thrift import ttypes


Encodable = Union[str, bytes]

T_AUTHORIZATION_SET = FrozenSet[bytes]
T_ENCODABLE = Union[str, bytes]
T_ITERATOR_SETTING = Tuple[str, str, int, Dict[str, str]]
T_KEY = Tuple[bytes, bytes, bytes, bytes, int]
T_MUTATION = Tuple[bytes, bytes, bytes, bytes, int, bytes, bool]
T_RANGE = Tuple[Optional[T_KEY], bool, Optional[T_KEY], bool]
T_SCAN_COLUMN = Tuple[bytes, Optional[bytes]]


class Types:
    T_AUTHORIZATION_SET = T_AUTHORIZATION_SET
    T_TIME_TYPE = NewType('TimeType', int)


def encode(encodable: Encodable) -> bytes:
    try:
        return encodable.encode()
    except AttributeError:
        return encodable


def encode_or_none(encodable: Optional[Encodable]) -> Union[bytes, None]:
    if encodable is None:
        return None
    return encode(encodable)


class KeyValueFacade:
    """Facade providing accessors over a low-level key value object."""

    # Prevent __dict__
    __slots__ = ('k', 'v', )

    def __init__(self, k: ttypes.Key, v: bytes):
        self.k = k
        self.v = v

    @property
    def row(self) -> str:
        return self.row_bytes.decode()

    @property
    def row_bytes(self) -> bytes:
        return self.k.row

    @property
    def cf(self) -> str:
        return self.cf_bytes.decode()

    @property
    def cf_bytes(self) -> bytes:
        return self.k.colFamily

    @property
    def cq(self) -> str:
        return self.cq_bytes.decode()

    @property
    def cq_bytes(self) -> bytes:
        return self.k.colQualifier

    @property
    def visibility(self) -> str:
        return self.visibility_bytes.decode()

    @property
    def visibility_bytes(self) -> bytes:
        return self.k.colVisibility

    @property
    def timestamp(self) -> int:
        return self.k.timestamp

    @property
    def value(self) -> str:
        return self.value_bytes.decode()

    @property
    def value_bytes(self) -> bytes:
        return self.v

    def __repr__(self):
        return 'KeyValueFacade(k=%r, v=%r)' % (self.k, self.v)


class AuthorizationSet(frozenset):
    """A set of authorizations. Behaves like a frozenset. Arguments are encoded."""

    __slots__ = ()

    def __new__(cls, auths: Iterable[Encodable]) -> T_AUTHORIZATION_SET:
        return super().__new__(AuthorizationSet, (encode(auth) for auth in auths))

    def __contains__(self, auth: Encodable):
        return super().__contains__(encode(auth))


class Mutation(
    namedtuple('Mutation', [
        'row_bytes', 'cf_bytes', 'cq_bytes', 'visibility_bytes', 'timestamp', 'value_bytes', 'delete'
    ])
):
    """A mutation. Behaves like a tuple with named attributes. Arguments are encoded."""

    __slots__ = ()

    def __new__(cls, row: Encodable = b'', cf: Encodable = b'', cq: Encodable = b'', visibility: Encodable = b'',
                timestamp: Optional[int] = None, value: Encodable = b'', delete: bool = False):
        args = (
            encode(row),
            encode(cf),
            encode(cq),
            encode(visibility),
            timestamp,
            encode(value),
            delete
        )
        return super().__new__(Mutation, *args)


class Key(namedtuple('Key', ['row_bytes', 'cf_bytes', 'cq_bytes', 'visibility_bytes', 'timestamp'])):

    __slots__ = ()

    def __new__(cls, row: Encodable, cf: Optional[Encodable] = None, cq: Optional[Encodable] = None,
                visibility: Optional[Encodable] = None, timestamp: Optional[int] = None):
        args = (
            encode(row),
            encode_or_none(cf),
            encode_or_none(cq),
            encode_or_none(visibility),
            timestamp
        )
        return super().__new__(Key, *args)


class Range(namedtuple('Range', ['start_key', 'is_start_key_inclusive', 'end_key', 'is_end_key_inclusive'])):

    __slots__ = ()

    def __new__(cls, *, start_key: Optional[Key] = None, is_start_key_inclusive: bool = True,
                end_key: Optional[Key] = None, is_end_key_inclusive: bool = False, **_):
        args = (
            start_key,
            is_start_key_inclusive,
            end_key,
            is_end_key_inclusive
        )
        return super().__new__(Range, *args)


class RangeExact(Range):

    def __new__(cls, row: Encodable, cf: Optional[Encodable] = None, cq: Optional[Encodable] = None):
        if cq is not None:
            return Range(
                start_key=Key(row, cf, cq),
                end_key=Key(row, cf, encode(cq) + b'\x00')
            )
        elif cf is not None:
            return Range(
                start_key=Key(row, cf),
                end_key=Key(row, encode(cf) + b'\x00')
            )
        else:
            return Range(
                start_key=Key(row),
                end_key=Key(encode(row) + b'\x00')
            )


class RangePrefix(Range):

    def __new__(cls, row: Encodable, cf: Optional[Encodable] = None, cq: Optional[Encodable] = None):
        if cq is not None:
            return Range(
                start_key=Key(row, cf, cq),
                end_key=Key(row, cf, encode(cq) + b'\xff'),
                is_end_key_inclusive=True
            )
        elif cf is not None:
            return Range(
                start_key=Key(row, cf),
                end_key=Key(row, encode(cf) + b'\xff'),
                is_end_key_inclusive=True
            )
        else:
            return Range(
                start_key=Key(row),
                end_key=Key(encode(row) + b'\xff'),
                is_end_key_inclusive=True
            )


class ScanColumn(namedtuple('ScanColumn', ['cf_bytes', 'cq_bytes'])):

    __slots__ = ()

    def __new__(cls, cf: Encodable, cq: Optional[Encodable] = None):
        args = (encode(cf), encode_or_none(cq), )
        return super().__new__(ScanColumn, *args)


@dataclass
class IteratorSetting:
    priority: int
    name: str
    iterator_class: str
    properties: Dict[str, str] = field(default_factory=dict)


@dataclass
class _BaseScanOptions:

    authorizations: Optional[Iterable[bytes]] = None
    columns: Optional[Iterable[ScanColumn]] = None
    iterators: Optional[Iterable[IteratorSetting]] = None

    def ttype_columns_or_none(self):
        if self.columns is None:
            return None
        return [TTypeFactory.scan_column(c) for c in self.columns]

    def ttype_iterators_or_none(self):
        if self.iterators is None:
            return None
        return [TTypeFactory.iterator_setting(i) for i in self.iterators]


@dataclass
class ScanOptions(_BaseScanOptions):

    range: Optional[Range] = None
    buffer_size: Optional[int] = None

    def ttype_range_or_none(self):
        if self.range is None:
            return None
        return TTypeFactory.range(self.range)


class TimeType:
    LOGICAL = Types.T_TIME_TYPE(ttypes.TimeType.LOGICAL)
    MILLIS = Types.T_TIME_TYPE(ttypes.TimeType.MILLIS)


@dataclass
class BatchScanOptions(_BaseScanOptions):

    ranges: Optional[Iterable[Range]] = None
    threads: Optional[int] = None

    def ttype_ranges_or_node(self):
        if self.ranges is None:
            return None
        return [TTypeFactory.range(r) for r in self.ranges]


@dataclass
class WriterOptions:
    max_memory: Optional[int] = None
    latency_ms: Optional[int] = None
    timeout_ms: Optional[int] = None
    threads: Optional[int] = None
    durability: Optional[int] = None


class TTypeFactory:

    @staticmethod
    def scan_column(c: ScanColumn) -> ttypes.ScanColumn:
        return ttypes.ScanColumn(c.cf_bytes, c.cq_bytes)

    @staticmethod
    def key(k: Key) -> ttypes.Key:
        return ttypes.Key(k.row_bytes, k.cf_bytes, k.cq_bytes, k.timestamp, k.timestamp)

    @staticmethod
    def key_or_none(k: Union[Key, None]) -> Union[ttypes.Key, None]:
        if k is None:
            return None
        return TTypeFactory.key(k)

    @staticmethod
    def range(r: Range) -> ttypes.Range:
        return ttypes.Range(
            TTypeFactory.key_or_none(r.start_key),
            r.is_start_key_inclusive,
            TTypeFactory.key_or_none(r.end_key),
            r.is_end_key_inclusive
        )

    @staticmethod
    def iterator_setting(i: IteratorSetting) -> ttypes.IteratorSetting:
        return ttypes.IteratorSetting(i.priority, i.name, i.iterator_class, i.properties)

    @staticmethod
    def column_update(m: Mutation) -> ttypes.ColumnUpdate:
        return ttypes.ColumnUpdate(m.cf_bytes, m.cq_bytes, m.visibility_bytes, m.timestamp, m.value_bytes, m.delete)

    @staticmethod
    def mutation_index(mutations: Iterable[Mutation]) -> Dict[bytes, Iterable[ttypes.ColumnUpdate]]:
        mutation_index = defaultdict(list)
        for mutation in mutations:
            mutation_index[mutation.row_bytes].append(TTypeFactory.column_update(mutation))
        return mutation_index

    @staticmethod
    def scan_options(opts: ScanOptions) -> ttypes.ScanOptions:
        return ttypes.ScanOptions(
            authorizations=opts.authorizations,
            columns=opts.ttype_columns_or_none(),
            iterators=opts.ttype_iterators_or_none(),
            range=opts.ttype_range_or_none(),
            bufferSize=opts.buffer_size
        )

    @staticmethod
    def batch_scan_options(opts: BatchScanOptions) -> ttypes.BatchScanOptions:
        return ttypes.BatchScanOptions(
            authorizations=opts.authorizations,
            columns=opts.ttype_columns_or_none(),
            iterators=opts.ttype_iterators_or_none(),
            ranges=opts.ttype_ranges_or_node(),
            threads=opts.threads
        )

    @staticmethod
    def writer_options(opts: WriterOptions) -> ttypes.WriterOptions:
        return ttypes.WriterOptions(
            maxMemory=opts.max_memory,
            latencyMs=opts.latency_ms,
            timeoutMs=opts.timeout_ms,
            threads=opts.threads,
            durability=opts.durability  # TODO there is a ttype for this, it is not an int
        )
