from dataclasses import dataclass

from accumulo.thrift import AccumuloProxy


@dataclass
class AccumuloProxyConnectionParams:
    hostname: str = '127.0.0.1'
    port: int = 42424


class AccumuloProxyConnection:
    """Manages an Accumulo Proxy client connection."""

    def __init__(self, params: AccumuloProxyConnectionParams = None):
        if params is None:
            params = AccumuloProxyConnectionParams()
        from thrift.transport import TSocket, TTransport
        from thrift.protocol import TCompactProtocol
        socket = TSocket.TSocket(params.hostname, params.port)
        self._transport = TTransport.TFramedTransport(socket)
        protocol = TCompactProtocol.TCompactProtocol(self._transport)
        self.client = AccumuloProxy.Client(protocol)
        self._transport.open()

    def close(self):
        self._transport.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class AccumuloProxyConnectionFactory:

    def __init__(self, params: AccumuloProxyConnectionParams = None):
        if params is None:
            params = AccumuloProxyConnectionParams()
        self.params = params

    def __call__(self) -> AccumuloProxyConnection:
        return AccumuloProxyConnection(self.params)
