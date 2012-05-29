# -*- coding: utf-8 -*-
# date: 2012-02-20
# author: notedit<notedit@gmail.com>

import socket
import struct
import bson

try:
    import cbson
    decode_document = cbson.decode_next
except ImportError:
    from bson import codec
    decode_document = codec.decode_document

"""
a simple connection client
"""

SOCKET_TIMEOUT = 10.0

len_struct = struct.Struct('<i')
unpack_length = len_struct.unpack_from
len_struct_size = len_struct.size
default_read_buffer_size = 8192

class ConnectionError(Exception):
    pass

class RpcError(Exception):
    
    def __init__(self,message):
        self.message = message

    def __str__(self):
        return self.message

    def __repr__(self):
        return self.message,self.detail

class BackendError(RpcError):
    pass


class Request(object):
    header = None
    body = None
    def __init__(self,method,args):
        self._operation = 1
        self._method = method
        self.body = args
        self.header = {'operation':self._operation,
                        'method':self._method}
    
    def encode_request(self):
        try:
            return bson.dumps(self.header) + bson.dumps(self.body)
        except Exception,ex:
            errstr = traceback.format_exc()
            raise RpcError('EncodeError:%s'%errstr)

class Response(object):
    
    header = None
    reply = None

    @property
    def error(self):
        return self.header.get('error')

    def decode_response(self,data):
        try:
            offset,self.header = decode_document(data,0)
            offset,self.reply = decode_document(data,offset)
        except Exception,ex:
            errstr = traceback.format_exc()
            raise RpcError('DecodeError:%s'%errstr)


class Connection(object):

    def __init__(self,host='localhost',port=9090,timeout=10.0):
        self.host = host
        self.port = port
        self.timeout = timeout
        self._conn = None

    def __del__(self):
        try:
            self.close()
        except:
            pass

    @property
    def conn(self):
        if self._conn:
            return self._conn
        try:
            sock = self.connect()
        except socket.timeout,e:
            raise ConnectionError('can not connect to %s:%d'%(self.host,self.port))
        except socket.error,e:
            raise ConnectionError('can not connect to %s:%d'%(self.host,self.port))
        self._conn = sock
        return self._conn

    def connect(self):
        sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        sock.settimeout(self.timeout)
        sock.connect((self.host,self.port))
        return sock

    def reconnect(self):
        self.close()
        try:
            sock = self.connect()
        except:
            raise ConnectionError('can not connect to %s:%d'%(self.host,self.port))
        self._conn = sock

    def close(self):
        if self._conn is None:
            return
        try:
            self._conn.close()
        except socket.error:
            pass
        self._conn = None

    def write_request(self,method,args):
        request = Request(method,args)
        data = request.encode_request()
        try:
            self.conn.sendall(data)
        except socket.error,ex:
            self.reconnect()
            self.conn.sendall(data)
        except socket.timeout,ex:
            self.reconnect()
            self.conn.sendall(data)

    def read_response(self):
        try:
            buf = []
            buf_write = buf.append
            data,data_len = self._read_more(buf,buf_write)
            while data_len < len_struct_size:
                data,data_len = self._read_more(buf,buf_write)
            # header's length
            header_len = unpack_length(data)[0]
            while data_len < (header_len + len_struct_size):
                data,data_len = self._read_more(buf,buf_write)
            # body's length
            body_len = unpack_length(data,header_len)[0]
            total_len = header_len + body_len
            while data_len < total_len:
                data,data_len = self._read_more(buf,buf_write)
            res = Response()
            res.decode_response(data)
            return res
        except struct.error,e:
            self.close()
            raise RpcError('can not unpack the response')

    def _read_more(self,buf,buf_write):
        try:
            data = self.conn.recv(default_read_buffer_size)
            if not data:
                raise ConnectionError('unexpected EOF in read')
        except socket.error:
            raise ConnectionError('unexpected recv error')
        except socket.timeout:
            raise ConnectionError('unexpected timeout error')
        if buf:
            buf_write(data)
            data = ''.join(buf)
        else:
            buf_write(data)
        return data,len(data)


class RpcClient(object):
    """rpc client"""
    def __init__(self,host='localhost',port=9090):
        self.host = host
        self.port = port
        self.conn = Connection(self.host,self.port)

    def __getattr__(self,funcname):
        func = lambda args:self.__call__(funcname,args)
        func.__name__ = funcname
        return func

    def __call__(self,method,args):
        if not isinstance(args,dict):
            raise RpcError("args should be dict type")
        self.conn.write_request(method,args)
        res = self.conn.read_response()
        if res.error:
            raise RpcError(res.error)
        if res.reply.has_key('_'):
            return res.reply['_']
        return res.reply

if __name__ == '__main__':
    client = RpcClient(host='localhost',port=9090)
    ret = client.Add({'a':7,'b':9})
    print 'Arith.Add',ret

    ret = client.Mul({'a':7,'b':8})
    print 'Arith.Mul',ret
