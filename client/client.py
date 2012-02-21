# -*- coding: utf-8 -*-
# date: 2012-02-20
# author: notedit<notedit@gmail.com>

import socket
import struct
import bson
from bson import BSON
from Queue import Queue

"""
a simple connection pool
"""

SOCKET_TIMEOUT = 10.0



class BackendError(Exception):

    def __init__(self,message,detail):
        self.message = message
        self.detail = detail

    def __str__(self):
        return 'BackendError(%s,%s)' % (self.message,self.detail)

    def __repr__(self):
        return 'BakcneError(%s,%s)' % (self.message,self.detail)


class RpcError(Exception):
    
    def __init__(self,message,detail):
        self.message = message
        self.detail = detail

    def __str__(self):
        return '%s:%s' % (self.message,self.detail)

    def __repr__(self):
        return '%s,%s' % (self.message,self.detail)

class Connection(object):

    def __init__(self,host='localhost',port=9090):
        self.host = host
        self.port = port
        self._sock = None

    def __del__(self):
        try:
            self.disconnect()
        except:
            pass

    def connect(self):
        if self._sock:
            return
        try:
            sock = self._connect()
        except socket.timeout,e:
            raise RpcError('ConnTimeoutError',str(e))
        except socket.error,e:
            raise RpcError('ConntionError',str(e))
        self._sock = sock

    def _connect(self):
        sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        sock.settimeout(SOCKET_TIMEOUT)
        sock.connect((self.host,self.port))
        return sock

    def disconnect(self):
        if self._sock is None:
            return
        try:
            self._sock.close()
        except socket.error:
            pass
        self._sock = None

    def write_request(self,method,args):
        if not self._sock:
            self.connect()
        try:
            data = self.encode(method,args)
            self._sock.sendall(data)
        except:
            self.disconnect()
            raise

    def read_request(self):
        try:
            m = self._sock.recv(4)
            while len(m) < 4:
                m += self._sock.recv(4-len(m))
            lt = struct.unpack('>i',m)[0]
            ret = self._sock.recv(lt)
            while len(ret) < lt:
                ret += self._sock.recv(lt - len(ret))
            return self.decode(ret)
        except:
            self.disconnect()
            raise

    def decode(self,data):
        ret = bson.BSON(data).decode()
        return ret

    def encode(self,method,args):
        cdict = {'operation':1,'method':method,'argument':args}
        cs = bson.BSON.encode(cdict)
        msghead = struct.pack('>i',len(cs))
        return msghead + cs

    def call(self,method,args):
        try:
            self.write_request(method,args)
            ret = self.read_request()
            if ret.get('operation',None) == 2:
                return ret['reply'] or None
            elif ret.get('operation',None) == 3:
                raise BackendError(ret['reply']['message'],ret['reply']['detail'])
            else:
                raise BackendError('InternalError','unvalid response')
        except BackendError,err:
            raise
        except Exception,err:
            raise BackendError('InternalError',str(err))


class ConnectionPool(object):
    """Generic connection pool"""
    def __init__(self,host='localhost',port=9090,max_connection=10):
        self.host = host
        self.port = port
        self.max_connection = max_connection
        self._connections = self.initconns()

    def initconns(self):
        conns = Queue(self.max_connection)
        #for x in xrange(self.max_connection):
        conns.put(None)
        return conns

    def get_connection(self):
        cn = self._connections.get(True,1) # set the timeout 1 second
        if cn is None:
            cn = Connection(self.host,self.port)
        return cn

    def release(self,cn):
        if not self._connections.full():
            self._connections.put(cn)
        else:
            cn.disconnect()

class Service(object):
    """rpc service"""
    def __init__(self,sname,cliet):
        self.sname = sname
        self.client = client
        self.mdict = {} # a dict 

    def __getattr__(self,method):
        sm = '%s.%s'% (self.sname,method)
        if self.mdict.get(sm,None) is None:
            me = lambda args: self.__call__(sm,args)
            me.__name__ = sm
            self.mdict[sm] = me
            return me
        else:
            return self.mdict.get(sm)

    def __call__(self,method,args):
        #serviceMethod
        conn = self.client.pool.get_connection()
        try:
            ret = conn.call(method,args)
        except BackendError,ex:
            self.client.pool.release(conn)
            raise
        self.client.pool.release(conn)
        return ret
        

class RpcClient(object):
    """rpc client"""
    sdict = {}
    pool = None
    def __init__(self,host='localhost',port=9090):
        self.host = host
        self.port = port
        self.pool = ConnectionPool(host,port)

    def __getattr__(self,service):
        if self.sdict.get(service,None) is None:
            ser = Service(service,self)
            self.sdict[service] = ser
            return ser
        else:
            return self.sdict.get(service)
            



if __name__ == '__main__':
    client = RpcClient(host='localhost',port=9090)
    ret = client.Arith.Add({'a':7,'b':8})
    print 'Arith.Add',ret

    ret = client.Arith.Mul({'a':7,'b':8})
    print 'Arith.Mul',ret
