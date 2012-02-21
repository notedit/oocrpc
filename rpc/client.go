// Date: 2012-02-14
// Author: notedit<notedit@gmail.com>

package rpc

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"io/ioutil"
	"launchpad.net/mgo/bson"
	"log"
	"net"
	"sync"
	"time"
)

// timeout
const DefaultTimeout = time.Duration(10000) * time.Millisecond

// connection pool number
const DefaultConnectionPool = 10

type Client struct {
	addr     net.Addr
	seq      uint32
	mutex    sync.Mutex
	Timeout  time.Duration
	freeconn []*conn
}

type conn struct {
	cn net.Conn
	rw *bufio.ReadWriter
	c  *Client
}

func (cn *conn) WriteRequest(req *clientRequest) (err error) {
	bys, err := bson.Marshal(req)
	if err != nil {
		log.Println(err)
		return
	}
	req.messageLength = uint32(len(bys))
	// write message header
	rw := cn.rw.Writer
	_, err = rw.Write([]byte{byte(req.messageLength >> 24), byte(req.messageLength >> 16), byte(req.messageLength >> 8), byte(req.messageLength)})
	if err != nil {
		log.Println("write requestHeader error:", err)
		return
	}
	_, err = rw.Write(bys)
	if err != nil {
		return
	}
	if err = rw.Flush(); err != nil {
		log.Println("write requestBody error:", err)
	}
	return
}

func (cn *conn) ReadResponse(res *clientResponse) (err error) {
	if err = cn.ReadResponseHeader(res); err != nil {
		return
	}
	err = cn.ReadResponseBody(res)
	return
}

func (cn *conn) ReadResponseHeader(res *clientResponse) (err error) {
	msgheader := make([]byte, 4)
	_, err = cn.rw.Read(msgheader)
	if err != nil {
		res = nil
		if err == io.EOF {
			return errors.New("rpc: client cannot read requestHeader" + err.Error())
		}
		err = errors.New("rpc: client cannot read requestHeader " + err.Error())
		return
	}
	res.messageLength = binary.BigEndian.Uint32(msgheader)
	return nil
}

func (cn *conn) ReadResponseBody(res *clientResponse) (err error) {
	msgbody, err := ioutil.ReadAll(io.LimitReader(cn.rw.Reader, int64(res.messageLength)))
	if err != nil {
		err = errors.New("rpc: client cannot read requestBody " + err.Error())
		return
	}
	if err = bson.Unmarshal(msgbody, res); err != nil {
		return
	}
	return
}

type clientRequest struct {
	messageLength uint32 // unexported element will not be marshaled
	Operation     uint8
	Method        string
	Argument      interface{}
}

type clientResponse struct {
	messageLength uint32
	Operation     uint8
	Reply         bson.Raw
}

func New(server string) *Client {
	addr, err := net.ResolveTCPAddr("tcp", server)
	if err != nil {
		panic(err)
	}
	return &Client{addr: addr, freeconn: make([]*conn, 0, DefaultConnectionPool)}
}

func (c *Client) dial() (net.Conn, error) {
	cn, err := net.Dial(c.addr.Network(), c.addr.String())
	if err != nil {
		return nil, err
	}
	return cn, nil
}

func (c *Client) getConn() (*conn, error) {
	cn, ok := c.getFreeConn()
	if ok {
		return cn, nil
	}

	nc, err := c.dial()
	if err != nil {
		return nil, err
	}

	return &conn{
		cn: nc,
		rw: bufio.NewReadWriter(bufio.NewReader(nc), bufio.NewWriter(nc)),
		c:  c,
	}, nil
}

func (c *Client) getFreeConn() (*conn, bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if len(c.freeconn) == 0 {
		return nil, false
	}
	cn := c.freeconn[len(c.freeconn)-1]
	c.freeconn = c.freeconn[:len(c.freeconn)-1]
	return cn, true
}

func (c *Client) release(cn *conn) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if len(c.freeconn) >= DefaultConnectionPool {
		cn.cn.Close()
		return
	}
	c.freeconn = append(c.freeconn, cn)
}

func (c *Client) call(req *clientRequest, reply interface{}) (err error) {
	cn, err := c.getConn()
	if err != nil {
		return
	}
	defer func() {
		if cn != nil {
			c.release(cn)
		}
		switch err.(type) {
		case BackendError:
		case error:
			err = BackendError{Message: "ClientError", Detail: err.Error()}
		default:
		}
	}()
	if err = cn.WriteRequest(req); err != nil {
		return err
	}
	res := &clientResponse{}
	if err = cn.ReadResponse(res); err != nil {
		return
	}
	if err = parseReply(res, reply); err != nil {
		return
	}
	return nil
}

func parseReply(res *clientResponse, reply interface{}) error {
	if res.Operation == 2 {
		// valid reply
		err := res.Reply.Unmarshal(reply)
		//if err != nil {
		//    e.Message = "UnvalidUnmarshalError"
		//    e.Detail = err.Error()
		//    return e
		//}
		return err
	} else if res.Operation == 3 {
		// error reply
		e := &BackendError{}
		err := res.Reply.Unmarshal(e)
		if err != nil {
			e.Message = "ClientUnmarshalError"
			e.Detail = err.Error()
		}
		return *e
	}
	return BackendError{
		Message: "UnvalidOperationError",
		Detail:  "unvalid oparation error, it may be 2 or 3",
	}
}

func (c *Client) Call(serviceMethod string, args interface{}, reply interface{}) error {
	req := new(clientRequest)
	req.Method = serviceMethod
	req.Argument = args
	req.Operation = uint8(1)
	err := c.call(req, reply)
	if err != nil {
		return err
	}
	return nil
}
