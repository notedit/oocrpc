// Date: 2012-02-14
// Author: notedit<notedit@gmail.com>

package rpc

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
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
	mutex    sync.Mutex
	Timeout  time.Duration
	freeconn []*conn
}

type conn struct {
	cn net.Conn
	rw *bufio.ReadWriter
	c  *Client
}

func (cn *conn) WriteRequest(req *clientRequest, body interface{}) (err error) {
	rw := cn.rw.Writer
	// write request header
	bys, err := bson.Marshal(req)
	if err != nil {
		log.Println("marshal request header error, ", err.Error())
		return
	}
	_, err = rw.Write(bys)
	if err != nil {
		log.Println("write request header error, ", err.Error())
		return
	}
	// write request body
	bys, err = bson.Marshal(body)
	if err != nil {
		log.Println("marshal request body error, ", err.Error())
		return
	}
	_, err = rw.Write(bys)
	if err != nil {
		log.Println("write request body error, ", err.Error())
	}
	if err = rw.Flush(); err != nil {
		log.Println("write request error, ", err.Error())
	}
	return
}

func (cn *conn) ReadResponse(res *clientResponse, reply interface{}) (err error) {
	if err = cn.ReadResponseHeader(res); err != nil {
		return
	}
	if res.Operation == 3 {
		cn.ReadResponseBody(&struct{}{})
		return errors.New(res.Error)
	}
	err = cn.ReadResponseBody(reply)
	return
}

func (cn *conn) ReadResponseHeader(res *clientResponse) (err error) {
	msgheader := make([]byte, 4)
	n, err := cn.rw.Read(msgheader)
	if n != 4 {
		return io.ErrUnexpectedEOF
	}
	if err != nil {
		res = nil
		if err == io.EOF {
			return errors.New("rpc: client cannot read requestHeader" + err.Error())
		}
		err = errors.New("rpc: client cannot read requestHeader " + err.Error())
		return
	}
	length := binary.LittleEndian.Uint32(msgheader)
	b := make([]byte, length)
	binary.LittleEndian.PutUint32(b, length)
	n, err = io.ReadFull(cn.rw.Reader, b[4:])
	if err != nil {
		if err == io.EOF {
			return io.ErrUnexpectedEOF
		}
		return
	}
	if n != int(length-4) {
		return io.ErrUnexpectedEOF
	}
	err = bson.Unmarshal(b, res)
	return
}

func (cn *conn) ReadResponseBody(reply interface{}) (err error) {
	msgbody := make([]byte, 4)
	n, err := cn.rw.Read(msgbody)
	if n != 4 {
		return io.ErrUnexpectedEOF
	}
	if err != nil {
		if err == io.EOF {
			return io.ErrUnexpectedEOF
		}
		return
	}
	length := binary.LittleEndian.Uint32(msgbody)
	b := make([]byte, length)
	binary.LittleEndian.PutUint32(b, length)
	n, err = io.ReadFull(cn.rw.Reader, b[4:])
	if err != nil {
		if err == io.EOF {
			return io.ErrUnexpectedEOF
		}
		return
	}
	if n != int(length-4) {
		return io.ErrUnexpectedEOF
	}
	err = bson.Unmarshal(b, reply)
	return
}

type clientRequest struct {
	Operation uint8
	Method    string
}

type clientResponse struct {
	Operation uint8
	Error     string
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

func (c *Client) call(req *clientRequest, args interface{}, reply interface{}) (err error) {
	cn, err := c.getConn()
	if err != nil {
		return
	}
	defer c.release(cn)
	if err = cn.WriteRequest(req, args); err != nil {
		return err
	}
	res := &clientResponse{}
	if err = cn.ReadResponse(res, reply); err != nil {
		return
	}
	return
}

func (c *Client) Call(serviceMethod string, args interface{}, reply interface{}) error {
	req := new(clientRequest)
	req.Method = serviceMethod
	req.Operation = uint8(1)
	err := c.call(req, args, reply)
	if err != nil {
		return err
	}
	return nil
}
