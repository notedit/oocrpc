// Date: 2012-02-08
// Author: notedit <notedit@gmail.com>
// make a go rpc service

package rpc

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"reflect"
	"strings"
	"sync"
	"unicode"
	// "runtime"
	"unicode/utf8"
    "oocrpc/bson"
)

var typeOfError = reflect.TypeOf((*error)(nil)).Elem()
var invalidRequest = struct{}{}
var nilRequestBody = &struct{}{}


type methodType struct {
	method    reflect.Method
	ArgType   reflect.Type
	ReplyType reflect.Type
}

type service struct {
	name   string
	rcvr   reflect.Value
	typ    reflect.Type
	method map[string]*methodType
}

// rpc server
type Server struct {
	mu         sync.Mutex
	serviceMap map[string]*service
	allMethod  map[string]*methodType // for python client
    methodServiceMap map[string]*service // for python client
	listener   *net.TCPListener
	reqLock    sync.Mutex
	freeReq    *serverRequest
	respLock   sync.Mutex
	freeResp   *serverResponse
}

// operation has three values -- call:1  reply:2  error:3

// request
type serverRequest struct {
	next      *serverRequest // unexported
	Operation uint8
	Method    string
}

// response
type serverResponse struct {
	next      *serverResponse // unexported
	Operation uint8
	Error     string
}

// decode request and encode response
type ServerCodec struct {
	cn net.Conn
	rw *bufio.ReadWriter
}

// read the request header
func (c *ServerCodec) ReadRequestHeader(req *serverRequest) (err error) {
	msgheader := make([]byte, 4)
	n, err := io.ReadFull(c.rw.Reader, msgheader)
	if err != nil {
		return
	}
	if n != 4 {
		return io.ErrUnexpectedEOF
	}
	length := binary.LittleEndian.Uint32(msgheader)
	b := make([]byte, length)
	binary.LittleEndian.PutUint32(b, length)
	n, err = io.ReadFull(c.rw.Reader, b[4:])
	if err != nil {
		if err == io.EOF {
			return io.ErrUnexpectedEOF
		}
		return err
	}
	if n != int(length-4) {
		return io.ErrUnexpectedEOF
	}
	if err = bson.Unmarshal(b, req); err != nil {
		return
	}
	return
}

// read the request body
func (c *ServerCodec) ReadRequestBody(body interface{}) (err error) {
	msgbody := make([]byte, 4)
	n, err := io.ReadFull(c.rw.Reader, msgbody)
	if err != nil {
		return
	}
	if n != 4 {
		return io.ErrUnexpectedEOF
	}
	length := binary.LittleEndian.Uint32(msgbody)
	b := make([]byte, length)
	binary.LittleEndian.PutUint32(b, length)
	n, err = io.ReadFull(c.rw.Reader, b[4:])
	if err != nil {
		if err == io.EOF {
			return io.ErrUnexpectedEOF
		}
		return
	}
    if err = bson.Unmarshal(b,body); err != nil {
		return
	}
	return
}

func (c *ServerCodec) WriteResponse(res *serverResponse, body interface{}) (err error) {

	bys, err := bson.Marshal(res)
	if err != nil {
		log.Println("marshal response header error", err)
		return
	}

	// write message header
	rw := c.rw.Writer
	_, err = rw.Write(bys)
	if err != nil {
		log.Println("write responseheader error", err)
		return
	}
	// write message body
    bys, err = bson.Marshal(body)
	if err != nil {
		log.Println("marshal response body error", err)
		return
	}
	_, err = rw.Write(bys)
	if err != nil {
		log.Println("write response body error", err)
	}
	if err = rw.Flush(); err != nil {
		log.Println("flush responseBody error", err)
	}
	return
}

// todo
func (c *ServerCodec) Close() error {
	return c.cn.Close()
}

// Is this an exported - upper case 
func isExported(name string) bool {
	rune, _ := utf8.DecodeRuneInString(name)
	return unicode.IsUpper(rune)
}

// Is this typoe exported or a builtin?
func isExportedOrBuiltinType(t reflect.Type) bool {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	return isExported(t.Name()) || t.PkgPath() == ""
}

// Register a service
func (server *Server) Register(rcvr interface{}) error {
	return server.register(rcvr, "", false)
}

// Register a sevice with a name
func (server *Server) RegisterName(name string, rcvr interface{}) error {
	return server.register(rcvr, name, true)
}

// the real register
func (server *Server) register(rcvr interface{}, name string, useName bool) error {
	server.mu.Lock()
	defer server.mu.Unlock()
	if server.serviceMap == nil {
		server.serviceMap = make(map[string]*service)
	}
	s := new(service)
	s.typ = reflect.TypeOf(rcvr)
	s.rcvr = reflect.ValueOf(rcvr)
	sname := reflect.Indirect(s.rcvr).Type().Name()
	if useName {
		sname = name
	}
	if sname == "" {
		log.Fatal("rpc: no service name for type", s.typ.String())
	}
	if !isExported(sname) && !useName {
		s := "rpc Register: type " + sname + " is not exported"
		log.Print(s)
		return errors.New(s)
	}
	if _, present := server.serviceMap[sname]; present {
		return errors.New("rpc: service already defined: " + sname)
	}
	s.name = sname
	s.method = make(map[string]*methodType)

	// Install the methods
	for m := 0; m < s.typ.NumMethod(); m++ {
		method := s.typ.Method(m)
		mtype := method.Type
		mname := method.Name
		if method.PkgPath != "" {
			fmt.Println(method.PkgPath)
			continue
		}

		//Method needs three ins
		if mtype.NumIn() != 3 {
			log.Println("method needs three ins")
			continue
		}

		// Method has one out:error
		if mtype.NumOut() != 1 {
			log.Println("method", mname, "has wrong number of outs:", mtype.NumOut())
			continue
		}

		// first arg need not be a pointer
		argType := mtype.In(1)
		if !isExportedOrBuiltinType(argType) {
			log.Println(mname, "argument type not exported or local", argType)
			continue
		}

		replyType := mtype.In(2)
		if replyType.Kind() != reflect.Ptr {
			log.Println("method", mname, " reply type not a pointer:", replyType)
			continue
		}

		if !isExportedOrBuiltinType(replyType) {
			log.Println("method ", mname, "reply type not exported or local", replyType)
			continue
		}

		// error type
		if returnType := mtype.Out(0); returnType != typeOfError {
			log.Println("method", mname, " returns", returnType.String(), "not error")
			continue
		}

		s.method[mname] = &methodType{method: method, ArgType: argType, ReplyType: replyType}

		// register the method in server's allMethod, for python client
		if _, ok := server.allMethod[mname]; ok {
			log.Println("method", mname, "  already exisit")
			return errors.New("method " + mname + "  already exisit")
		}
		server.allMethod[mname] = &methodType{method: method, ArgType: argType, ReplyType: replyType}
        server.methodServiceMap[mname] = s
	}

	if len(s.method) == 0 {
		ss := "rpc Register: type " + sname + " has no exported methods of suitable type"
		log.Println(ss)
		return errors.New(ss)
	}
	server.serviceMap[s.name] = s
	return nil
}

func NewServer(host string, port uint) *Server {
	addr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		log.Fatal("rpc error:", err.Error())
	}
	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		log.Fatal("rpc error:", err.Error())
	}
	return &Server{
		serviceMap: make(map[string]*service),
		allMethod:  make(map[string]*methodType),
        methodServiceMap: make(map[string]*service),
		listener:   listener,
	}
}

// request and response pool

func (server *Server) getRequest() *serverRequest {
	server.reqLock.Lock()
	req := server.freeReq
	if req == nil {
		req = new(serverRequest)
	} else {
		server.freeReq = req.next
		*req = serverRequest{}
	}
	server.reqLock.Unlock()
	return req
}

func (server *Server) freeRequest(req *serverRequest) {
	server.reqLock.Lock()
	req.next = server.freeReq
	server.freeReq = req
	server.reqLock.Unlock()
}

func (server *Server) getResponse() *serverResponse {
	server.respLock.Lock()
	resp := server.freeResp
	if resp == nil {
		resp = new(serverResponse)
	} else {
		server.freeResp = resp.next
		*resp = serverResponse{}
	}
	server.respLock.Unlock()
	return resp
}

func (server *Server) freeResponse(resp *serverResponse) {
	server.respLock.Lock()
	resp.next = server.freeResp
	server.freeResp = resp
	server.respLock.Unlock()
}

// serv 
func (server *Server) Serv() {

	for {
		c, err := server.listener.Accept()
		if err != nil {
			log.Print("rpc:", err.Error())
			continue
		}
		go server.ServeConn(c)
	}

}

func (server *Server) ServeConn(conn net.Conn) {
	src := &ServerCodec{
		cn: conn,
		rw: bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn)),
	}
	server.ServeCodec(src)
}

func (server *Server) ServeCodec(codec *ServerCodec) {
	sending := new(sync.Mutex)
	for {
		service, mtype, req, argv, replyv, keepReading, err := server.readRequest(codec)
		if err != nil {
			if err != io.EOF {
				log.Println(err)
			}
			if !keepReading {
				break
			}
			// we just got the req
			if req != nil {
				server.sendResponse(sending, req, invalidRequest, codec, err.Error())
				server.freeRequest(req)
			}
			continue
		}
		go service.call(server, sending, mtype, req, argv, replyv, codec)
	}
	codec.Close()
}

func (server *Server) readRequest(codec *ServerCodec) (service *service, mtype *methodType, req *serverRequest, argv reflect.Value, replyv reflect.Value, keepReading bool, err error) {
	service, mtype, req, keepReading, err = server.readRequestHeader(codec)
	if err != nil {
		if !keepReading {
			return
		}
		// just discard body
		codec.ReadRequestBody(nilRequestBody)
		return
	}

	argIsValue := false
	if mtype.ArgType.Kind() == reflect.Ptr {
		argv = reflect.New(mtype.ArgType.Elem())
	} else {
		argv = reflect.New(mtype.ArgType)
		argIsValue = true
	}

	// argv guaranteed to be a pointer 
	if err = codec.ReadRequestBody(argv.Interface()); err != nil {
		return
	}
	if argIsValue {
		argv = argv.Elem()
	}
	replyv = reflect.New(mtype.ReplyType.Elem())
	return
}

func (server *Server) readRequestHeader(codec *ServerCodec) (service *service, mtype *methodType, req *serverRequest, keepReading bool, err error) {
	req = server.getRequest()
	err = codec.ReadRequestHeader(req)
	if err != nil {
		req = nil
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return
		}
		err = errors.New("rpc: server cannot decode the requestheader: " + err.Error())
		return
	}

	keepReading = true
	serviceMethod := strings.Split(req.Method, ".")
	// just have the method  
	if len(serviceMethod) == 1 {
		server.mu.Lock()
		mtype = server.allMethod[serviceMethod[0]]
        service = server.methodServiceMap[serviceMethod[0]]
		server.mu.Unlock()
		if mtype == nil {
			err = errors.New("rpc: can not find method " + req.Method)
            return
		}
        if service == nil {
            err = errors.New("rpc: can not find service " + req.Method)
            return 
        }
	}
	// need to check service and method all
	if len(serviceMethod) == 2 {
		server.mu.Lock()
		service = server.serviceMap[serviceMethod[0]]
		server.mu.Unlock()
		if service == nil {
			err = errors.New("rpc: can not find service " + req.Method)
			return
		}
		mtype = service.method[serviceMethod[1]]
		if mtype == nil {
			err = errors.New("rpc: can not find method " + req.Method)
		}
	}
	return
}

func (server *Server) sendResponse(sending *sync.Mutex, req *serverRequest, reply interface{}, codec *ServerCodec, errmsg string) {
	resp := server.getResponse()
	if errmsg != "" {
		resp.Error = errmsg
		reply = invalidRequest
		resp.Operation = uint8(3)
	} else {
		resp.Operation = uint8(2)
	}

	sending.Lock()
    err := codec.WriteResponse(resp, reply)
	if err != nil {
		log.Println("rpc: writing response:", err)
	}
	sending.Unlock()
	server.freeResponse(resp)
}

// run the service.method
func (s *service) call(server *Server, sending *sync.Mutex, mtype *methodType, req *serverRequest, argv, replyv reflect.Value, codec *ServerCodec) {
	function := mtype.method.Func
	returnValues := function.Call([]reflect.Value{s.rcvr, argv, replyv})
	errInter := returnValues[0].Interface()
	errmsg := ""
	if errInter != nil {
		errmsg = errInter.(error).Error()
	}
	server.sendResponse(sending, req, replyv.Interface(), codec, errmsg)
	server.freeRequest(req)
}

//////////////////////////////////////////////////////////////////////
// some test
