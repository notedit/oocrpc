// Date: 2012-02-08
// Author: notedit <notedit@gmail.com>
// make a go rpc service

package rpc

import (
    "fmt"
    "log"
    "net"
    "io"
    "io/ioutil"
    "encoding/binary"
    "bufio"
    "sync"
    "reflect"
    "errors"
    "strings"
    "unicode"
   // "runtime"
    "unicode/utf8"
    "launchpad.net/mgo/bson"
)

var typeOfError = reflect.TypeOf((*error)(nil)).Elem()

type methodType struct {
    method reflect.Method
    ArgType     reflect.Type
    ReplyType   reflect.Type
}

type service struct {
    name    string
    rcvr    reflect.Value
    typ     reflect.Type
    method  map[string]*methodType
}

// rpc server
type Server struct {
    mu              sync.Mutex
    serviceMap      map[string]*service
    listener        *net.TCPListener
    reqLock         sync.Mutex
    freeReq         *serverRequest
    respLock        sync.Mutex
    freeResp        *serverResponse
}


//BackendError
type BackendError struct {
    Message string
    Detail  string
}

func (e BackendError) Error() string {
    return fmt.Sprintf("%s:%s",e.Message,e.Detail)
}

// operation has three values -- call:1  reply:2  error:3

// request
type serverRequest struct {
    messageLength   uint32          // unexported
    next            *serverRequest  // unexported
    Operation       uint8
    Method          string
    Argument        bson.Raw
}

// response
type serverResponse struct {
    messageLength   uint32          // unexported
    next            *serverResponse  // unexported
    Operation       uint8
    Reply           interface{}
}

// decode request and encode response
type ServerCodec struct {
    cn  net.Conn
    rw  *bufio.ReadWriter
}

// read the message header
func (c *ServerCodec)ReadRequestHeader(req *serverRequest) (err error) {
    msgheader := make([]byte,4)
    _,err = c.rw.Read(msgheader)
    if err != nil {
        req = nil
        if err == io.EOF {
            return
        }
        err = errors.New("rpc: server cannot decode requestheader: " + err.Error())
        return
    }
    req.messageLength = binary.BigEndian.Uint32(msgheader)
    return nil
}

func (c *ServerCodec)ReadRequestBody(req *serverRequest) (err error) {
    msgbytes,err := ioutil.ReadAll(io.LimitReader(c.rw.Reader,int64(req.messageLength)))
    if err != nil {
        if err == io.EOF {
            return
        }
        err = errors.New("rpc: server cannot read full requestBody: " + err.Error())
        return
    }
    if err = bson.Unmarshal(msgbytes,req); err != nil {
        return
    }
    return
}

func (c *ServerCodec)WriteResponse(res *serverResponse) (err error) {
    bys, err := bson.Marshal(res)
    if err != nil {
        log.Println("writeresponse error",err)
        return
    }
    res.messageLength = uint32(len(bys))
    // write message header
    rw := c.rw.Writer
    _,err = rw.Write([]byte{byte(res.messageLength>>24),byte(res.messageLength>>16),byte(res.messageLength>>8),byte(res.messageLength)})
    if err != nil {
        log.Println("write responseHeader error",err)
        return
    }
    // write message body
    _,err = rw.Write(bys)
    if err != nil {
        log.Println("write responseBody error",err)
        return
    }
    if err = rw.Flush(); err != nil {
        log.Println("flush responseBody error",err)
    }
    return
}

// todo
func (c *ServerCodec)Close() error {
    return c.cn.Close()
}

// Is this an exported - upper case 
func isExported(name string) bool {
    rune,_ := utf8.DecodeRuneInString(name)
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
func (server *Server)Register(rcvr interface{}) error {
    return server.register(rcvr,"",false)
}

// Register a sevice with a name
func (server *Server)RegisterName(name string,rcvr interface{}) error {
    return server.register(rcvr,name,true)
}

// the real register
func (server *Server)register(rcvr interface{}, name string,useName bool) error {
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
        log.Fatal("rpc: no service name for type",s.typ.String())
    }
    if !isExported(sname) && !useName {
        s := "rpc Register: type " + sname + " is not exported"
        log.Print(s)
        return errors.New(s)
    }
    if _,present := server.serviceMap[sname]; present {
        return errors.New("rpc: service already defined: " + sname)
    }
    s.name = sname
    s.method = make(map[string]*methodType)

    // Install the methods
    for m:=0; m < s.typ.NumMethod(); m++ {
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
            log.Println("method",mname,"has wrong number of outs:",mtype.NumOut())
            continue
        }

        // first arg need not be a pointer
        argType := mtype.In(1)
        if !isExportedOrBuiltinType(argType) {
            log.Println(mname,"argument type not exported or local",argType)
            continue
        }

        replyType := mtype.In(2)
        if replyType.Kind() != reflect.Ptr {
            log.Println("method",mname," reply type not a pointer:",replyType)
            continue
        }
        
        if !isExportedOrBuiltinType(replyType) {
            log.Println("method ",mname,"reply type not exported or local",replyType)
            continue
        }

        // error type
        if returnType := mtype.Out(0); returnType != typeOfError {
            log.Println("method",mname," returns",returnType.String(),"not error")
            continue
        }

        s.method[mname] = &methodType{method:method,ArgType:argType,ReplyType:replyType}
    }

    if len(s.method) == 0 {
        s := "rpc Register: type " + sname + " has no exported methods of suitable type"
        log.Println(s)
        return errors.New(s)
    }
    server.serviceMap[s.name] = s
    return nil
}



func NewServer(host string,port uint) *Server {
    addr,err := net.ResolveTCPAddr("tcp",fmt.Sprintf("%s:%d",host,port))
    if err != nil {
        log.Fatal("rpc error:",err.Error());
    }
    listener,err := net.ListenTCP("tcp",addr)
    if err != nil {
        log.Fatal("rpc error:",err.Error())
    }
    return &Server{
        serviceMap:make(map[string]*service),
        listener:listener,
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

    for{
        c,err := server.listener.Accept()
        if err != nil {
            log.Print("rpc:",err.Error())
            continue
        }
        go server.ServeConn(c)
    }

}

func (server *Server) ServeConn(conn net.Conn){
    src := &ServerCodec{
        cn:conn,
        rw:bufio.NewReadWriter(bufio.NewReader(conn),bufio.NewWriter(conn)),
    }
    server.ServeCodec(src)
}

func (server *Server)ServeCodec(codec *ServerCodec) {
    sending := new(sync.Mutex)
    for { 
        service,mtype,req,argv,replyv,keepReading,err := server.readRequest(codec)
        if err != nil {
            if err != io.EOF {
                log.Println(err)
            }
            if !keepReading {
                break
            }
            // we just got the req
            if req != nil {
                server.sendResponse(nil,req,codec,err,sending)
                server.freeRequest(req)
            }
            continue
        }
        go service.call(server,mtype,req,argv,replyv,codec,sending)
    }
    // to do some recover
    codec.Close()
}

func (server *Server)readRequest(codec *ServerCodec) (service *service,mtype *methodType,req *serverRequest,argv reflect.Value,replyv reflect.Value,keepReading bool,err error){
    req,keepReading,err = server.readRequestHeader(codec)
    if err != nil {
        return
    }
    service,mtype,argv,replyv,keepReading,err = server.readRequestBody(codec,req)
    return
}

func (server *Server)readRequestBody(codec *ServerCodec,req *serverRequest) (service *service,mtype *methodType, argv reflect.Value,replyv reflect.Value,keepReading bool,err error){
    err = codec.ReadRequestBody(req)
    if err != nil {
        return
    }
    // funcname'format  -- service.method

    keepReading = true
    serviceMethod := strings.Split(req.Method,".")
    if len(serviceMethod) != 2 {
        err = errors.New("rpc: service/method request ill-formed: " + req.Method)
        return
    }
    // look up the service
    server.mu.Lock()
    service = server.serviceMap[serviceMethod[0]]
    server.mu.Unlock()
    if service == nil {
        err = errors.New("rpc: can't find service " + serviceMethod[0])
        return
    }
    // look up the method
    mtype = service.method[serviceMethod[1]]
    if mtype == nil {
        err = errors.New("rpc: can't find method " + serviceMethod[1])
        return
    }

    argIsValue := false
    if mtype.ArgType.Kind() == reflect.Ptr {
        argv = reflect.New(mtype.ArgType.Elem())
    } else {
        argv = reflect.New(mtype.ArgType)
        argIsValue = true
    }

    //argv now is a pointer now
    if err = req.Argument.Unmarshal(argv.Interface()); err != nil {
        return
    }

    if argIsValue {
        argv = argv.Elem()
    }

    replyv = reflect.New(mtype.ReplyType.Elem())
    return
}

func (server *Server)readRequestHeader(codec *ServerCodec) (req *serverRequest,keepReading bool,err error){
    req = server.getRequest()
    err = codec.ReadRequestHeader(req)
    if err != nil {
        req = nil
        if err == io.EOF || err == io.ErrUnexpectedEOF {
            return 
        }
        err = errors.New("rpc: server cannot decode the requestheader: " + err.Error())
    }
    return
}

func (server *Server)sendResponse(reply interface{},req *serverRequest,codec *ServerCodec,err interface{},sending *sync.Mutex) {
    var rerr error
    res := server.getResponse()
    switch err.(type) {
        case nil:
            res.Operation = uint8(2)
            res.Reply = reply
        case BackendError:
            res.Operation = uint8(3)
            res.Reply = err
        case error:
            res.Operation = uint8(3)
            res.Reply = BackendError{Message:"InternalError",Detail:err.(error).Error()}
        default:
            res.Operation = uint8(3)
            res.Reply = BackendError{Message:"InternalError",Detail:"error is unvalid"}
    }
    sending.Lock()
    rerr = codec.WriteResponse(res)
    if rerr != nil {
        log.Println("rpc error:",rerr)
    }
    sending.Unlock()
    server.freeResponse(res)
}


// run the service.method
func (s *service) call(server *Server,mtype *methodType,req *serverRequest,argv,replyv reflect.Value, codec *ServerCodec, sending *sync.Mutex) {
    defer func(){
        // it may be panic in the method
        if r := recover(); r != nil {
            err := errors.New(fmt.Sprint(r))
            server.sendResponse(nil,req,codec,err,sending)
            server.freeRequest(req)
        }
    }()
    function := mtype.method.Func
    returnValues := function.Call([]reflect.Value{s.rcvr,argv,replyv})
    err := returnValues[0].Interface()
    server.sendResponse(replyv.Interface(),req,codec,err,sending)
    server.freeRequest(req)
}




//////////////////////////////////////////////////////////////////////
// some test

