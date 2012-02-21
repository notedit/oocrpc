// Date: 2012-02-16
// Author: notedit<notedit@gmail.com>

package rpc

import (
    "fmt"
    "errors"
    "testing"
)


type Args struct {
    A,B int
}

type Reply struct {
    C int
}

type Arith int

func (t *Arith) Add(args *Args,reply *Reply) error {
    reply.C = args.A + args.B
    return nil
}

func (t *Arith) Mul(args *Args,reply *Reply) error {
    reply.C = args.A * args.B
    return nil
}

func (t *Arith) Div(args *Args,reply *Reply) error {
    if args.B == 0 {
        return BackendError{"InternalError","divide by zero"}
    }
    reply.C = args.A / args.B
    return nil
}

func (t *Arith) NError(args *Args,reply *Reply) error {
    return errors.New("normalerror")
}

func (t *Arith) Error(args *Args,reply *Reply) error {
    panic("ERROR")
}

func startServer() {
    newServer := NewServer("localhost",9091)
    newServer.Register(new(Arith))
    newServer.Serv()
}

func TestServer(t *testing.T) {
    go startServer()
    client := New("localhost:9091")

    // normal calls
    args := &Args{7,8}
    reply := new(Reply)
    err := client.Call("Arith.Add",args,reply)
    if err != nil {
        t.Errorf("Add: expected no error but got string %q",err.Error())
    }
    if reply.C != args.A + args.B {
        t.Errorf("Add: expected %d got %d",reply.C,args.A + args.B)
    }

    // Nonexistent method
    args = &Args{7,0}
    reply = new(Reply)
    err = client.Call("Arith.BadOperation",args,reply)
    if err == nil {
        t.Error("BadOperation: expected errpor")
    } else if  err.(BackendError).Message != "InternalError" {
        fmt.Printf("%#v\n",err)
        t.Errorf("BadOperation: expected can't find method error")
    }

    // normal error

    err = client.Call("Arith.NError",args,reply)
    if err == nil {
        t.Error("expected normal error")
    } else if err.(BackendError).Detail !=  "normalerror" {
        fmt.Println(err)
        t.Errorf("error detail will be normalerror, %v\n",err)
    }

    // Unknown service
    args = &Args{7,8}
    reply = new(Reply)
    err = client.Call("Unknow.Arith",args,reply)
    if err == nil {
        t.Error("expected Unknow service error")
    }  else if err.(BackendError).Message != "InternalError" {
        t.Error("error message will be InternalError  ")
    }


    // Error test
    args = &Args{7,0}
    reply = new(Reply)
    err = client.Call("Arith.Div",args,reply)

    fmt.Printf("%#v\n",err)
    if err == nil {
        t.Error("Div: expected error")
    } else if err.(BackendError).Detail != "divide by zero" {
        t.Error("expected divide by zero error detail")
    }

    // Panic test
    err = client.Call("Arith.Error",args,reply)
    if err == nil {
        t.Error("expect panic error")
    } else if err.(BackendError).Detail != "ERROR" {
        t.Error("Panic test expect ERROR detail")
    }
}
