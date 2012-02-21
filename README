# lisrpc

This is lisrpc, a rpc based on bson, you can call the remote golang service from the python client.

so you can write the webapp's frontend with python(django tornado flask), write the webapp's backend with go.

## QUICK START

    $ go get  github.com/notedit/lisrpc/rpc
    $ go test github.com/notedit/lisrpc/rpc


# go rpc server:

```go
package main
                                                                                                                            
import (
    "errors"
    "github.com/notedit/lisrpc/rpc"
)

type Args struct {
    A, B int
}

type Reply struct {
    C int
}

type Arith int

func (t *Arith) Add(args *Args, reply *Reply) error {
    reply.C = args.A + args.B
    return nil
}

func (t *Arith) Mul(args *Args, reply *Reply) error {
    reply.C = args.A * args.B
    return nil
}

func (t *Arith) Div(args *Args, reply *Reply) error {
    if args.B == 0 {
        return rpc.BackendError{"InternalError", "divide by zero"}
    }
    reply.C = args.A / args.B
    return nil
}


func (t *Arith) Error(args *Args, reply *Reply) error {
    panic("ERROR")
}

func (t *Arith) NError(args *Args, reply *Reply) error {
    return errors.New("normalerror")
}

func main() {
    newServer := rpc.NewServer("localhost", 9091)
    newServer.Register(new(Arith))
    newServer.Serv()
}    
```

# go rpc client:

```go
package main

import (
    "fmt"
    "github.com/notedit/lisrpc/rpc"
)

type Args struct {
    A, B int
}

type Reply struct {
    C int
}

func main() {
    client := rpc.New("localhost:9090")
    // normal test
    args := &Args{7, 8}
    reply := &Reply{}

    err := client.Call("Arith.Mul", args, reply)
    if err != nil {
        fmt.Println(err.Error())
    }

    err = client.Call("Arith.Add", args, reply)
    if err != nil {
        fmt.Println(err.Error())
    }

    // un exist method
    err = client.Call("Arith.Notfound", args, reply)
    if err != nil {
        fmt.Println(err.Error())
    }
    // un exist service
    err = client.Call("Notfound.arith", args, reply)
    if err != nil {
        fmt.Println(err.Error())
    }
    // test error 
    args = &Args{7, 0}
    reply = &Reply{}

    err = client.Call("Arith.Div", args, reply)
    if err != nil {
        fmt.Println(err.Error())
    }

    // test panic
    args = &Args{7, 8}
    reply = &Reply{}

    err = client.Call("Arith.Error", args, reply)
    if err != nil {
        fmt.Println(err.Error())
    }
}                
```

# python rpc client:

```python
from client import RpcClient

client = RpcClient(host='localhost',port=9090)                                                                          
ret = client.Arith.Add({'a':7,'b':8})
print 'Arith.Add',ret

ret = client.Arith.Mul({'a':7,'b':8})
print 'Arith.Mul',ret
```
