package server

import (
    "github.com/mark3labs/mcp-go/server/test"
    "github.com/nats-io/nats-server/v2/server"
    "github.com/nats-io/nats.go"
)

type NatsTestServer struct {
    srv *server.Server
    acc test.Acc

    nc *nats.Conn
    ns *NatsServer
}

func (n *NatsTestServer) Close() {
    n.srv.Shutdown()
}

func (n *NatsTestServer) Server() *NatsServer {
    return n.ns
}

func NewNatsTestServer(mcp *MCPServer, ) *NatsTestServer {
    acc := test.Account("TEST_ACCOUNT")
    srv := test.NewServerBuilder().WithAccount(acc).Run()

    nc, err := acc.Connect(srv)
    if err != nil {
        panic(err)
    }

    ns, err := NewNatsServer(mcp, WithNatsClient(nc))
    if err != nil {
        panic(err)
    }

    result := &NatsTestServer{
        srv: srv,
        acc: acc,
        ns:  ns,
        nc:  nc,
    }

    if err := result.ns.Listen(); err != nil {
        panic(err)
    }

    return result
}
