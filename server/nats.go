package server

import (
    "context"
    "errors"
    "fmt"
    "github.com/mark3labs/mcp-go/mcp"
    "github.com/nats-io/nats.go"
    "github.com/nats-io/nats.go/micro"
    "github.com/nats-io/nkeys"
    "log/slog"
    "strings"
    "time"
)

const (
    ClientHeader    = "X-MCP-Client"
    ServerPublicKey = "X-MCP-Public-Key"
    MsgId           = "X-MCP-Message-Id"
)

type NatsServerOpt func(*NatsServer) error

func WithXKey(xkey nkeys.KeyPair) NatsServerOpt {
    return func(s *NatsServer) error {
        s.xkey = xkey
        pk, err := xkey.PublicKey()
        if err != nil {
            return err
        }
        s.pk = pk
        return nil
    }
}

func WithDescription(description string) NatsServerOpt {
    return func(s *NatsServer) error {
        s.description = description
        return nil
    }
}

func WithNatsClient(nc *nats.Conn) NatsServerOpt {
    return func(s *NatsServer) error {
        s.nc = nc
        return nil
    }
}

type NatsServer struct {
    description string
    xkey        nkeys.KeyPair
    pk          string

    handlerTimeout time.Duration

    nc  *nats.Conn
    mcp *MCPServer
}

func NewNatsServer(mcp *MCPServer, opts ...NatsServerOpt) (*NatsServer, error) {
    xkey, _ := nkeys.CreateCurveKeys()
    pk, _ := xkey.PublicKey()

    result := &NatsServer{
        xkey:           xkey,
        pk:             pk,
        mcp:            mcp,
        nc:             nil,
        handlerTimeout: 10 * time.Second,
    }

    for _, opt := range opts {
        if err := opt(result); err != nil {
            return nil, err
        }
    }

    if result.nc == nil {
        return nil, errors.New("nats client is not set")
    }

    if result.xkey == nil {
        return nil, errors.New("xkey is not set")
    }

    return result, nil
}

func (s *NatsServer) Listen() error {

    srvCfg := micro.Config{
        Name:        s.mcp.name,
        Description: s.description,
        Version:     s.mcp.version,
    }

    svc, err := micro.AddService(s.nc, srvCfg)
    if err != nil {
        return err
    }

    var errs error
    errs = errors.Join(errs, s.addMCPEndpoint(svc, "ping", mcp.MethodPing))
    errs = errors.Join(errs, s.addMCPEndpoint(svc, "initialize", mcp.MethodInitialize))

    grpPrompts := svc.AddGroup("prompts")
    errs = errors.Join(errs, s.addMCPEndpoint(grpPrompts, "list-prompts", mcp.MethodPromptsList))
    errs = errors.Join(errs, s.addMCPEndpoint(grpPrompts, "get-prompt", mcp.MethodPromptsGet))

    grpResources := svc.AddGroup("resources")
    errs = errors.Join(errs, s.addMCPEndpoint(grpResources, "list-resources", mcp.MethodResourcesList))
    errs = errors.Join(errs, s.addMCPEndpoint(grpResources, "read-resource", mcp.MethodResourcesRead))
    errs = errors.Join(errs, s.addMCPEndpoint(grpResources, "subscribe-resource", mcp.MethodResourcesSubscribe))
    errs = errors.Join(errs, s.addMCPEndpoint(grpResources, "unsubscribe-resource", mcp.MethodResourcesUnsubscribe))

    grpResourceTemplates := grpResources.AddGroup("resource-templates")
    errs = errors.Join(errs, s.addMCPEndpoint(grpResourceTemplates, "list-resource-templates", mcp.MethodResourcesTemplatesList))

    grpTools := svc.AddGroup("tools")
    errs = errors.Join(errs, s.addMCPEndpoint(grpTools, "list-tools", mcp.MethodToolsList))
    errs = errors.Join(errs, s.addMCPEndpoint(grpTools, "call-tool", mcp.MethodToolsCall))

    grpLogger := svc.AddGroup("logger")
    errs = errors.Join(errs, s.addEndpoint(grpLogger, "set-log-level", "LOGGER.SET-LOG-LEVEL"))

    return errs
}

func (s *NatsServer) addMCPEndpoint(grp micro.Group, name string, method mcp.MCPMethod) error {
    subject := strings.ReplaceAll(strings.ToUpper(string(method)), "/", ".")
    subject = fmt.Sprintf("%s.*.%s.%s", "$MCP", s.mcp.name, subject)

    slog.Info(fmt.Sprintf("Adding endpoint %s", subject))

    return grp.AddEndpoint(name, s.handle(), micro.WithEndpointSubject(subject))
}

func (s *NatsServer) addEndpoint(grp micro.Group, name string, subject string) error {
    subject = fmt.Sprintf("%s.*.%s.%s", "$MCP", s.mcp.name, subject)

    slog.Info(fmt.Sprintf("Adding endpoint %s", subject))

    return grp.AddEndpoint(name, s.handle(), micro.WithEndpointSubject(subject))
}

func (s *NatsServer) handle() micro.HandlerFunc {
    return func(request micro.Request) {
        ctx, cancel := context.WithTimeout(context.Background(), s.handlerTimeout)
        defer cancel()

        resp := s.mcp.HandleMessage(ctx, request.Data())

        request.RespondJSON(resp)
    }
}
