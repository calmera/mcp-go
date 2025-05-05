package client

import (
    "context"
    "encoding/json"
    "errors"
    "fmt"
    "github.com/mark3labs/mcp-go/mcp"
    "github.com/nats-io/nats.go"
    "github.com/nats-io/nkeys"
    "strings"
    "sync"
    "time"
)

const (
    ClientHeader = "X-MCP-Client"
)

func NewNatsMCPClient(nc *nats.Conn, ncManaged bool, serverName string) (*NatsMCPClient, error) {
    xkey, err := nkeys.CreateCurveKeys()
    if err != nil {
        return nil, err
    }

    pk, err := xkey.PublicKey()
    if err != nil {
        return nil, err
    }

    return &NatsMCPClient{
        nc:         nc,
        ncManaged:  ncManaged,
        serverName: serverName,
        timeout:    5 * time.Second,
        xkey:       xkey,
        pk:         pk,
    }, nil
}

type NatsMCPClient struct {
    ncManaged bool
    nc        *nats.Conn

    xkey nkeys.KeyPair
    pk   string

    serverName string

    // serverPk is only being filled once an initialization is done
    serverPk string

    sub      *nats.Subscription
    handlers []func(notification mcp.JSONRPCNotification)
    lock     sync.Mutex

    timeout time.Duration
}

func (n *NatsMCPClient) request(subject string, data any, target any) error {
    var err error
    var b []byte

    if data != nil {
        b, err = json.Marshal(data)
        if err != nil {
            return fmt.Errorf("failed to marshal request: %w", err)
        }
    }

    resp, err := n.nc.Request(subject, b, n.timeout)
    if err != nil {
        return err
    }

    if target != nil {
        if err := json.Unmarshal(resp.Data, target); err != nil {
            return fmt.Errorf("failed to unmarshal response: %w", err)
        }
    }

    return nil
}

func (n *NatsMCPClient) mcpMethodSubject(method mcp.MCPMethod) string {
    subject := strings.ReplaceAll(strings.ToUpper(string(method)), "/", ".")
    return fmt.Sprintf("$MCP.%s.%s.%s", n.pk, n.serverName, subject)
}

func (n *NatsMCPClient) Ready() bool {
    return n.serverPk != ""
}

func (n *NatsMCPClient) Initialize(ctx context.Context, request mcp.InitializeRequest) (*mcp.InitializeResult, error) {
    var result mcp.InitializeResult
    if err := n.request(n.mcpMethodSubject(mcp.MethodInitialize), request, &result); err != nil {
        return nil, err
    }

    return &result, nil
}

func (n *NatsMCPClient) Ping(ctx context.Context) error {
    return n.request(n.mcpMethodSubject(mcp.MethodPing), nil, nil)
}

func (n *NatsMCPClient) ListResources(ctx context.Context, request mcp.ListResourcesRequest) (*mcp.ListResourcesResult, error) {
    if !n.Ready() {
        return nil, fmt.Errorf("client has not been initialized yet")
    }

    var result mcp.ListResourcesResult
    if err := n.request(n.mcpMethodSubject(mcp.MethodResourcesList), request, &result); err != nil {
        return nil, err
    }

    return &result, nil
}

func (n *NatsMCPClient) ListResourceTemplates(ctx context.Context, request mcp.ListResourceTemplatesRequest) (*mcp.ListResourceTemplatesResult, error) {
    if !n.Ready() {
        return nil, fmt.Errorf("client has not been initialized yet")
    }

    var result mcp.ListResourceTemplatesResult
    if err := n.request(n.mcpMethodSubject(mcp.MethodResourcesTemplatesList), request, &result); err != nil {
        return nil, err
    }

    return &result, nil
}

func (n *NatsMCPClient) ReadResource(ctx context.Context, request mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
    if !n.Ready() {
        return nil, fmt.Errorf("client has not been initialized yet")
    }

    var result mcp.ReadResourceResult
    if err := n.request(n.mcpMethodSubject(mcp.MethodResourcesRead), request, &result); err != nil {
        return nil, err
    }

    return &result, nil
}

func (n *NatsMCPClient) Subscribe(ctx context.Context, request mcp.SubscribeRequest) error {
    if !n.Ready() {
        return fmt.Errorf("client has not been initialized yet")
    }

    n.lock.Lock()
    defer n.lock.Unlock()
    if n.sub == nil {
        var err error

        fullSubject := fmt.Sprintf("%s.%s.%s", BaseSubject, n.pk, NotifySubject)
        n.sub, err = n.nc.Subscribe(fullSubject, func(msg *nats.Msg) {
            if msg.Data == nil {
                return
            }

            b, err := n.xkey.Open(msg.Data, n.serverPk)
            if err != nil {
                fmt.Printf("failed to decrypt notification: %s\n", err)
                return
            }

            var notification mcp.JSONRPCNotification
            if err := json.Unmarshal(b, &notification); err != nil {
                fmt.Printf("failed to unmarshal notification: %s\n", err)
                return
            }

            for _, handler := range n.handlers {
                handler(notification)
            }
        })

        if err != nil {
            return fmt.Errorf("failed to subscribe to %s: %w", fullSubject, err)
        }
    }

    _, err := n.requestSession(request)
    return err
}

func (n *NatsMCPClient) Unsubscribe(ctx context.Context, request mcp.UnsubscribeRequest) error {
    if !n.Ready() {
        return fmt.Errorf("client has not been initialized yet")
    }

    _, err := n.requestSession(request)
    return err
}

func (n *NatsMCPClient) ListPrompts(ctx context.Context, request mcp.ListPromptsRequest) (*mcp.ListPromptsResult, error) {
    if !n.Ready() {
        return nil, fmt.Errorf("client has not been initialized yet")
    }

    var result mcp.ListPromptsResult
    if err := n.request(n.mcpMethodSubject(mcp.MethodPromptsList), request, &result); err != nil {
        return nil, err
    }

    return &result, nil
}

func (n *NatsMCPClient) GetPrompt(ctx context.Context, request mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
    if !n.Ready() {
        return nil, fmt.Errorf("client has not been initialized yet")
    }

    var result mcp.GetPromptResult
    if err := n.request(n.mcpMethodSubject(mcp.MethodPromptsGet), request, &result); err != nil {
        return nil, err
    }

    return &result, nil
}

func (n *NatsMCPClient) ListTools(ctx context.Context, request mcp.ListToolsRequest) (*mcp.ListToolsResult, error) {
    if !n.Ready() {
        return nil, fmt.Errorf("client has not been initialized yet")
    }

    var result mcp.ListToolsResult
    if err := n.request(n.mcpMethodSubject(mcp.MethodToolsList), request, &result); err != nil {
        return nil, err
    }

    return &result, nil
}

func (n *NatsMCPClient) CallTool(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
    if !n.Ready() {
        return nil, fmt.Errorf("client has not been initialized yet")
    }

    var result json.RawMessage
    if err := n.request(n.mcpMethodSubject(mcp.MethodToolsCall), request, &result); err != nil {
        return nil, err
    }

    return mcp.ParseCallToolResult(&result)
}

func (n *NatsMCPClient) SetLevel(ctx context.Context, request mcp.SetLevelRequest) error {
    if !n.Ready() {
        return fmt.Errorf("client has not been initialized yet")
    }

    _, err := n.requestSession(request)
    return err
}

func (n *NatsMCPClient) Complete(ctx context.Context, request mcp.CompleteRequest) (*mcp.CompleteResult, error) {
    if !n.Ready() {
        return nil, fmt.Errorf("client has not been initialized yet")
    }

    resp, err := n.requestSession(request)
    if err != nil {
        return nil, err
    }

    var result mcp.CompleteResult
    if err := json.Unmarshal(resp, &result); err != nil {
        return nil, fmt.Errorf("failed to unmarshal CompleteResult: %w", err)
    }

    return &result, nil
}

func (n *NatsMCPClient) Close() error {
    var errs error
    if n.sub != nil {
        if err := n.sub.Unsubscribe(); err != nil {
            errs = errors.Join(errs, err)
        }
    }

    if n.ncManaged {
        n.nc.Close()
    }

    return errs
}

func (n *NatsMCPClient) OnNotification(handler func(notification mcp.JSONRPCNotification)) {
    n.lock.Lock()
    defer n.lock.Unlock()
    n.handlers = append(n.handlers, handler)
}
