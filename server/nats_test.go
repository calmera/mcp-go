package server

import (
    "encoding/json"
    "fmt"
    "sync"
    "testing"
    "time"
)

func TestNatsServer(t *testing.T) {
    t.Run("Can instantiate", func(t *testing.T) {
        mcpServer := NewMCPServer("test", "1.0.0")

        tw := NewNatsTestServer(mcpServer)
        defer tw.Close()

        if tw.Server() == nil {
            t.Error("NatsMCPServer should not be nil")
        }
        if tw.Server().mcp == nil {
            t.Error("MCPServer should not be nil")
        }
        if tw.Server().nc == nil {
            t.Error("NATS connection should not be nil")
        }
        if tw.Server().xkey == nil {
            t.Error("NATS MCP server should not be nil")
        }
    })

    t.Run("Can send and receive messages", func(t *testing.T) {
        mcpServer := NewMCPServer("test", "1.0.0",
            WithResourceCapabilities(true, true),
        )
        tw := NewNatsTestServer(mcpServer)
        defer tw.Close()

        // Send initialize request
        initRequest := map[string]interface{}{
            "jsonrpc": "2.0",
            "id":      1,
            "method":  "initialize",
            "params": map[string]interface{}{
                "protocolVersion": "2024-11-05",
                "clientInfo": map[string]interface{}{
                    "name":    "test-client",
                    "version": "1.0.0",
                },
            },
        }

        requestBody, err := json.Marshal(initRequest)
        if err != nil {
            t.Fatalf("Failed to marshal request: %v", err)
        }

        resp, err := tw.nc.Request("$MCP.test.INITIALIZE", requestBody, time.Second)
        if err != nil {
            t.Fatalf("Failed to send message: %v", err)
        }

        // Verify response
        var response map[string]interface{}
        if err := json.Unmarshal(resp.Data, &response); err != nil {
            t.Fatalf("Failed to decode response: %v", err)
        }

        if response["jsonrpc"] != "2.0" {
            t.Errorf("Expected jsonrpc 2.0, got %v", response["jsonrpc"])
        }
        if response["id"].(float64) != 1 {
            t.Errorf("Expected id 1, got %v", response["id"])
        }
    })

    t.Run("Can handle multiple sessions", func(t *testing.T) {
        mcpServer := NewMCPServer("test", "1.0.0",
            WithResourceCapabilities(true, true),
        )
        tw := NewNatsTestServer(mcpServer)
        defer tw.Close()

        numSessions := 3
        var wg sync.WaitGroup
        wg.Add(numSessions)

        for i := 0; i < numSessions; i++ {
            go func(sessionNum int) {
                defer wg.Done()

                nc, err := tw.acc.Connect(tw.srv)
                if err != nil {
                    t.Errorf(
                        "Session %d: Failed to connect to NATS: %v",
                        sessionNum,
                        err,
                    )
                    return
                }
                defer nc.Close()

                // Send initialize request
                initRequest := map[string]interface{}{
                    "jsonrpc": "2.0",
                    "id":      sessionNum,
                    "method":  "initialize",
                    "params": map[string]interface{}{
                        "protocolVersion": "2024-11-05",
                        "clientInfo": map[string]interface{}{
                            "name": fmt.Sprintf(
                                "test-client-%d",
                                sessionNum,
                            ),
                            "version": "1.0.0",
                        },
                    },
                }

                requestBody, err := json.Marshal(initRequest)
                if err != nil {
                    t.Errorf(
                        "Session %d: Failed to marshal request: %v",
                        sessionNum,
                        err,
                    )
                    return
                }

                resp, err := nc.Request("$MCP.test.INITIALIZE", requestBody, time.Second)
                if err != nil {
                    t.Errorf(
                        "Session %d: Failed to send message: %v",
                        sessionNum,
                        err,
                    )
                    return
                }

                var response map[string]interface{}
                if err := json.Unmarshal(resp.Data, &response); err != nil {
                    t.Errorf(
                        "Session %d: Failed to decode response: %v",
                        sessionNum,
                        err,
                    )
                    return
                }

                if response["id"].(float64) != float64(sessionNum) {
                    t.Errorf(
                        "Session %d: Expected id %d, got %v",
                        sessionNum,
                        sessionNum,
                        response["id"],
                    )
                }
            }(i)
        }

        // Wait with timeout
        done := make(chan struct{})
        go func() {
            wg.Wait()
            close(done)
        }()

        select {
        case <-done:
            // All sessions completed successfully
        case <-time.After(5 * time.Second):
            t.Fatal("Timeout waiting for sessions to complete")
        }
    })
}
