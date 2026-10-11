package executor

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
	pluginconfig "github.com/router-for-me/CLIProxyAPIPlugins/mirasim/internal/config"
	"github.com/router-for-me/CLIProxyAPIPlugins/mirasim/internal/mirasim"
	"github.com/tidwall/gjson"
)

func messagesNonStreamFixture(model string) []byte {
	return []byte(fmt.Sprintf(`{"id":"msg_test","type":"message","role":"assistant","model":%q,"content":[{"type":"text","text":"response text"},{"type":"tool_use","id":"toolu_test","name":"lookup","input":{"query":"test"}}],"stop_reason":"tool_use","usage":{"input_tokens":11,"output_tokens":17}}`, model))
}

func messagesSSEFixture(model string) []byte {
	events := []string{
		fmt.Sprintf(`{"type":"message_start","message":{"id":"msg_test","type":"message","role":"assistant","model":%q,"content":[],"usage":{"input_tokens":11,"output_tokens":0}}}`, model),
		`{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"reasoning text"}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"test-signature"}}`,
		`{"type":"content_block_stop","index":0}`,
		`{"type":"content_block_start","index":1,"content_block":{"type":"text","text":""}}`,
		`{"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"response "}}`,
		`{"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"text"}}`,
		`{"type":"content_block_stop","index":1}`,
		`{"type":"content_block_start","index":2,"content_block":{"type":"tool_use","id":"toolu_test","name":"lookup","input":{}}}`,
		`{"type":"content_block_delta","index":2,"delta":{"type":"input_json_delta","partial_json":"{\"query\":"}}`,
		`{"type":"content_block_delta","index":2,"delta":{"type":"input_json_delta","partial_json":"\"test\"}"}}`,
		`{"type":"content_block_stop","index":2}`,
		`{"type":"message_delta","delta":{"stop_reason":"tool_use","stop_sequence":null},"usage":{"output_tokens":17}}`,
		`{"type":"message_stop"}`,
	}
	return []byte("data: " + strings.Join(events, "\r\n\r\ndata: "))
}

func TestExecuteNonStreamMessagesPreservesContentAcrossProtocols(t *testing.T) {
	for _, model := range []string{"claude-sonnet-5-5", "deepseek-flash", "glm-5.3-flash", "kimi-code/k3", "gemini-3.1-pro-preview"} {
		for _, format := range []struct {
			name, request, inputUsage, outputUsage string
		}{
			{"claude", `{"messages":[{"role":"user","content":"hello"}],"max_tokens":256}`, "usage.input_tokens", "usage.output_tokens"},
			{"openai", `{"messages":[{"role":"user","content":"hello"}],"stream":false}`, "usage.prompt_tokens", "usage.completion_tokens"},
			{"openai-response", `{"input":"hello","stream":false}`, "usage.input_tokens", "usage.output_tokens"},
			{"gemini", `{"contents":[{"role":"user","parts":[{"text":"hello"}]}]}`, "usageMetadata.promptTokenCount", "usageMetadata.candidatesTokenCount"},
		} {
			t.Run(model+"/"+format.name, func(t *testing.T) {
				storage := executorTestStorage(t)
				host := executorHostClient{do: func(_ context.Context, req pluginapi.HTTPRequest) (pluginapi.HTTPResponse, error) {
					parsed, _ := url.Parse(req.URL)
					switch parsed.Path {
					case "/v1/device/session":
						return pluginapi.HTTPResponse{StatusCode: 200, Body: []byte(`{"ticket":"ticket","expiresIn":900}`)}, nil
					case "/v1/messages":
						if gjson.GetBytes(req.Body, "stream").Bool() {
							if format.name == "claude" || req.Headers.Get("Accept") != "text/event-stream" {
								t.Errorf("unexpected upstream stream: format=%s headers=%v", format.name, req.Headers)
							}
							return pluginapi.HTTPResponse{StatusCode: 200, Body: messagesSSEFixture(model)}, nil
						}
						return pluginapi.HTTPResponse{StatusCode: 200, Body: messagesNonStreamFixture(model)}, nil
					default:
						return pluginapi.HTTPResponse{}, fmt.Errorf("unexpected path %s", parsed.Path)
					}
				}}
				payload := []byte(format.request)
				resp, err := New(pluginconfig.Defaults(), mirasim.NewPool()).Execute(context.Background(), pluginapi.ExecutorRequest{
					Model: model, Format: format.name, SourceFormat: format.name, StorageJSON: storage.JSON(),
					Payload: payload, OriginalRequest: payload, HTTPClient: host,
				})
				if err != nil {
					t.Fatal(err)
				}
				if !gjson.ValidBytes(resp.Payload) || resp.Headers.Get("Content-Type") != "application/json" {
					t.Fatalf("expected downstream JSON, got %s (%v)", resp.Payload, resp.Headers)
				}
				for _, text := range []string{"response text", "lookup", "query", "test"} {
					if !strings.Contains(string(resp.Payload), text) {
						t.Errorf("response lost %q: %s", text, resp.Payload)
					}
				}
				if got := gjson.GetBytes(resp.Payload, format.inputUsage).Int(); got != 11 {
					t.Errorf("input usage = %d, want 11: %s", got, resp.Payload)
				}
				if got := gjson.GetBytes(resp.Payload, format.outputUsage).Int(); got != 17 {
					t.Errorf("output usage = %d, want 17: %s", got, resp.Payload)
				}
				if format.name == "claude" && string(resp.Payload) != string(messagesNonStreamFixture(model)) {
					t.Fatalf("native Messages JSON changed: %s", resp.Payload)
				}
				if format.name != "claude" && !strings.Contains(string(resp.Payload), "reasoning text") {
					t.Fatalf("reasoning lost: %s", resp.Payload)
				}
			})
		}
	}
}

func TestExecuteNonStreamMessagesRejectsIncompleteOrFailedStream(t *testing.T) {
	const failed = "data: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"upstream overloaded\"}}\n\n"
	const stop = "data: {\"type\":\"message_stop\"}\n\n"
	for name, wire := range map[string]string{
		"empty": "", "truncated": "data: {\"type\":\"message_start\",\"message\":{}}\n\n", "done only": "data: [DONE]\n\n",
		"failed": failed, "failed then stop": failed + stop, "stop then failed": stop + failed,
	} {
		for _, format := range []string{"openai", "openai-response", "gemini"} {
			t.Run(name+"/"+format, func(t *testing.T) {
				storage := executorTestStorage(t)
				host := executorHostClient{do: func(_ context.Context, req pluginapi.HTTPRequest) (pluginapi.HTTPResponse, error) {
					parsed, _ := url.Parse(req.URL)
					if parsed.Path == "/v1/device/session" {
						return pluginapi.HTTPResponse{StatusCode: 200, Body: []byte(`{"ticket":"ticket","expiresIn":900}`)}, nil
					}
					return pluginapi.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(wire)}, nil
				}}
				_, err := New(pluginconfig.Defaults(), mirasim.NewPool()).Execute(context.Background(), pluginapi.ExecutorRequest{
					Model: "claude-sonnet-5-5", SourceFormat: "claude", Format: format, StorageJSON: storage.JSON(),
					Payload: []byte(`{"messages":[{"role":"user","content":"hello"}],"max_tokens":256}`), HTTPClient: host,
				})
				want := "before message_stop"
				if strings.Contains(name, "failed") {
					want = "upstream overloaded"
				}
				if err == nil || !strings.Contains(err.Error(), want) {
					t.Fatalf("error = %v, want %q", err, want)
				}
			})
		}
	}
}

func TestNonStreamMessagesRetainsSplitCacheUsage(t *testing.T) {
	wire := strings.Replace(string(messagesSSEFixture("claude-sonnet-5-5")), `"input_tokens":11,"output_tokens":0`, `"input_tokens":11,"output_tokens":0,"cache_creation_input_tokens":3,"cache_read_input_tokens":7`, 1)
	normalized, err := messagesNonStreamPayload([]byte(wire))
	if err != nil {
		t.Fatal(err)
	}
	out, err := translateNonStream(context.Background(), "claude", "openai", "claude-sonnet-5-5", nil, []byte(`{}`), normalized)
	if err != nil {
		t.Fatal(err)
	}
	if gjson.GetBytes(out, "usage.prompt_tokens").Int() != 21 || gjson.GetBytes(out, "usage.completion_tokens").Int() != 17 || gjson.GetBytes(out, "usage.total_tokens").Int() != 38 || gjson.GetBytes(out, "usage.prompt_tokens_details.cached_tokens").Int() != 7 {
		t.Fatalf("split cache usage lost or counted twice: %s", out)
	}
	out, err = translateNonStream(context.Background(), "claude", "gemini", "claude-sonnet-5-5", nil, []byte(`{}`), normalized)
	if err != nil || gjson.GetBytes(out, "usageMetadata.promptTokenCount").Int() != 11 || gjson.GetBytes(out, "usageMetadata.cachedContentTokenCount").Int() != 10 || gjson.GetBytes(out, "usageMetadata.candidatesTokenCount").Int() != 17 {
		t.Fatalf("Gemini split usage lost: %s, error=%v", out, err)
	}
}
