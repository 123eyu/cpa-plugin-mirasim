package quotapage

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
	pluginconfig "github.com/router-for-me/CLIProxyAPIPlugins/mirasim/internal/config"
	"github.com/router-for-me/CLIProxyAPIPlugins/mirasim/internal/credentials"
	"github.com/router-for-me/CLIProxyAPIPlugins/mirasim/internal/executor"
	"github.com/router-for-me/CLIProxyAPIPlugins/mirasim/internal/mirasim"
	"github.com/router-for-me/CLIProxyAPIPlugins/mirasim/internal/models"
)

type probeRelayFixture struct {
	inferences map[string]int
}

func (f *probeRelayFixture) Do(_ context.Context, req pluginapi.HTTPRequest) (pluginapi.HTTPResponse, error) {
	parsed, _ := url.Parse(req.URL)
	body := ""
	switch parsed.Path {
	case "/v1/device/session":
		body = `{"ticket":"ticket","expiresIn":900}`
	case "/v1/models":
		body = `{"data":[{"id":"claude-opus-5-5"},{"id":"gpt-6-astra"},{"id":"kimi-code/k3"},{"id":"gemini-3.1-pro-preview"}]}`
	case "/v1/model-roster":
		body = `{"version":"probe-fixture","models":{"claude-opus-5-5":{"contextWindow":1000000,"adaptive":true}}}`
	case "/v1/messages", "/v1/responses", "/v1/images/generations":
		var payload struct {
			Model string `json:"model"`
		}
		if err := json.Unmarshal(req.Body, &payload); err != nil {
			return pluginapi.HTTPResponse{}, err
		}
		if strings.HasPrefix(payload.Model, "claude-") {
			if err := validateClaudeProbe(req); err != nil {
				return pluginapi.HTTPResponse{StatusCode: http.StatusForbidden, Body: []byte(`{"error":{"message":"this client is not supported; use the mirasim client or Claude Code","type":"permission_error"},"type":"error"}`)}, nil
			}
			if parsed.Query().Get("beta") != "true" {
				return pluginapi.HTTPResponse{}, fmt.Errorf("Claude probe lost its beta route")
			}
		}
		f.inferences[payload.Model]++
		switch parsed.Path {
		case "/v1/messages":
			body = `{"id":"msg_test","type":"message","role":"assistant","model":"` + payload.Model + `","content":[{"type":"text","text":"OK"}],"stop_reason":"end_turn","usage":{"input_tokens":8,"output_tokens":1}}`
		case "/v1/responses":
			body = "data: " + `{"type":"response.completed","response":{"id":"resp_test","object":"response","model":"gpt-6-astra","status":"completed","output":[{"id":"msg_1","type":"message","role":"assistant","content":[{"type":"output_text","text":"OK","annotations":[]}]}],"usage":{"input_tokens":8,"output_tokens":1}}}` + "\n\n"
		case "/v1/images/generations":
			body = `{"data":[{"b64_json":"fixture-image-data"}]}`
		}
	default:
		return pluginapi.HTTPResponse{}, fmt.Errorf("unexpected route %s", parsed.Path)
	}
	return pluginapi.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(body)}, nil
}

func TestProbeRelayRefusesBareClaudeMessages(t *testing.T) {
	relay := &probeRelayFixture{inferences: make(map[string]int)}
	response, err := relay.Do(context.Background(), pluginapi.HTTPRequest{
		URL: "https://relay.example/v1/messages", Headers: http.Header{"Content-Type": {"application/json"}},
		Body: []byte(`{"model":"claude-opus-5-5","messages":[{"role":"user","content":"Reply with OK."}],"max_tokens":32,"stream":false}`),
	})
	if err != nil || response.StatusCode != http.StatusForbidden || !strings.Contains(string(response.Body), "this client is not supported") || len(relay.inferences) != 0 {
		t.Fatalf("bare probe did not reproduce the client refusal: %+v, %v", response, err)
	}
}

func (f *probeRelayFixture) DoStream(context.Context, pluginapi.HTTPRequest) (pluginapi.HTTPStreamResponse, error) {
	return pluginapi.HTTPStreamResponse{}, fmt.Errorf("probe must use the executor's non-stream response")
}

func TestProbesUseRealCatalogAndExecutorWithoutDuplicateAliasCalls(t *testing.T) {
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	storage := credentials.Storage{Type: "mirasim", AccessToken: "opaque-test-token", RefreshToken: "refresh-test-token",
		DevicePrivateKey: string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})),
		RelayURL:         "https://relay.example", AdminURL: "https://admin.example", ClientVersion: "test-client",
	}
	settings := pluginconfig.Defaults()
	settings.ClientVersion = "test-client"
	pool := mirasim.NewPool()
	relay := &probeRelayFixture{inferences: make(map[string]int)}
	host := &fakeHost{
		entries: []pluginapi.HostAuthFileEntry{{ID: "fixture", AuthIndex: "fixture", Provider: "mirasim"}},
		auths:   map[string]pluginapi.HostAuthGetResponse{"fixture": {JSON: storage.JSON()}}, client: relay,
	}
	page := New(&fakeFetcher{}, ProbeServices{Models: models.New(settings, pool), Executor: executor.New(settings, pool)})
	rows := prepareFixtureProbes(t, page, host)
	if len(rows) != 9 {
		t.Fatalf("plan must include four chat models and five image models: %+v", rows)
	}
	for _, row := range rows {
		response := mustServe(t, page, probeRequest(page, "probe", row.Ticket), host)
		var result probeResult
		if err := json.Unmarshal(response.Body, &result); err != nil {
			t.Fatal(err)
		}
		if result.Status != "available" {
			t.Fatalf("%s: %s", row.Model, response.Body)
		}
	}
	if relay.inferences["kimi-code/k3"] != 1 || relay.inferences["claude-opus-5-5"] != 1 || relay.inferences["gpt-6-astra"] != 1 {
		t.Fatalf("inference counts=%v", relay.inferences)
	}
	for model, calls := range relay.inferences {
		if calls != 1 {
			t.Errorf("%s tested %d times", model, calls)
		}
	}
}
