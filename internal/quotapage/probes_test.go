package quotapage

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
	"github.com/router-for-me/CLIProxyAPIPlugins/mirasim/internal/mirasim"
)

type probeModelsFake struct {
	models []pluginapi.ModelInfo
	calls  int
}

func (f *probeModelsFake) ModelsForAuth(_ context.Context, req pluginapi.AuthModelRequest) (pluginapi.ModelResponse, error) {
	f.calls++
	return pluginapi.ModelResponse{Provider: "mirasim", Models: f.models}, nil
}

type probeExecutorFake struct {
	mu       sync.Mutex
	requests []pluginapi.ExecutorRequest
	execute  func(pluginapi.ExecutorRequest) (pluginapi.ExecutorResponse, error)
}

func (f *probeExecutorFake) Execute(_ context.Context, req pluginapi.ExecutorRequest) (pluginapi.ExecutorResponse, error) {
	f.mu.Lock()
	f.requests = append(f.requests, req)
	f.mu.Unlock()
	if f.execute != nil {
		return f.execute(req)
	}
	return pluginapi.ExecutorResponse{Payload: []byte(`{"choices":[{"message":{"content":"OK"},"finish_reason":"stop"}]}`)}, nil
}

func probeFixture() (*Page, *fakeHost, *probeModelsFake, *probeExecutorFake) {
	models := &probeModelsFake{models: []pluginapi.ModelInfo{
		{ID: "claude-opus-5-5"}, {ID: "claude-opus-5-5[1m]"},
		{ID: "kimi-code/k3"}, {ID: "kimi-k3"}, {ID: "gpt-6-astra"},
		{ID: "gpt-image-2", Type: "openai-image"},
	}}
	executor := &probeExecutorFake{}
	host := &fakeHost{
		entries: []pluginapi.HostAuthFileEntry{{ID: "private-auth-id", AuthIndex: "private-index", Provider: "mirasim"}},
		auths:   map[string]pluginapi.HostAuthGetResponse{"private-index": {JSON: []byte(`{"type":"mirasim","access_token":"access-secret","refresh_token":"refresh-secret","email":"private@example.com"}`)}},
		client:  fakeHTTPClient{},
	}
	return New(&fakeFetcher{}, ProbeServices{Models: models, Executor: executor}), host, models, executor
}

func probeRequest(page *Page, action, ticket string) pluginapi.ManagementRequest {
	return pluginapi.ManagementRequest{Method: http.MethodGet, Path: quotaPageURL(page),
		Headers: http.Header{probeHeader: {page.probes.token}, "Sec-Fetch-Site": {"same-origin"}},
		Query:   url.Values{"action": {action}, "ticket": {ticket}},
	}
}

func prepareFixtureProbes(t *testing.T, page *Page, host HostServices) []probeRow {
	t.Helper()
	resp := mustServe(t, page, probeRequest(page, "models", ""), host)
	if resp.StatusCode != 200 {
		t.Fatalf("plan=%d %s", resp.StatusCode, resp.Body)
	}
	var plan struct {
		Rows []probeRow `json:"rows"`
	}
	if err := json.Unmarshal(resp.Body, &plan); err != nil {
		t.Fatal(err)
	}
	return plan.Rows
}

func TestProbePageAndPlanDoNotRunInference(t *testing.T) {
	page, host, models, executor := probeFixture()
	resp := mustServe(t, page, quotaRequest(page), host)
	if !strings.Contains(string(resp.Body), "一键测试所有模型") || !strings.Contains(resp.Headers.Get("Content-Security-Policy"), "connect-src 'self'") {
		t.Fatal("page is missing the probe controls or same-origin fetch policy")
	}
	if models.calls != 0 || len(executor.requests) != 0 {
		t.Fatal("opening the page sent model requests")
	}
	rows := prepareFixtureProbes(t, page, host)
	if len(rows) != 4 || len(executor.requests) != 0 {
		t.Fatalf("rows=%+v inference=%d", rows, len(executor.requests))
	}
	for _, row := range rows {
		if row.Ticket == "" || row.Model == "kimi-k3" || strings.Contains(row.Model, "[1m]") {
			t.Fatalf("invalid/doubled model=%+v", row)
		}
	}
	for _, secret := range []string{"private-auth-id", "private-index", "access-secret", "refresh-secret", "private@example.com"} {
		if strings.Contains(string(resp.Body), secret) {
			t.Fatalf("page leaked %q", secret)
		}
		data, _ := json.Marshal(rows)
		if strings.Contains(string(data), secret) {
			t.Fatalf("plan leaked %q", secret)
		}
	}
}

func TestProbeRequiresPageTokenAndSameOrigin(t *testing.T) {
	page, host, models, executor := probeFixture()
	for _, site := range []string{"cross-site", "same-site", "missing-token"} {
		req := probeRequest(page, "models", "")
		if site == "missing-token" {
			req.Headers.Del(probeHeader)
		} else {
			req.Headers.Set("Sec-Fetch-Site", site)
		}
		resp := mustServe(t, page, req, host)
		if resp.StatusCode != 403 {
			t.Fatalf("site=%s status=%d", site, resp.StatusCode)
		}
	}
	if models.calls != 0 || len(executor.requests) != 0 {
		t.Fatal("unauthorized probe performed work")
	}
}

func TestProbeRunsOnceAndDoesNotAcceptAnArbitraryModel(t *testing.T) {
	page, host, _, executor := probeFixture()
	rows := prepareFixtureProbes(t, page, host)
	req := probeRequest(page, "probe", rows[0].Ticket)
	req.Query.Set("model", "unregistered-model")
	for i := 0; i < 2; i++ {
		resp := mustServe(t, page, req, host)
		if resp.StatusCode != 200 || !strings.Contains(string(resp.Body), `"status":"available"`) || !strings.Contains(string(resp.Body), `"response":"OK"`) {
			t.Fatalf("probe=%d %s", resp.StatusCode, resp.Body)
		}
	}
	if len(executor.requests) != 1 {
		t.Fatalf("duplicate inference requests=%d", len(executor.requests))
	}
	call := executor.requests[0]
	if call.Model != rows[0].Model || call.AuthID != "private-auth-id" || call.AuthProvider != "mirasim" || call.HTTPClient == nil || !strings.Contains(string(call.Payload), "Reply with OK.") || !strings.Contains(string(call.Payload), `"max_tokens":32`) {
		t.Fatalf("invalid minimal request: model=%s auth=%s payload=%s", call.Model, call.AuthID, call.Payload)
	}
	resp := mustServe(t, page, probeRequest(page, "probe", "made-up-ticket"), host)
	if resp.StatusCode != 410 || len(executor.requests) != 1 {
		t.Fatal("unknown ticket triggered inference")
	}
}

func TestConcurrentProbeReplaysShareOneResult(t *testing.T) {
	page, host, _, executor := probeFixture()
	row := prepareFixtureProbes(t, page, host)[0]
	entered, release := make(chan struct{}), make(chan struct{})
	executor.execute = func(pluginapi.ExecutorRequest) (pluginapi.ExecutorResponse, error) {
		close(entered)
		<-release
		return pluginapi.ExecutorResponse{Payload: []byte(`{"choices":[{"message":{"content":"OK"}}]}`)}, nil
	}
	results := make(chan pluginapi.ManagementResponse, 2)
	go func() {
		resp, _ := page.Serve(context.Background(), probeRequest(page, "probe", row.Ticket), host)
		results <- resp
	}()
	<-entered
	go func() {
		resp, _ := page.Serve(context.Background(), probeRequest(page, "probe", row.Ticket), host)
		results <- resp
	}()
	close(release)
	first, second := <-results, <-results
	if string(first.Body) != string(second.Body) || len(executor.requests) != 1 {
		t.Fatalf("replay responses=%s / %s calls=%d", first.Body, second.Body, len(executor.requests))
	}
}

func TestProbeRevalidatesDisabledAndReplacedAccounts(t *testing.T) {
	for _, changed := range []string{"disabled", "replaced"} {
		t.Run(changed, func(t *testing.T) {
			page, host, _, executor := probeFixture()
			row := prepareFixtureProbes(t, page, host)[0]
			if changed == "disabled" {
				host.entries[0].Disabled = true
			} else {
				host.entries[0].ID = "another-account"
			}
			resp := mustServe(t, page, probeRequest(page, "probe", row.Ticket), host)
			if len(executor.requests) != 0 || !strings.Contains(string(resp.Body), "账户已变更或停用") {
				t.Fatalf("changed account tested: %s", resp.Body)
			}
		})
	}
}

func TestProbeFailureIsRedactedAndNextModelStillRuns(t *testing.T) {
	page, host, _, executor := probeFixture()
	rows := prepareFixtureProbes(t, page, host)
	executor.execute = func(req pluginapi.ExecutorRequest) (pluginapi.ExecutorResponse, error) {
		if req.Model == rows[0].Model {
			return pluginapi.ExecutorResponse{}, mirasim.NewStatusError(429, []byte(`{"error":{"message":"limit for private@example.com access-secret"}}`), nil)
		}
		return pluginapi.ExecutorResponse{Payload: []byte(`{"choices":[{"message":{"content":"OK"}}]}`)}, nil
	}
	failed := mustServe(t, page, probeRequest(page, "probe", rows[0].Ticket), host)
	if strings.Contains(string(failed.Body), "access-secret") || strings.Contains(string(failed.Body), "private@example.com") || !strings.Contains(string(failed.Body), `"http_status":429`) {
		t.Fatalf("failure=%s", failed.Body)
	}
	next := mustServe(t, page, probeRequest(page, "probe", rows[1].Ticket), host)
	if !strings.Contains(string(next.Body), `"status":"available"`) || len(executor.requests) != 2 {
		t.Fatalf("next=%s calls=%d", next.Body, len(executor.requests))
	}
}

func TestProbeDistinguishesClientRefusalFromModelPermission(t *testing.T) {
	for _, test := range []struct {
		message string
		status  string
	}{
		{"this client is not supported; use the mirasim client or Claude Code", "client_rejected"},
		{"this account cannot use this model", "unavailable"},
	} {
		t.Run(test.status, func(t *testing.T) {
			page, host, _, executor := probeFixture()
			row := prepareFixtureProbes(t, page, host)[0]
			executor.execute = func(pluginapi.ExecutorRequest) (pluginapi.ExecutorResponse, error) {
				body, _ := json.Marshal(map[string]any{"error": map[string]string{"type": "permission_error", "message": test.message}})
				return pluginapi.ExecutorResponse{}, mirasim.NewStatusError(http.StatusForbidden, body, nil)
			}
			response := mustServe(t, page, probeRequest(page, "probe", row.Ticket), host)
			var result probeResult
			if err := json.Unmarshal(response.Body, &result); err != nil {
				t.Fatal(err)
			}
			if result.Status != test.status || result.HTTPStatus != http.StatusForbidden || !strings.Contains(result.Response, test.message) {
				t.Fatalf("incorrect refusal result: %+v", result)
			}
		})
	}
}

func TestProbeSkipsDisabledAndOtherProviders(t *testing.T) {
	page, host, _, executor := probeFixture()
	host.entries = append([]pluginapi.HostAuthFileEntry{{ID: "other", AuthIndex: "other", Provider: "claude"}, {ID: "disabled", AuthIndex: "disabled", Provider: "mirasim", Disabled: true}}, host.entries...)
	rows := prepareFixtureProbes(t, page, host)
	if len(rows) != 5 || rows[0].Ticket != "" || rows[0].Problem == "" {
		t.Fatalf("rows=%+v", rows)
	}
	for _, index := range host.getCalls {
		if index != "private-index" {
			t.Fatalf("read excluded credential %q", index)
		}
	}
	if len(executor.requests) != 0 {
		t.Fatal("planning ran inference")
	}
}

func TestProbeImageRequestAndResponseClassification(t *testing.T) {
	req := minimalProbeRequest("gpt-image-2", "image")
	if req.Format != "openai-image" || req.Metadata["request_path"] != "/v1/images/generations" || !strings.Contains(string(req.Payload), `"n":1`) {
		t.Fatalf("image request=%+v", req)
	}
	for _, tt := range []struct{ kind, raw, status string }{
		{"image", `{"data":[{"b64_json":"image-bytes"}]}`, "available"},
		{"image", `{"data":[]}`, "empty"},
		{"chat", `{"choices":[{"message":{"content":""},"finish_reason":"length"}]}`, "empty"},
		{"chat", `{"error":{"message":"failed"}}`, "unavailable"},
		{"chat", `not json`, "unavailable"},
		{"chat", `{"choices":[{"message":{"content":[{"type":"text","text":"OK"}]}}]}`, "available"},
	} {
		status, text := summarizeProbeResponse([]byte(tt.raw), tt.kind)
		if status != tt.status || text == "" || strings.Contains(text, "image-bytes") {
			t.Fatalf("classification=%s %s", status, text)
		}
	}
}

func TestProbeExecutorFailureDoesNotBlockLaterTests(t *testing.T) {
	page, host, _, executor := probeFixture()
	rows := prepareFixtureProbes(t, page, host)
	executor.execute = func(pluginapi.ExecutorRequest) (pluginapi.ExecutorResponse, error) { panic("private panic") }
	resp := mustServe(t, page, probeRequest(page, "probe", rows[0].Ticket), host)
	if !strings.Contains(string(resp.Body), "测试执行失败") || strings.Contains(string(resp.Body), "private panic") {
		t.Fatalf("panic response=%s", resp.Body)
	}
	executor.execute = func(pluginapi.ExecutorRequest) (pluginapi.ExecutorResponse, error) {
		return pluginapi.ExecutorResponse{}, errors.New("next error")
	}
	resp = mustServe(t, page, probeRequest(page, "probe", rows[1].Ticket), host)
	if !strings.Contains(string(resp.Body), "next error") {
		t.Fatalf("next probe blocked: %s", resp.Body)
	}
}
