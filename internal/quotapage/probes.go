package quotapage

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
	"github.com/router-for-me/CLIProxyAPIPlugins/mirasim/internal/credentials"
	"github.com/router-for-me/CLIProxyAPIPlugins/mirasim/internal/thinking"
)

const probeHeader = "X-Mirasim-Probe"

type ProbeModels interface {
	ModelsForAuth(context.Context, pluginapi.AuthModelRequest) (pluginapi.ModelResponse, error)
}

type ProbeExecutor interface {
	Execute(context.Context, pluginapi.ExecutorRequest) (pluginapi.ExecutorResponse, error)
}

type ProbeServices struct {
	Models   ProbeModels
	Executor ProbeExecutor
}

type probeRow struct {
	Ticket  string   `json:"ticket,omitempty"`
	Account int      `json:"account"`
	Model   string   `json:"model"`
	Aliases []string `json:"aliases,omitempty"`
	Kind    string   `json:"kind"`
	Problem string   `json:"problem,omitempty"`
}

type probeResult struct {
	Status     string `json:"status"`
	HTTPStatus int    `json:"http_status,omitempty"`
	DurationMS int64  `json:"duration_ms"`
	Response   string `json:"response"`
	CheckedAt  string `json:"checked_at"`
}

type probeTicket struct {
	row       probeRow
	authIndex string
	authID    string
	expires   time.Time
	done      chan struct{}
	result    probeResult
}

type probeStore struct {
	mu      sync.Mutex
	tickets map[string]*probeTicket
	gate    chan struct{}
	token   string
}

func newProbeStore() *probeStore {
	return &probeStore{tickets: make(map[string]*probeTicket), gate: make(chan struct{}, 1), token: rand.Text()}
}

// CPA resource routes are GET-only. A non-simple header, a page-scoped token,
// and single-use execution tickets keep navigation/prefetch/replay from issuing
// inference requests. Credentials and arbitrary request bodies stay server-side.
func (p *Page) serveProbe(ctx context.Context, req pluginapi.ManagementRequest, host HostServices) (pluginapi.ManagementResponse, error) {
	if p.probes == nil || p.services.Models == nil || p.services.Executor == nil {
		return probeJSON(http.StatusServiceUnavailable, map[string]string{"error": "模型测试暂不可用。"}), nil
	}
	site := req.Headers.Get("Sec-Fetch-Site")
	if subtle.ConstantTimeCompare([]byte(req.Headers.Get(probeHeader)), []byte(p.probes.token)) != 1 || site != "" && site != "same-origin" && site != "none" {
		return probeJSON(http.StatusForbidden, map[string]string{"error": "请刷新配额页面后点击测试按钮。"}), nil
	}
	switch req.Query.Get("action") {
	case "models":
		return p.prepareProbes(ctx, host)
	case "probe":
		return p.runProbe(ctx, req.Query.Get("ticket"), host)
	default:
		return probeJSON(http.StatusBadRequest, map[string]string{"error": "未知的测试操作。"}), nil
	}
}

func (p *Page) prepareProbes(ctx context.Context, host HostServices) (pluginapi.ManagementResponse, error) {
	entries, err := host.ListAuth(ctx)
	if err != nil {
		return probeJSON(http.StatusBadGateway, map[string]string{"error": "无法读取 Mirasim 账户。"}), nil
	}
	rows := make([]probeRow, 0)
	tickets := make(map[string]*probeTicket)
	number := 0
	for _, entry := range entries {
		if !isMirasimAuth(entry) {
			continue
		}
		number++
		if entry.Disabled {
			rows = append(rows, probeRow{Account: number, Problem: "账户已停用，未发送测试请求。"})
			continue
		}
		auth, errGet := host.GetAuth(ctx, entry.AuthIndex)
		if errGet != nil || len(auth.JSON) == 0 {
			rows = append(rows, probeRow{Account: number, Problem: "无法读取此账户的凭据。"})
			continue
		}
		catalog, errModels := p.services.Models.ModelsForAuth(ctx, pluginapi.AuthModelRequest{
			AuthID: entry.ID, StorageJSON: auth.JSON, HTTPClient: host.HTTPClient(),
		})
		if errModels != nil {
			rows = append(rows, probeRow{Account: number, Problem: "无法读取此账户的模型目录。"})
			continue
		}
		unique := make(map[string]*probeRow)
		for _, model := range catalog.Models {
			id := thinking.ParseModel(model.ID).ModelName
			if id == "" {
				continue
			}
			key := strings.ToLower(id)
			row := unique[key]
			if row == nil {
				row = &probeRow{Account: number, Model: id, Kind: "chat"}
				if model.Type == "openai-image" || strings.HasPrefix(key, "gpt-image-") {
					row.Kind = "image"
				}
				unique[key] = row
			}
			if model.ID != id {
				row.Aliases = append(row.Aliases, model.ID)
			}
		}
		ids := make([]string, 0, len(unique))
		for id := range unique {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			row := *unique[id]
			row.Ticket = rand.Text()
			rows = append(rows, row)
			tickets[row.Ticket] = &probeTicket{row: row, authIndex: entry.AuthIndex, authID: entry.ID, expires: time.Now().Add(time.Hour)}
		}
		if len(ids) == 0 {
			rows = append(rows, probeRow{Account: number, Problem: "此账户没有可测试的模型。"})
		}
	}
	p.probes.mu.Lock()
	defer p.probes.mu.Unlock()
	for key, ticket := range p.probes.tickets {
		if time.Now().After(ticket.expires) {
			delete(p.probes.tickets, key)
		}
	}
	if len(p.probes.tickets)+len(tickets) > 2048 {
		return probeJSON(http.StatusTooManyRequests, map[string]string{"error": "测试次数过多，请稍后再试。"}), nil
	}
	for key, ticket := range tickets {
		p.probes.tickets[key] = ticket
	}
	return probeJSON(http.StatusOK, struct {
		Rows []probeRow `json:"rows"`
	}{Rows: rows}), nil
}

func (p *Page) runProbe(ctx context.Context, key string, host HostServices) (pluginapi.ManagementResponse, error) {
	p.probes.mu.Lock()
	ticket := p.probes.tickets[key]
	if ticket == nil || time.Now().After(ticket.expires) {
		p.probes.mu.Unlock()
		return probeJSON(http.StatusGone, map[string]string{"error": "测试已过期，请重新点击一键测试。"}), nil
	}
	if ticket.done != nil {
		done := ticket.done
		p.probes.mu.Unlock()
		select {
		case <-done:
			return probeJSON(http.StatusOK, ticket.result), nil
		case <-ctx.Done():
			return probeJSON(http.StatusRequestTimeout, map[string]string{"error": "测试等待已取消。"}), nil
		}
	}
	ticket.done = make(chan struct{})
	p.probes.mu.Unlock()
	result := probeResult{Status: "cancelled", Response: "请求已取消，结果未知。", CheckedAt: time.Now().UTC().Format(time.RFC3339)}
	select {
	case p.probes.gate <- struct{}{}:
		result = func() (out probeResult) {
			defer func() {
				<-p.probes.gate
				if recover() != nil {
					out = probeResult{Status: "unavailable", Response: "测试执行失败。", CheckedAt: time.Now().UTC().Format(time.RFC3339)}
				}
			}()
			return p.executeProbe(ctx, ticket, host)
		}()
	case <-ctx.Done():
	}
	p.probes.mu.Lock()
	ticket.result = result
	close(ticket.done)
	p.probes.mu.Unlock()
	return probeJSON(http.StatusOK, result), nil
}

func (p *Page) executeProbe(ctx context.Context, ticket *probeTicket, host HostServices) probeResult {
	start := time.Now()
	result := probeResult{Status: "unavailable", CheckedAt: start.UTC().Format(time.RFC3339)}
	// Revalidate the account before spending quota; auth indexes can be reused.
	entries, err := host.ListAuth(ctx)
	found := false
	for _, entry := range entries {
		found = found || isMirasimAuth(entry) && !entry.Disabled && entry.ID == ticket.authID && entry.AuthIndex == ticket.authIndex
	}
	if err != nil || !found {
		result.Response = "账户已变更或停用，请重新读取模型列表。"
		return result
	}
	auth, err := host.GetAuth(ctx, ticket.authIndex)
	if err != nil || len(auth.JSON) == 0 {
		result.Response = "无法读取此账户的凭据。"
		return result
	}
	request := minimalProbeRequest(ticket.row.Model, ticket.row.Kind)
	request.AuthID, request.AuthProvider = ticket.authID, credentials.Provider
	request.StorageJSON, request.HTTPClient = auth.JSON, host.HTTPClient()
	response, err := p.services.Executor.Execute(ctx, request)
	result.DurationMS = time.Since(start).Milliseconds()
	if err != nil {
		var status interface{ StatusCode() int }
		if errors.As(err, &status) {
			result.HTTPStatus = status.StatusCode()
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			result.Status = "cancelled"
		}
		result.Response = safeProbeText(err.Error(), auth.JSON)
		if result.HTTPStatus == http.StatusForbidden && strings.Contains(strings.ToLower(err.Error()), "this client is not supported; use the mirasim client or claude code") {
			result.Status = "client_rejected"
			result.Response = "上游拒绝了测试请求的客户端身份，本次无法确认模型是否可用。\n" + result.Response
		}
		return result
	}
	result.HTTPStatus = http.StatusOK
	result.Status, result.Response = summarizeProbeResponse(response.Payload, ticket.row.Kind)
	result.Response = safeProbeText(result.Response, auth.JSON)
	return result
}

func minimalProbeRequest(model, kind string) pluginapi.ExecutorRequest {
	// Inspect each relay's native response without cross-protocol translation.
	request := pluginapi.ExecutorRequest{Model: model, SourceFormat: "claude", Format: "claude", Headers: http.Header{"Content-Type": {"application/json"}}}
	body := map[string]any{"model": model, "messages": []map[string]string{{"role": "user", "content": probePrompt}}, "max_tokens": 32, "stream": false}
	if kind == "image" {
		request.SourceFormat, request.Format = "openai-image", "openai-image"
		request.Metadata = map[string]any{"request_path": "/v1/images/generations"}
		body = map[string]any{"model": model, "prompt": "A black dot on white.", "n": 1}
	} else if strings.HasPrefix(strings.ToLower(model), "gpt-") {
		request.SourceFormat, request.Format = "codex", "codex"
		body = map[string]any{"model": model, "instructions": "", "store": false,
			"input": []map[string]any{{"type": "message", "role": "user", "content": []map[string]string{{"type": "input_text", "text": probePrompt}}}},
		}
	} else if strings.HasPrefix(strings.ToLower(model), "claude-") {
		applyClaudeProbeProfile(&request, body)
	}
	request.Payload, _ = json.Marshal(body)
	request.OriginalRequest = append([]byte(nil), request.Payload...)
	return request
}

func summarizeProbeResponse(raw []byte, kind string) (string, string) {
	type block struct {
		Type     string `json:"type"`
		Text     string `json:"text"`
		Thinking string `json:"thinking"`
	}
	var response struct {
		Error    json.RawMessage `json:"error"`
		Type     string          `json:"type"`
		Response json.RawMessage `json:"response"`
		Content  []block         `json:"content"`
		Output   []struct {
			Content []block `json:"content"`
		} `json:"output"`
		Choices []struct {
			Message struct {
				Content   json.RawMessage `json:"content"`
				Reasoning string          `json:"reasoning_content"`
			} `json:"message"`
			Finish string `json:"finish_reason"`
		} `json:"choices"`
		Data []struct {
			URL    string `json:"url"`
			Base64 string `json:"b64_json"`
		} `json:"data"`
	}
	if json.Unmarshal(raw, &response) != nil {
		return "unavailable", "上游返回了无法解析的响应。"
	}
	if len(response.Error) > 0 && string(response.Error) != "null" {
		return "unavailable", string(response.Error)
	}
	if len(response.Response) > 0 && string(response.Response) != "null" {
		// Execute aggregates a Codex SSE response into its terminal event.
		return summarizeProbeResponse(response.Response, kind)
	}
	if kind == "image" {
		count := 0
		for _, item := range response.Data {
			if item.URL != "" || item.Base64 != "" {
				count++
			}
		}
		if count > 0 {
			return "available", fmt.Sprintf("已收到 %d 张图片的生成响应。", count)
		}
		return "empty", "请求已完成，但没有返回图片。"
	}
	blocks := append([]block(nil), response.Content...)
	for _, item := range response.Output {
		blocks = append(blocks, item.Content...)
	}
	var content, reasoning strings.Builder
	for _, part := range blocks {
		content.WriteString(part.Text)
		reasoning.WriteString(part.Thinking)
	}
	if strings.TrimSpace(content.String()) != "" {
		return "available", content.String()
	}
	if strings.TrimSpace(reasoning.String()) != "" {
		return "available", "思考响应：" + reasoning.String()
	}
	if len(response.Choices) == 0 {
		return "empty", "请求已完成，但没有返回模型内容。"
	}
	choice := response.Choices[0]
	var text string
	if json.Unmarshal(choice.Message.Content, &text) != nil {
		var blocks []struct {
			Text string `json:"text"`
		}
		if json.Unmarshal(choice.Message.Content, &blocks) == nil {
			for _, block := range blocks {
				text += block.Text
			}
		}
	}
	if strings.TrimSpace(text) != "" {
		return "available", text
	}
	if strings.TrimSpace(choice.Message.Reasoning) != "" {
		return "available", "思考响应：" + choice.Message.Reasoning
	}
	return "empty", "已响应但没有文本，结束原因：" + choice.Finish
}

func safeProbeText(value string, authJSON []byte) string {
	var auth map[string]any
	_ = json.Unmarshal(authJSON, &auth)
	for key, raw := range auth {
		key = strings.ToLower(key)
		if strings.Contains(key, "token") || strings.Contains(key, "key") || strings.Contains(key, "secret") || key == "email" || key == "account_id" {
			if secret, ok := raw.(string); ok && secret != "" {
				value = strings.ReplaceAll(value, secret, "[redacted]")
				value = strings.ReplaceAll(value, url.QueryEscape(secret), "[redacted]")
				encoded, _ := json.Marshal(secret)
				value = strings.ReplaceAll(value, string(encoded[1:len(encoded)-1]), "[redacted]")
			}
		}
	}
	runes := []rune(value)
	if len(runes) > 2000 {
		return string(runes[:2000]) + "…"
	}
	return value
}

func probeJSON(status int, value any) pluginapi.ManagementResponse {
	body, _ := json.Marshal(value)
	return pluginapi.ManagementResponse{StatusCode: status, Headers: http.Header{
		"Content-Type": {"application/json; charset=utf-8"}, "Cache-Control": {"no-store"}, "X-Content-Type-Options": {"nosniff"},
	}, Body: body}
}
