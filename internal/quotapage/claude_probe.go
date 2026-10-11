package quotapage

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/url"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

const (
	probePrompt            = "Reply with OK."
	claudeProbeVersion     = "2.1.292"
	claudeProbeIdentity    = "You are a Claude agent, built on Anthropic's Claude Agent SDK."
	claudeProbeBillingText = "x-anthropic-billing-header: cc_version=" + claudeProbeVersion + ".61d; cc_entrypoint=sdk-cli;"
)

// applyClaudeProbeProfile supplies the client envelope missing from a bare
// Messages request. The baseline was captured from Claude Code 2.1.292 using
// a local mock endpoint, --tools "", and the fixed probe prompt. Its custom
// gateway leg omits OAuth betas and cch; the relay supplies its own auth/signing.
// The billing suffix 61d fingerprints probePrompt with this client version.
// Keep this profile local to quota probes; callers own their request identity.
func applyClaudeProbeProfile(request *pluginapi.ExecutorRequest, body map[string]any) {
	var device [32]byte
	var session [16]byte
	_, _ = rand.Read(device[:])
	_, _ = rand.Read(session[:])
	session[6] = session[6]&0x0f | 0x40
	session[8] = session[8]&0x3f | 0x80
	encoded := hex.EncodeToString(session[:])
	sessionID := encoded[:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:]
	userID, _ := json.Marshal(map[string]string{
		"device_id": hex.EncodeToString(device[:]), "account_uuid": "", "session_id": sessionID,
	})
	request.Metadata = map[string]any{"execution_session_id": sessionID}
	request.Query = url.Values{"beta": {"true"}}
	for name, value := range map[string]string{
		"User-Agent":               "claude-cli/" + claudeProbeVersion + " (external, sdk-cli)",
		"X-App":                    "cli",
		"X-Claude-Code-Session-Id": sessionID,
		"Anthropic-Version":        "2023-06-01",
		"Anthropic-Beta":           "interleaved-thinking-2025-05-14,thinking-token-count-2026-05-13,context-management-2025-06-27,prompt-caching-scope-2026-01-05,claude-code-20250219",
		"Anthropic-Dangerous-Direct-Browser-Access": "true",
		"X-Stainless-Lang":                          "js",
		"X-Stainless-Package-Version":               "0.128.0",
		"X-Stainless-Retry-Count":                   "0",
		"X-Stainless-Runtime":                       "node",
		"X-Stainless-Runtime-Version":               "v26.3.0",
		"X-Stainless-OS":                            "MacOS",
		"X-Stainless-Arch":                          "arm64",
	} {
		request.Headers.Set(name, value)
	}
	body["messages"] = []map[string]any{{"role": "user", "content": []map[string]string{{"type": "text", "text": probePrompt}}}}
	body["system"] = []map[string]any{
		{"type": "text", "text": claudeProbeBillingText},
		{"type": "text", "text": claudeProbeIdentity, "cache_control": map[string]string{"type": "ephemeral"}},
		{"type": "text", "text": probePrompt, "cache_control": map[string]string{"type": "ephemeral"}},
	}
	body["metadata"] = map[string]string{"user_id": string(userID)}
	body["tools"] = []any{}
	// A 32-token availability check must not enable Claude Code's normal
	// 31,999-token thinking budget or ask for tool execution.
	body["thinking"] = map[string]string{"type": "disabled"}
}
