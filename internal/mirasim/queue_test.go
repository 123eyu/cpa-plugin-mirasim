package mirasim

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

func queueTestHost(do func(context.Context, pluginapi.HTTPRequest) (pluginapi.HTTPResponse, error)) fakeHostClient {
	return fakeHostClient{do: do, doStream: func(ctx context.Context, req pluginapi.HTTPRequest) (pluginapi.HTTPStreamResponse, error) {
		resp, err := do(ctx, req)
		if err != nil {
			return pluginapi.HTTPStreamResponse{}, err
		}
		chunks := make(chan pluginapi.HTTPStreamChunk, 2)
		mid := len(resp.Body) / 2
		chunks <- pluginapi.HTTPStreamChunk{Payload: resp.Body[:mid]}
		chunks <- pluginapi.HTTPStreamChunk{Payload: resp.Body[mid:]}
		close(chunks)
		return pluginapi.HTTPStreamResponse{StatusCode: resp.StatusCode, Headers: resp.Headers, Chunks: chunks}, nil
	}}
}

func queueTestCall(ctx context.Context, client *Client, host fakeHostClient, stream bool, method, path string) (pluginapi.HTTPResponse, error) {
	body := []byte(`{"model":"test-model","input":"hello"}`)
	query := url.Values{"beta": {"1"}}
	headers := http.Header{"Anthropic-Beta": {"test-beta"}}
	if !stream {
		return client.Do(ctx, host, method, path, query, headers, body)
	}
	resp, err := client.DoStream(ctx, host, method, path, query, headers, body)
	if err != nil {
		return pluginapi.HTTPResponse{}, err
	}
	result := pluginapi.HTTPResponse{StatusCode: resp.StatusCode, Headers: resp.Headers}
	for chunk := range resp.Chunks {
		result.Body = append(result.Body, chunk.Payload...)
		if chunk.Err != nil {
			err = chunk.Err
		}
	}
	return result, err
}

func TestQueueRetryDelayMatchesOfficialBounds(t *testing.T) {
	for raw, want := range map[string]time.Duration{
		"": 5 * time.Second, "garbage": 5 * time.Second, "0": 5 * time.Second, "-1": 5 * time.Second,
		"NaN": 5 * time.Second, "Infinity": 5 * time.Second, "0.25": time.Second,
		" 17 ": 17 * time.Second, "1.5": 1500 * time.Millisecond, "90": time.Minute, "1e30": time.Minute,
	} {
		t.Run(raw, func(t *testing.T) {
			if got := queueRetryDelay(http.Header{"Retry-After": {raw}}); got != want {
				t.Fatalf("delay=%s, want %s", got, want)
			}
		})
	}
}

func TestQueueRefusalsWaitAndRetryBothTransports(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, path := range []string{"/v1/messages", "/v1/responses"} {
			t.Run(fmt.Sprintf("%s/stream=%t", path, stream), func(t *testing.T) {
				storage, publicKey, relayPrivate := newTestStorage(t, futureJWT())
				client := NewClient(storage)
				now := time.Now()
				client.now = func() time.Time { return now }
				var delays []time.Duration
				client.queueWait = func(_ context.Context, delay time.Duration) error {
					delays = append(delays, delay)
					now = now.Add(delay)
					return nil
				}
				mintCalls, relayCalls := 0, 0
				callIDs := map[string]bool{}
				host := queueTestHost(func(_ context.Context, req pluginapi.HTTPRequest) (pluginapi.HTTPResponse, error) {
					if mustRequestPath(t, req.URL) == sessionPath {
						mintCalls++
						return pluginapi.HTTPResponse{StatusCode: 200, Body: []byte(`{"ticket":"ticket","expiresIn":900}`)}, nil
					}
					relayCalls++
					assertSealedRelayRequest(t, publicKey, relayPrivate, req, "ticket")
					metadata := decryptRelayMetadata(t, relayPrivate, req.Method, path, req.Headers.Get(headerMirasimEncryptedMetadata))
					callID := metadata[headerMirasimCall]
					if callID == "" || callIDs[callID] {
						t.Errorf("retry reused call ID %q", callID)
					}
					callIDs[callID] = true
					if !strings.HasSuffix(req.URL, path+"?beta=1") || req.Headers.Get("Anthropic-Beta") != "test-beta" || string(req.Body) != `{"model":"test-model","input":"hello"}` {
						t.Fatalf("retry changed business request: %s %s", req.URL, req.Body)
					}
					switch relayCalls {
					case 1:
						return pluginapi.HTTPResponse{StatusCode: 503, Headers: http.Header{"X-Router-Queue-Class": {"model"}, "Retry-After": {"17"}}, Body: []byte(`{"error":{"message":"queued"}}`)}, nil
					case 2:
						return pluginapi.HTTPResponse{StatusCode: 503, Headers: http.Header{"Retry-After": {"0.25"}}, Body: []byte(`{"error":{"code":"platform_queue_full"}}`)}, nil
					default:
						return pluginapi.HTTPResponse{StatusCode: 200, Headers: http.Header{"X-Test": {"final"}}, Body: []byte("successful response")}, nil
					}
				})
				resp, err := queueTestCall(context.Background(), client, host, stream, http.MethodPost, path)
				if err != nil || resp.StatusCode != 200 || string(resp.Body) != "successful response" || resp.Headers.Get("X-Test") != "final" {
					t.Fatalf("response=%+v err=%v", resp, err)
				}
				if mintCalls != 1 || relayCalls != 3 || !reflect.DeepEqual(delays, []time.Duration{17 * time.Second, time.Second}) {
					t.Fatalf("mints=%d calls=%d delays=%v", mintCalls, relayCalls, delays)
				}
			})
		}
	}
}

func TestQueueWaitBudgetReturnsLastRefusalAndResetsPerRequest(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprint(stream), func(t *testing.T) {
			storage, _, _ := newTestStorage(t, futureJWT())
			client := NewClient(storage)
			now := time.Now()
			client.now = func() time.Time { return now }
			waits, calls := 0, 0
			client.queueWait = func(_ context.Context, delay time.Duration) error {
				waits++
				now = now.Add(delay)
				return nil
			}
			host := queueTestHost(func(_ context.Context, req pluginapi.HTTPRequest) (pluginapi.HTTPResponse, error) {
				if mustRequestPath(t, req.URL) == sessionPath {
					return pluginapi.HTTPResponse{StatusCode: 200, Body: []byte(`{"ticket":"ticket","expiresIn":1800}`)}, nil
				}
				calls++
				return pluginapi.HTTPResponse{StatusCode: 503, Headers: http.Header{"Retry-After": {"60"}, "X-Router-Queue-Class": {"model"}}, Body: []byte(fmt.Sprintf(`{"attempt":%d}`, calls))}, nil
			})
			for request := 1; request <= 2; request++ {
				resp, err := queueTestCall(context.Background(), client, host, stream, http.MethodPost, "/v1/messages")
				if err != nil || resp.StatusCode != 503 || string(resp.Body) != fmt.Sprintf(`{"attempt":%d}`, request*5) || resp.Headers.Get("Retry-After") != "60" {
					t.Fatalf("last refusal changed: response=%+v err=%v", resp, err)
				}
				if calls != request*5 || waits != request*4 {
					t.Fatalf("request=%d calls=%d waits=%d", request, calls, waits)
				}
			}
		})
	}
}

func TestQueueOnlyRetriesExplicitModelRefusals(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, tc := range []struct {
			name, method, path, body string
			status                   int
			queueHeader              bool
		}{
			{"ordinary unavailable", "POST", "/v1/messages", `{"error":{"code":"model_unavailable"}}`, 503, false},
			{"invalid error", "POST", "/v1/responses", `invalid`, 503, false},
			{"rate limited", "POST", "/v1/messages", `{}`, 429, true},
			{"server error", "POST", "/v1/messages", `{}`, 500, true},
			{"successful HTTP", "POST", "/v1/messages", `{"error":{"code":"platform_queue_full"}}`, 200, true},
			{"catalog", "GET", modelsPath, `{}`, 503, true},
			{"limits", "GET", limitsPath, `{}`, 503, true},
			{"token count", "POST", "/v1/messages/count_tokens", `{}`, 503, true},
			{"reset card", "POST", "/v1/reset-cards/card/redeem", `{}`, 503, true},
			{"compact", "POST", "/v1/responses/compact", `{}`, 503, true},
			{"image", "POST", "/v1/images/generations", `{}`, 503, true},
		} {
			t.Run(fmt.Sprintf("%s/stream=%t", tc.name, stream), func(t *testing.T) {
				storage, _, _ := newTestStorage(t, futureJWT())
				client := NewClient(storage)
				client.queueWait = func(context.Context, time.Duration) error {
					t.Fatal("unrelated response was retried")
					return nil
				}
				calls := 0
				host := queueTestHost(func(_ context.Context, req pluginapi.HTTPRequest) (pluginapi.HTTPResponse, error) {
					if mustRequestPath(t, req.URL) == sessionPath {
						return pluginapi.HTTPResponse{StatusCode: 200, Body: []byte(`{"ticket":"ticket","expiresIn":900}`)}, nil
					}
					calls++
					headers := make(http.Header)
					if tc.queueHeader {
						headers.Set("X-Router-Queue-Class", "model")
					}
					return pluginapi.HTTPResponse{StatusCode: tc.status, Headers: headers, Body: []byte(tc.body)}, nil
				})
				resp, err := queueTestCall(context.Background(), client, host, stream, tc.method, tc.path)
				if err != nil || calls != 1 || resp.StatusCode != tc.status || string(resp.Body) != tc.body {
					t.Fatalf("calls=%d response=%+v err=%v", calls, resp, err)
				}
			})
		}
	}
}

func TestQueueAndUnauthorizedRetryBudgetsAreIndependent(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, finalStatus := range []int{200, 401} {
			t.Run(fmt.Sprintf("stream=%t/final=%d", stream, finalStatus), func(t *testing.T) {
				storage, _, _ := newTestStorage(t, futureJWT())
				client := NewClient(storage)
				now := time.Now()
				client.now = func() time.Time { return now }
				waits, mints, calls := 0, 0, 0
				client.queueWait = func(_ context.Context, delay time.Duration) error { waits++; now = now.Add(delay); return nil }
				host := queueTestHost(func(_ context.Context, req pluginapi.HTTPRequest) (pluginapi.HTTPResponse, error) {
					if mustRequestPath(t, req.URL) == sessionPath {
						mints++
						return pluginapi.HTTPResponse{StatusCode: 200, Body: []byte(fmt.Sprintf(`{"ticket":"ticket-%d","expiresIn":900}`, mints))}, nil
					}
					calls++
					statuses := []int{503, 401, 503, finalStatus}
					if calls > len(statuses) {
						t.Fatal("retry budget was reset")
					}
					return pluginapi.HTTPResponse{StatusCode: statuses[calls-1], Headers: http.Header{"X-Router-Queue-Class": {"model"}}, Body: []byte(`{"error":{"code":"platform_queue_full"}}`)}, nil
				})
				resp, err := queueTestCall(context.Background(), client, host, stream, http.MethodPost, "/v1/messages")
				if err != nil || resp.StatusCode != finalStatus || calls != 4 || mints != 2 || waits != 2 || client.refreshRequired != (finalStatus == 401) {
					t.Fatalf("status=%d err=%v calls=%d mints=%d waits=%d refresh=%t", resp.StatusCode, err, calls, mints, waits, client.refreshRequired)
				}
			})
		}
	}
}

func TestQueueCancellationDoesNotBlockOtherCalls(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprint(stream), func(t *testing.T) {
			storage, _, _ := newTestStorage(t, futureJWT())
			client := NewClient(storage)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			waiting := make(chan struct{})
			client.queueWait = func(ctx context.Context, delay time.Duration) error {
				close(waiting)
				return waitQueueRetry(ctx, delay)
			}
			host := queueTestHost(func(_ context.Context, req pluginapi.HTTPRequest) (pluginapi.HTTPResponse, error) {
				switch mustRequestPath(t, req.URL) {
				case sessionPath:
					return pluginapi.HTTPResponse{StatusCode: 200, Body: []byte(`{"ticket":"ticket","expiresIn":900}`)}, nil
				case "/v1/responses":
					return pluginapi.HTTPResponse{StatusCode: 200, Body: []byte("other call succeeded")}, nil
				default:
					return pluginapi.HTTPResponse{StatusCode: 503, Headers: http.Header{"X-Router-Queue-Class": {"model"}, "Retry-After": {"60"}}}, nil
				}
			})
			done := make(chan error, 1)
			go func() {
				_, err := queueTestCall(ctx, client, host, stream, http.MethodPost, "/v1/messages")
				done <- err
			}()
			<-waiting
			resp, err := queueTestCall(context.Background(), client, host, stream, http.MethodPost, "/v1/responses")
			if err != nil || resp.StatusCode != 200 {
				t.Fatalf("queued request blocked another call: %v", err)
			}
			cancel()
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Fatalf("cancel error=%v", err)
			}
		})
	}
}

func TestQueueStreamDoesNotRetryIncompleteRefusals(t *testing.T) {
	sentinel := errors.New("connection interrupted")
	for _, tc := range []struct {
		name   string
		chunks []pluginapi.HTTPStreamChunk
		want   string
		err    error
	}{
		{"transport error", []pluginapi.HTTPStreamChunk{{Payload: []byte(`{"error":{"code":"platform_queue_full"}}`), Err: sentinel}}, `{"error":{"code":"platform_queue_full"}}`, sentinel},
		{"oversized with unread tail", []pluginapi.HTTPStreamChunk{{Payload: []byte(strings.Repeat("x", maxErrorBody+1))}, {Payload: []byte("tail"), Err: sentinel}}, strings.Repeat("x", maxErrorBody+1) + "tail", sentinel},
	} {
		t.Run(tc.name, func(t *testing.T) {
			storage, _, _ := newTestStorage(t, futureJWT())
			client := NewClient(storage)
			client.queueWait = func(context.Context, time.Duration) error { t.Fatal("incomplete refusal retried"); return nil }
			calls := 0
			host := fakeHostClient{
				do: func(context.Context, pluginapi.HTTPRequest) (pluginapi.HTTPResponse, error) {
					return pluginapi.HTTPResponse{StatusCode: 200, Body: []byte(`{"ticket":"ticket","expiresIn":900}`)}, nil
				},
				doStream: func(context.Context, pluginapi.HTTPRequest) (pluginapi.HTTPStreamResponse, error) {
					calls++
					chunks := make(chan pluginapi.HTTPStreamChunk, len(tc.chunks))
					for _, chunk := range tc.chunks {
						chunks <- chunk
					}
					close(chunks)
					return pluginapi.HTTPStreamResponse{StatusCode: 503, Headers: http.Header{"X-Router-Queue-Class": {"model"}}, Chunks: chunks}, nil
				},
			}
			resp, err := queueTestCall(context.Background(), client, host, true, http.MethodPost, "/v1/messages")
			if calls != 1 || resp.StatusCode != 503 || string(resp.Body) != tc.want || !errors.Is(err, tc.err) {
				t.Fatalf("refusal changed: calls=%d status=%d bytes=%d err=%v", calls, resp.StatusCode, len(resp.Body), err)
			}
		})
	}
}
