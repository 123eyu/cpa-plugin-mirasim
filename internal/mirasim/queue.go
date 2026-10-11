package mirasim

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

const (
	queueWaitLimit    = 4 * time.Minute
	queueRetryDefault = 5 * time.Second
	queueRetryMin     = time.Second
	queueRetryMax     = time.Minute
)

// A queue refusal means the relay has not started inference. Only model calls
// with that explicit signal may be resubmitted; other failures stay with CPA.
func queueModelCall(method, path string) bool {
	if method != http.MethodPost {
		return false
	}
	switch strings.TrimRight(path, "/") {
	case "/v1/messages", "/v1/responses":
		return true
	default:
		return false
	}
}

func queueRefused(status int, headers http.Header, body []byte) bool {
	if status != http.StatusServiceUnavailable || len(body) > maxErrorBody {
		return false
	}
	if headers.Get("X-Router-Queue-Class") != "" {
		return true
	}
	var payload struct {
		Code  string `json:"code"`
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	return json.Unmarshal(body, &payload) == nil && (payload.Error.Code == "platform_queue_full" || payload.Code == "platform_queue_full")
}

// Match the official client's numeric Retry-After handling, clamping before
// conversion to time.Duration so an excessive upstream value cannot overflow.
func queueRetryDelay(headers http.Header) time.Duration {
	seconds, err := strconv.ParseFloat(strings.TrimSpace(headers.Get("Retry-After")), 64)
	if err != nil || math.IsNaN(seconds) || math.IsInf(seconds, 0) || seconds <= 0 {
		return queueRetryDefault
	}
	seconds = math.Max(queueRetryMin.Seconds(), math.Min(queueRetryMax.Seconds(), seconds))
	return time.Duration(seconds * float64(time.Second))
}

// State belongs to one request, never to the credential or another turn. The
// budget limits queue waits without imposing a timeout on a successful reply.
type queueRetryState struct {
	heldSince time.Time
}

func (q *queueRetryState) next(now time.Time, headers http.Header) (time.Duration, bool) {
	if q.heldSince.IsZero() {
		q.heldSince = now
	}
	delay := queueRetryDelay(headers)
	if now.Sub(q.heldSince)+delay > queueWaitLimit {
		return 0, false
	}
	return delay, true
}

func waitQueueRetry(ctx context.Context, delay time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return ctx.Err()
	}
}

// Buffer only a bounded refusal body before deciding whether it is safe to
// retry. An oversized, interrupted or unrecognized response is replayed to the
// caller, including its transport error and any unread chunks.
func readQueueResponse(ctx context.Context, chunks <-chan pluginapi.HTTPStreamChunk) (pluginapi.HTTPStreamChunk, bool) {
	var buffered pluginapi.HTTPStreamChunk
	if chunks == nil {
		return buffered, false
	}
	for {
		select {
		case <-ctx.Done():
			buffered.Err = ctx.Err()
			return buffered, false
		case chunk, ok := <-chunks:
			if !ok {
				return buffered, true
			}
			buffered.Payload = append(buffered.Payload, chunk.Payload...)
			buffered.Err = chunk.Err
			if chunk.Err != nil || len(buffered.Payload) > maxErrorBody {
				return buffered, false
			}
		}
	}
}

func replayQueueResponse(ctx context.Context, buffered pluginapi.HTTPStreamChunk, rest <-chan pluginapi.HTTPStreamChunk, complete bool) <-chan pluginapi.HTTPStreamChunk {
	output := make(chan pluginapi.HTTPStreamChunk, 1)
	if len(buffered.Payload) > 0 || buffered.Err != nil {
		output <- buffered
	}
	if complete || rest == nil {
		close(output)
		return output
	}
	go func() {
		defer close(output)
		for {
			select {
			case <-ctx.Done():
				return
			case chunk, ok := <-rest:
				if !ok {
					return
				}
				select {
				case output <- chunk:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return output
}
