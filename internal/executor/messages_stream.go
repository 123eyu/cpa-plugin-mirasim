package executor

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

// Validate the complete SSE body before a non-stream translator can turn an
// upstream failure or premature EOF into an empty successful JSON response.
func messagesNonStreamPayload(body []byte) ([]byte, error) {
	scanner := bufio.NewScanner(bytes.NewReader(body))
	scanner.Buffer(make([]byte, 0, 64*1024), maxCodexEventBytes)
	terminal := false
	var normalized bytes.Buffer
	usage := make(map[string]json.RawMessage)
	for scanner.Scan() {
		line := scanner.Bytes()
		ended, errEvent := messagesStreamEvent(line)
		if errEvent != nil {
			return nil, errEvent
		}
		terminal = terminal || ended
		if bytes.HasPrefix(line, []byte("data:")) {
			var event struct {
				Type    string `json:"type"`
				Message struct {
					Usage map[string]json.RawMessage `json:"usage"`
				} `json:"message"`
				Usage map[string]json.RawMessage `json:"usage"`
			}
			raw := bytes.TrimSpace(line[len("data:"):])
			if json.Unmarshal(raw, &event) == nil {
				switch event.Type {
				case "message_start":
					usage = event.Message.Usage
					if usage == nil {
						usage = make(map[string]json.RawMessage)
					}
				case "message_delta":
					// Messages splits input/cache counts and output counts between
					// events. Some CPA aggregators only keep the last usage object.
					for key, value := range event.Usage {
						usage[key] = value
					}
					if len(usage) > 0 {
						var fields map[string]json.RawMessage
						_ = json.Unmarshal(raw, &fields)
						fields["usage"], _ = json.Marshal(usage)
						updated, _ := json.Marshal(fields)
						line = append([]byte("data: "), updated...)
					}
				}
			}
		}
		normalized.Write(line)
		normalized.WriteByte('\n')
	}
	if errScan := scanner.Err(); errScan != nil {
		return nil, fmt.Errorf("read Mirasim Messages response: %w", errScan)
	}
	if !terminal {
		return nil, fmt.Errorf("Mirasim Messages stream ended before message_stop")
	}
	return normalized.Bytes(), nil
}

// messagesStreamEvent recognizes both successful and failed terminal events.
// A transport EOF or an OpenAI [DONE] marker does not complete a Messages turn.
func messagesStreamEvent(line []byte) (bool, error) {
	line = bytes.TrimSpace(line)
	if !bytes.HasPrefix(line, []byte("data:")) {
		return false, nil
	}
	var event struct {
		Type  string `json:"type"`
		Error struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(bytes.TrimSpace(line[len("data:"):]), &event) != nil {
		return false, nil
	}
	switch event.Type {
	case "message_stop":
		return true, nil
	case "error":
		message := strings.TrimSpace(event.Error.Message)
		if message == "" {
			message = "upstream stream failed"
		}
		if len(message) > 4096 {
			message = message[:4096] + "..."
		}
		if code := strings.TrimSpace(event.Error.Type); code != "" {
			return true, fmt.Errorf("Mirasim Messages stream failed (%s): %s", code, message)
		}
		return true, fmt.Errorf("Mirasim Messages stream failed: %s", message)
	default:
		return false, nil
	}
}
