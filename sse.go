package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
)

func readValidSSEPrefix(body io.Reader, chat bool) ([]byte, *bufio.Reader, error) {
	reader := bufio.NewReader(body)
	prefix := make([]byte, 0, 4096)
	var line, data []byte
	errorEvent := false
	for {
		fragment, err := reader.ReadSlice('\n')
		if len(prefix)+len(fragment) > maxSSEPrefixBytes {
			return nil, nil, errors.New("upstream stream prefix exceeded validation limit")
		}
		prefix = append(prefix, fragment...)
		line = append(line, fragment...)
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if err != nil {
			return nil, nil, err
		}
		trimmed := bytes.TrimSuffix(bytes.TrimSuffix(line, []byte("\n")), []byte("\r"))
		if len(trimmed) == 0 {
			payload := bytes.TrimSpace(data)
			if len(payload) > 0 {
				if bytes.Equal(payload, []byte("[DONE]")) {
					return nil, nil, errors.New("upstream stream ended before meaningful output")
				}
				if payloadErr, exists := structuredPayloadError(payload, true); exists {
					return prefix, nil, payloadErr
				}
				if errorEvent {
					return prefix, nil, &upstreamPayloadError{}
				}
				var object map[string]json.RawMessage
				if err := json.Unmarshal(payload, &object); err != nil || object == nil {
					return nil, nil, errors.New("invalid SSE JSON event")
				}
				if !chat || meaningfulChatEvent(object) {
					return prefix, reader, nil
				}
			}
			data = data[:0]
			errorEvent = false
		} else if trimmed[0] != ':' {
			field, value, _ := bytes.Cut(trimmed, []byte(":"))
			value = bytes.TrimPrefix(value, []byte(" "))
			switch string(field) {
			case "event":
				errorEvent = strings.EqualFold(string(value), "error")
			case "data":
				data = append(data, value...)
				data = append(data, '\n')
			}
		}
		line = line[:0]
	}
}

func meaningfulChatEvent(object map[string]json.RawMessage) bool {
	var choices []struct {
		Delta        map[string]json.RawMessage `json:"delta"`
		Text         string                     `json:"text"`
		FinishReason *string                    `json:"finish_reason"`
	}
	if json.Unmarshal(object["choices"], &choices) != nil {
		return false
	}
	for _, choice := range choices {
		if choice.Text != "" || (choice.FinishReason != nil && *choice.FinishReason != "") {
			return true
		}
		for _, name := range []string{"content", "reasoning", "reasoning_content", "refusal"} {
			var value string
			if json.Unmarshal(choice.Delta[name], &value) == nil && value != "" {
				return true
			}
		}
		var calls []struct {
			ID       string `json:"id"`
			Function struct {
				Name      string `json:"name"`
				Arguments string `json:"arguments"`
			} `json:"function"`
		}
		if json.Unmarshal(choice.Delta["tool_calls"], &calls) == nil {
			for _, call := range calls {
				if call.ID != "" || call.Function.Name != "" || call.Function.Arguments != "" {
					return true
				}
			}
		}
		var function struct {
			Name      string `json:"name"`
			Arguments string `json:"arguments"`
		}
		if json.Unmarshal(choice.Delta["function_call"], &function) == nil && (function.Name != "" || function.Arguments != "") {
			return true
		}
	}
	return false
}
