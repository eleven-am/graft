package xjson

import (
	stdjson "encoding/json"
	gjson "github.com/goccy/go-json"
)

// Marshal/Unmarshal wrappers to allow a single import site to switch
// between standard encoding/json and goccy/go-json without touching callers.

func Marshal(v interface{}) ([]byte, error) {
	return gjson.Marshal(v)
}

func Unmarshal(data []byte, v interface{}) error {
	return gjson.Unmarshal(data, v)
}

// RawMessage is kept compatible with encoding/json's RawMessage type.
type RawMessage = stdjson.RawMessage
