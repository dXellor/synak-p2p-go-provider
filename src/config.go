package main

import (
	"os"
	"strings"
)

// SyncContext mirrors syncd's SyncContext dataclass exactly (same JSON field names).
type SyncContext struct {
	PairID         string         `json:"pair_id"`
	Local          string         `json:"local"`
	Direction      string         `json:"direction"`
	Interval       int            `json:"interval"`
	ProviderConfig map[string]any `json:"provider_config"`
	Exclude        []string       `json:"exclude"`
}

// ProviderStatus mirrors syncd's ProviderStatus dataclass.
type ProviderStatus struct {
	PairID   string         `json:"pair_id"`
	State    string         `json:"state"`
	LastSync float64        `json:"last_sync"`
	Error    string         `json:"error"`
	Extra    map[string]any `json:"extra"`
}

func expandHome(path string) string {
	if strings.HasPrefix(path, "~/") {
		if home, err := os.UserHomeDir(); err == nil {
			return home + path[1:]
		}
	}
	return path
}

func cfgString(cfg map[string]any, key, def string) string {
	if v, ok := cfg[key].(string); ok && v != "" {
		return v
	}
	return def
}

func cfgBool(cfg map[string]any, key string, def bool) bool {
	if v, ok := cfg[key].(bool); ok {
		return v
	}
	return def
}

func cfgFloat(cfg map[string]any, key string, def float64) float64 {
	if v, ok := cfg[key].(float64); ok {
		return v
	}
	return def
}

func cfgStrings(cfg map[string]any, key string) []string {
	raw, ok := cfg[key].([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, v := range raw {
		if s, ok := v.(string); ok {
			out = append(out, s)
		}
	}
	return out
}
