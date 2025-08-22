package static

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/eleven-am/graft/internal/domain"
	"gopkg.in/yaml.v3"
)

type PeerConfig struct {
	ID       string            `json:"id" yaml:"id"`
	Address  string            `json:"address" yaml:"address"`
	Port     int               `json:"port" yaml:"port"`
	Metadata map[string]string `json:"metadata,omitempty" yaml:"metadata,omitempty"`
}

type Config struct {
	Peers []PeerConfig `json:"peers" yaml:"peers"`
}

func LoadConfig() (*Config, error) {
	peersEnv := os.Getenv("GRAFT_PEERS")
	if peersEnv != "" {
		return parseEnvConfig(peersEnv)
	}

	configFile := os.Getenv("GRAFT_DISCOVERY_CONFIG")
	if configFile == "" {
		configFile = "peers.yaml"
		if _, err := os.Stat(configFile); os.IsNotExist(err) {
			configFile = "peers.json"
			if _, err := os.Stat(configFile); os.IsNotExist(err) {
				return &Config{Peers: []PeerConfig{}}, nil
			}
		}
	}

	return loadConfigFromFile(configFile)
}

func parseEnvConfig(peersEnv string) (*Config, error) {
	config := &Config{Peers: []PeerConfig{}}
	
	peerStrings := strings.Split(peersEnv, ",")
	for i, peerString := range peerStrings {
		peerString = strings.TrimSpace(peerString)
		if peerString == "" {
			continue
		}

		parts := strings.Split(peerString, ":")
		if len(parts) != 2 {
			return nil, domain.Error{
				Type:    domain.ErrorTypeValidation,
				Message: fmt.Sprintf("invalid peer format: %s", peerString),
				Details: map[string]interface{}{
					"peer":   peerString,
					"format": "expected address:port",
				},
			}
		}

		port := 0
		if _, err := fmt.Sscanf(parts[1], "%d", &port); err != nil {
			return nil, domain.Error{
				Type:    domain.ErrorTypeValidation,
				Message: fmt.Sprintf("invalid port number: %s", parts[1]),
				Details: map[string]interface{}{
					"peer": peerString,
					"port": parts[1],
				},
			}
		}

		config.Peers = append(config.Peers, PeerConfig{
			ID:      fmt.Sprintf("peer-%d", i),
			Address: parts[0],
			Port:    port,
			Metadata: map[string]string{
				"source": "env",
			},
		})
	}

	return config, nil
}

func loadConfigFromFile(filename string) (*Config, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: fmt.Sprintf("failed to read config file: %v", err),
			Details: map[string]interface{}{
				"file": filename,
			},
		}
	}

	config := &Config{}
	
	switch {
	case strings.HasSuffix(filename, ".yaml"), strings.HasSuffix(filename, ".yml"):
		if err := yaml.Unmarshal(data, config); err != nil {
			return nil, domain.Error{
				Type:    domain.ErrorTypeValidation,
				Message: fmt.Sprintf("failed to parse YAML config: %v", err),
				Details: map[string]interface{}{
					"file": filename,
				},
			}
		}
	case strings.HasSuffix(filename, ".json"):
		if err := json.Unmarshal(data, config); err != nil {
			return nil, domain.Error{
				Type:    domain.ErrorTypeValidation,
				Message: fmt.Sprintf("failed to parse JSON config: %v", err),
				Details: map[string]interface{}{
					"file": filename,
				},
			}
		}
	default:
		return nil, domain.Error{
			Type:    domain.ErrorTypeValidation,
			Message: "unsupported config file format",
			Details: map[string]interface{}{
				"file":            filename,
				"supported_types": []string{"json", "yaml", "yml"},
			},
		}
	}

	if err := validateConfig(config); err != nil {
		return nil, err
	}

	return config, nil
}

func validateConfig(config *Config) error {
	seenIDs := make(map[string]bool)
	seenAddresses := make(map[string]bool)

	for i, peer := range config.Peers {
		if peer.ID == "" {
			config.Peers[i].ID = fmt.Sprintf("peer-%d", i)
		} else {
			if seenIDs[peer.ID] {
				return domain.Error{
					Type:    domain.ErrorTypeValidation,
					Message: fmt.Sprintf("duplicate peer ID: %s", peer.ID),
					Details: map[string]interface{}{
						"peer_id": peer.ID,
					},
				}
			}
			seenIDs[peer.ID] = true
		}

		if peer.Address == "" {
			return domain.Error{
				Type:    domain.ErrorTypeValidation,
				Message: "peer address cannot be empty",
				Details: map[string]interface{}{
					"peer_id": peer.ID,
				},
			}
		}

		if peer.Port <= 0 || peer.Port > 65535 {
			return domain.Error{
				Type:    domain.ErrorTypeValidation,
				Message: fmt.Sprintf("invalid port number: %d", peer.Port),
				Details: map[string]interface{}{
					"peer_id": peer.ID,
					"port":    peer.Port,
				},
			}
		}

		addressKey := fmt.Sprintf("%s:%d", peer.Address, peer.Port)
		if seenAddresses[addressKey] {
			return domain.Error{
				Type:    domain.ErrorTypeValidation,
				Message: fmt.Sprintf("duplicate peer address: %s", addressKey),
				Details: map[string]interface{}{
					"peer_id": peer.ID,
					"address": addressKey,
				},
			}
		}
		seenAddresses[addressKey] = true

		if config.Peers[i].Metadata == nil {
			config.Peers[i].Metadata = make(map[string]string)
		}
	}

	return nil
}

func LoadConfigFromAPI(peers []PeerConfig) (*Config, error) {
	config := &Config{Peers: peers}
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	return config, nil
}