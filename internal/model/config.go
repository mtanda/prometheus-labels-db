package model

import (
	"os"

	yaml "gopkg.in/yaml.v2"
)

type Config struct {
	Targets []Target `yaml:"targets"`
}

type Target struct {
	Region    string   `yaml:"region"`
	Namespace []string `yaml:"namespace"`
}

func LoadConfig(configFile string) (*Config, error) {
	buf, err := os.ReadFile(configFile)
	if err != nil {
		return nil, err
	}

	var cfg Config
	err = yaml.Unmarshal(buf, &cfg)
	if err != nil {
		return nil, err
	}

	return &cfg, nil
}
