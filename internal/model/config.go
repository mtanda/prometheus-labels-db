package model

import (
	"context"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/ec2/imds"
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

	for i, target := range cfg.Targets {
		if target.Region == "" {
			region, err := getDefaultRegion()
			if err != nil {
				return nil, err
			}
			cfg.Targets[i].Region = region
		}
	}

	return &cfg, nil
}

var defaultRegion string

func getDefaultRegion() (string, error) {
	if defaultRegion != "" {
		return defaultRegion, nil
	}

	envRegion := os.Getenv("AWS_REGION")
	if envRegion != "" {
		defaultRegion = envRegion
		return defaultRegion, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return "", err
	}
	client := imds.NewFromConfig(cfg)
	region, err := client.GetRegion(ctx, &imds.GetRegionInput{})
	if err != nil {
		return "", err
	}
	defaultRegion = region.Region
	return defaultRegion, nil
}
