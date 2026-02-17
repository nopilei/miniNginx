package config

import (
	"errors"
	"os"

	"gopkg.in/yaml.v3"
)

type TimeoutsConfig struct {
    ConnectMs int `yaml:"connect_ms"`
    ReadMs int `yaml:"read_ms"`
    WriteMs int `yaml:"write_ms"`
    TotalMs int `yaml:"total_ms"`
}

type UpstreamConfig struct {
    Host string `yaml:"host"`
    Port int `yaml:"port"`
}

type LimitsConfig struct {
    MaxClientConns int `yaml:"max_client_conns"`
    MaxConnsPerUpstream int `yaml:"max_conns_per_upstream"`
}


type Config struct {
    Listen string `yaml:"listen"`
    Upstreams []UpstreamConfig `yaml:"upstreams"`
    Timeouts TimeoutsConfig `yaml:"timeouts"`
    Limits LimitsConfig `yaml:"limits"`
}

type ConfigLoader struct {
    Path string
}
func (c ConfigLoader) GetConfig() (Config, error) {
    rawConfig, err := c.getRawConfig(c.Path)
    if err != nil {
        return Config{}, err
    }
    err = c.validateConfig(rawConfig)
    if err != nil {
        return Config{}, err
    }
    return rawConfig, nil
}
func (c ConfigLoader) getRawConfig(path string) (Config, error) {
    file, err := os.Open(path)
    if err != nil {
        return Config{}, err
    }
    defer file.Close()
    decoder := yaml.NewDecoder(file)
    rawConfig := Config{}
    err = decoder.Decode(&rawConfig)
    if err != nil {
        return Config{}, err
    }
    return rawConfig, nil
}
func (c ConfigLoader) validateConfig(rawConfig Config) error {
    if rawConfig.Listen == "" {
        return errors.New("'listen' param required")
    }
    if len(rawConfig.Upstreams) == 0 {
        return errors.New("'upstreams' param required")
    }
    if rawConfig.Timeouts == (TimeoutsConfig{}) {
        return errors.New("'timeouts' param required")
    }
    if rawConfig.Limits == (LimitsConfig{}) {
        return errors.New("'limits' param required")
    }
    return nil
}

