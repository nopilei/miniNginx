package config

import (
	"go.uber.org/zap"
)

func GetLogger() (*zap.Logger, error) {
	cfg := zap.NewDevelopmentConfig()
	cfg.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	logger, err := cfg.Build()
	if err != nil {
		return nil, err
	}
	return logger, nil
}
